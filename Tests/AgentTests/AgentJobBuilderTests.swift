import Foundation
import NIO
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

/// Tests for the SQLServerAgentJobBuilder lifecycle: create, verify, and clean up jobs.
final class AgentJobBuilderTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    override func setUp() async throws {
        guard envFlagEnabled("TDS_ENABLE_AGENT_TESTS") else {
            throw XCTSkip("Skipping agent tests. Set TDS_ENABLE_AGENT_TESTS=1 to enable.")
        }
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()

        // Ensure Agent XPs are enabled for these tests
        let metadata = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                return SQLServerMetadataClient(connection: connection)
            }
        }

        let agentStatus = try await withTimeout(TIMEOUT) {
            try await metadata.fetchAgentStatus().get()
        }

        if agentStatus.isSqlAgentRunning && !agentStatus.isSqlAgentEnabled {
            _ = try await client.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;").get()
            _ = try await client.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;").get()
        }
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully().get()
        } catch {
            if error.localizedDescription.contains("Already closed") ||
               error.localizedDescription.contains("ChannelError error 6") {
                // Both errors are expected during EventLoop shutdown
            } else {
                throw error
            }
        }

        try await group?.shutdownGracefully()
    }

    // MARK: - Tests

    func testJobBuilderCreateAndCleanup() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        let jobName = "app_test_builder_\(UUID().uuidString.prefix(8))"
        let stepName = "step_select_1"
        let scheduleName = "sched_daily_\(UUID().uuidString.prefix(8))"

        // Build and commit the job
        let builder = SQLServerAgentJobBuilder(
            agent: agent,
            jobName: jobName,
            description: "Integration test job created by AgentJobBuilderTests",
            enabled: false
        )

        let result = try await withTimeout(TIMEOUT) {
            try await builder
                .addStep(SQLServerAgentJobStep(
                    name: stepName,
                    subsystem: .tsql,
                    command: "SELECT 1;",
                    database: "master"
                ))
                .addSchedule(SQLServerAgentJobSchedule(
                    name: scheduleName,
                    enabled: false,
                    kind: .daily(everyDays: 1, startTime: 120000)
                ))
                .commit()
        }

        XCTAssertEqual(result.name, jobName, "Committed job name should match")
        XCTAssertFalse(result.jobId.isEmpty, "Job ID must be a non-empty UUID string")

        // Verify the returned jobId matches what SQL Server has
        let resolvedId = try await withTimeout(TIMEOUT) {
            try await agent.fetchJobId(named: jobName)
        }
        XCTAssertEqual(result.jobId, resolvedId, "Returned jobId should match the server's job_id")

        // Verify the job exists via getJobDetail
        let detail = try await withTimeout(TIMEOUT) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNotNil(detail, "Job should exist after commit")
        XCTAssertEqual(detail?.name, jobName)
        XCTAssertEqual(detail?.enabled, false, "Job should be disabled as specified")

        // Verify step is attached
        let steps = try await withTimeout(TIMEOUT) {
            try await agent.listSteps(jobName: jobName)
        }
        XCTAssertEqual(steps.count, 1, "Job should have exactly one step")
        XCTAssertEqual(steps.first?.name, stepName)
        XCTAssertEqual(steps.first?.subsystem, "TSQL")

        // Verify schedule is attached
        let schedules = try await withTimeout(TIMEOUT) {
            try await agent.getJobSchedules(jobName: jobName)
        }
        XCTAssertGreaterThanOrEqual(schedules.count, 1, "Job should have at least one schedule")
        XCTAssertTrue(schedules.contains(where: { $0.name == scheduleName }), "Schedule name should match")

        // Clean up: delete the job
        try await withTimeout(TIMEOUT) {
            try await agent.deleteJob(named: jobName)
        }

        // Verify job is gone
        let afterDelete = try await withTimeout(TIMEOUT) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNil(afterDelete, "Job should not exist after deletion")

        // Clean up the schedule (may already be removed with the job, ignore errors)
        _ = try? await withTimeout(TIMEOUT) {
            try await agent.deleteSchedule(named: scheduleName)
        }
    }

    func testJobBuilderRollbackOnInvalidStep() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        let jobName = "app_test_rollback_\(UUID().uuidString.prefix(8))"

        // Build a job with an invalid step: empty step name should cause sp_add_jobstep to fail
        let builder = SQLServerAgentJobBuilder(
            agent: agent,
            jobName: jobName,
            description: "This job should be rolled back",
            enabled: false
        )

        do {
            _ = try await withTimeout(TIMEOUT) {
                try await builder
                    .addStep(SQLServerAgentJobStep(
                        name: "",
                        subsystem: .tsql,
                        command: "SELECT 1;"
                    ))
                    .commit()
            }
            // If commit somehow succeeds despite the invalid step, clean up
            _ = try? await withTimeout(TIMEOUT) {
                try await agent.deleteJob(named: jobName)
            }
            XCTFail("Commit should have thrown for an invalid step configuration")
        } catch {
            // Expected: commit failed, rollback should have deleted the job
            let detail = try await withTimeout(TIMEOUT) {
                try await agent.getJobDetail(jobName: jobName)
            }
            XCTAssertNil(detail, "Job should have been rolled back (deleted) after commit failure")
        }
    }

    func testJobBuilderMinimalJob() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        let jobName = "app_test_minimal_\(UUID().uuidString.prefix(8))"

        // Build a minimal job with no steps and no schedules
        let builder = SQLServerAgentJobBuilder(
            agent: agent,
            jobName: jobName,
            description: "Minimal job with no steps or schedules",
            enabled: false
        )

        let result = try await withTimeout(TIMEOUT) {
            try await builder.commit()
        }

        XCTAssertEqual(result.name, jobName, "Committed job name should match")
        XCTAssertFalse(result.jobId.isEmpty, "Job ID must be a non-empty UUID string")

        // Verify the job exists
        let detail = try await withTimeout(TIMEOUT) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNotNil(detail, "Minimal job should exist after commit")
        XCTAssertEqual(detail?.name, jobName)
        XCTAssertEqual(detail?.enabled, false)

        // Verify no steps
        let steps = try await withTimeout(TIMEOUT) {
            try await agent.listSteps(jobName: jobName)
        }
        XCTAssertEqual(steps.count, 0, "Minimal job should have no steps")

        // Clean up
        try await withTimeout(TIMEOUT) {
            try await agent.deleteJob(named: jobName)
        }

        // Verify deletion
        let afterDelete = try await withTimeout(TIMEOUT) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNil(afterDelete, "Minimal job should not exist after deletion")
    }
}
