import Foundation
import NIO
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

/// Tests for the SQLServerAgentJobBuilder lifecycle: create, verify, and clean up jobs.
final class AgentJobBuilderTests: AgentTestBase, @unchecked Sendable {

    // MARK: - Tests

    func testJobBuilderCreateAndCleanup() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

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

        let result = try await withTimeout(operationTimeout) {
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
        let resolvedId = try await withTimeout(operationTimeout) {
            try await agent.getJobId(named: jobName)
        }
        XCTAssertEqual(result.jobId, resolvedId, "Returned jobId should match the server's job_id")

        // Verify the job exists via getJobDetail
        let detail = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNotNil(detail, "Job should exist after commit")
        XCTAssertEqual(detail?.name, jobName)
        XCTAssertEqual(detail?.enabled, false, "Job should be disabled as specified")

        // Verify step is attached
        let steps = try await withTimeout(operationTimeout) {
            try await agent.listSteps(jobName: jobName)
        }
        XCTAssertEqual(steps.count, 1, "Job should have exactly one step")
        XCTAssertEqual(steps.first?.name, stepName)
        XCTAssertEqual(steps.first?.subsystem, "TSQL")

        // Verify schedule is attached
        let schedules = try await withTimeout(operationTimeout) {
            try await agent.listJobSchedules(jobName: jobName)
        }
        XCTAssertGreaterThanOrEqual(schedules.count, 1, "Job should have at least one schedule")
        XCTAssertTrue(schedules.contains(where: { $0.name == scheduleName }), "Schedule name should match")

        // Clean up: delete the job
        try await withTimeout(operationTimeout) {
            try await agent.deleteJob(named: jobName)
        }

        // Verify job is gone
        let afterDelete = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNil(afterDelete, "Job should not exist after deletion")

        // Clean up the schedule (may already be removed with the job, ignore errors)
        _ = try? await withTimeout(operationTimeout) {
            try await agent.deleteSchedule(named: scheduleName)
        }
    }

    func testJobBuilderRollbackOnInvalidStep() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        let jobName = "app_test_rollback_\(UUID().uuidString.prefix(8))"

        // Build a job with an invalid step: empty step name should cause sp_add_jobstep to fail
        let builder = SQLServerAgentJobBuilder(
            agent: agent,
            jobName: jobName,
            description: "This job should be rolled back",
            enabled: false
        )

        do {
            _ = try await withTimeout(operationTimeout) {
                try await builder
                    .addStep(SQLServerAgentJobStep(
                        name: "",
                        subsystem: .tsql,
                        command: "SELECT 1;"
                    ))
                    .commit()
            }
            // If commit somehow succeeds despite the invalid step, clean up
            _ = try? await withTimeout(operationTimeout) {
                try await agent.deleteJob(named: jobName)
            }
            XCTFail("Commit should have thrown for an invalid step configuration")
        } catch {
            // Expected: commit failed, rollback should have deleted the job
            let detail = try await withTimeout(operationTimeout) {
                try await agent.getJobDetail(jobName: jobName)
            }
            XCTAssertNil(detail, "Job should have been rolled back (deleted) after commit failure")
        }
    }

    func testJobBuilderMinimalJob() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        let jobName = "app_test_minimal_\(UUID().uuidString.prefix(8))"

        // Build a minimal job with no steps and no schedules
        let builder = SQLServerAgentJobBuilder(
            agent: agent,
            jobName: jobName,
            description: "Minimal job with no steps or schedules",
            enabled: false
        )

        let result = try await withTimeout(operationTimeout) {
            try await builder.commit()
        }

        XCTAssertEqual(result.name, jobName, "Committed job name should match")
        XCTAssertFalse(result.jobId.isEmpty, "Job ID must be a non-empty UUID string")

        // Verify the job exists
        let detail = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNotNil(detail, "Minimal job should exist after commit")
        XCTAssertEqual(detail?.name, jobName)
        XCTAssertEqual(detail?.enabled, false)

        // Verify no steps
        let steps = try await withTimeout(operationTimeout) {
            try await agent.listSteps(jobName: jobName)
        }
        XCTAssertEqual(steps.count, 0, "Minimal job should have no steps")

        // Clean up
        try await withTimeout(operationTimeout) {
            try await agent.deleteJob(named: jobName)
        }

        // Verify deletion
        let afterDelete = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: jobName)
        }
        XCTAssertNil(afterDelete, "Minimal job should not exist after deletion")
    }
}
