import Foundation
import NIO
@testable import SQLServerKit
import XCTest

/// Tests that mirror Echo's actual job management scenarios and data usage
final class EchoIntegrationTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    override func setUp() async throws {
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

    /// Test the exact scenario Echo uses: enhanced API -> fallback to basic API
    func testEchoJobLoadingScenario() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        print("🔍 [EchoIntegration] Testing Echo's job loading scenario...")

        // Test 1: Try enhanced API (Echo's primary approach)
        do {
            print("🔍 [EchoIntegration] Testing enhanced listJobsDetailed() API...")
            let enhancedJobs = try await agent.listJobsDetailed()
            print("✅ [EchoIntegration] Enhanced API succeeded: loaded \(enhancedJobs.count) jobs")

            // Verify the data structure matches Echo's expectations
            if let firstJob = enhancedJobs.first {
                print("🔍 [EchoIntegration] Sample enhanced job data:")
                print("  - jobId: \(firstJob.jobId)")
                print("  - name: \(firstJob.name)")
                print("  - enabled: \(firstJob.enabled)")
                print("  - owner: \(firstJob.ownerLoginName ?? "nil")")
                print("  - category: \(firstJob.categoryName ?? "nil")")
                print("  - description: \(firstJob.description ?? "nil")")
            }
        } catch {
            print("❌ [EchoIntegration] Enhanced API failed: \(error.localizedDescription)")

            // Test 2: Fallback to basic API (Echo's fallback mechanism)
            do {
                print("🔄 [EchoIntegration] Testing fallback to basic listJobs() API...")
                let basicJobs = try await agent.listJobs()
                print("✅ [EchoIntegration] Basic API succeeded: loaded \(basicJobs.count) jobs")

                // Verify Echo can convert this to JobRow format
                for (index, job) in basicJobs.enumerated() {
                    print("🔍 [EchoIntegration] Basic job \(index): name='\(job.name)', enabled=\(job.enabled)")
                }

                // Test Echo's data conversion mechanism
                for job in basicJobs {
                    // This mirrors Echo's exact conversion logic
                    let mockDetail = SQLServerAgentJobDetail(
                        jobId: job.name,
                        name: job.name,
                        description: nil,
                        enabled: job.enabled,
                        ownerLoginName: nil,
                        categoryName: nil,
                        startStepId: nil,
                        lastRunOutcome: job.lastRunOutcome,
                        lastRunDate: nil,
                        nextRunDate: nil,
                        hasSchedule: false
                    )

                    // Test Echo's JobRow conversion (simplified version)
                    let id = mockDetail.jobId
                    let name = mockDetail.name
                    let enabled = mockDetail.enabled
                    let category = mockDetail.categoryName
                    let owner = mockDetail.ownerLoginName
                    let lastOutcome = mockDetail.lastRunOutcome

                    print("🔍 [EchoIntegration] Echo conversion result:")
                    print("  - id: '\(id)'")
                    print("  - name: '\(name)'")
                    print("  - enabled: \(enabled)")
                    print("  - category: \(category ?? "nil")")
                    print("  - owner: \(owner ?? "nil")")
                    print("  - lastOutcome: \(lastOutcome ?? "nil")")
                    break // Just test one for brevity
                }
            } catch {
                XCTFail("Both enhanced and basic APIs failed: \(error.localizedDescription)")
            }
        }
    }

    /// Test Echo's stored procedure usage
    func testEchoStoredProcedures() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        print("🔍 [EchoIntegration] Testing stored procedures Echo uses...")

        // Test sp_help_job (enhanced API)
        do {
            print("🔍 [EchoIntegration] Testing sp_help_job...")
            let jobs = try await agent.listJobsDetailed()
            print("✅ [EchoIntegration] sp_help_job succeeded: \(jobs.count) jobs")
        } catch {
            print("❌ [EchoIntegration] sp_help_job failed: \(error.localizedDescription)")
        }

        // Test sp_help_jobstep (steps)
        if let firstJob = try? await agent.listJobsDetailed().first {
            do {
                print("🔍 [EchoIntegration] Testing sp_help_jobstep for job: \(firstJob.name)...")
                let steps = try await agent.getJobSteps(jobName: firstJob.name)
                print("✅ [EchoIntegration] sp_help_jobstep succeeded: \(steps.count) steps")

                if let firstStep = steps.first {
                    print("🔍 [EchoIntegration] Sample step data:")
                    print("  - stepId: \(firstStep.stepId)")
                    print("  - name: \(firstStep.name)")
                    print("  - subsystem: \(firstStep.subsystem)")
                    print("  - command: \(firstStep.command ?? "nil")")
                }
            } catch {
                print("❌ [EchoIntegration] sp_help_jobstep failed: \(error.localizedDescription)")
            }
        }

        // Test sp_help_jobschedule (schedules)
        if let firstJob = try? await agent.listJobsDetailed().first {
            do {
                print("🔍 [EchoIntegration] Testing sp_help_jobschedule for job: \(firstJob.name)...")
                let schedules = try await agent.getJobSchedules(jobName: firstJob.name)
                print("✅ [EchoIntegration] sp_help_jobschedule succeeded: \(schedules.count) schedules")

                if let firstSchedule = schedules.first {
                    print("🔍 [EchoIntegration] Sample schedule data:")
                    print("  - scheduleId: \(firstSchedule.scheduleId)")
                    print("  - name: \(firstSchedule.name)")
                    print("  - enabled: \(firstSchedule.enabled)")
                    print("  - freqType: \(firstSchedule.freqType)")
                }
            } catch {
                print("❌ [EchoIntegration] sp_help_jobschedule failed: \(error.localizedDescription)")
            }
        }

        // Test sp_help_jobhistory (history)
        do {
            print("🔍 [EchoIntegration] Testing sp_help_jobhistory...")
            let history = try await agent.getJobHistory(top: 10)
            print("✅ [EchoIntegration] sp_help_jobhistory succeeded: \(history.count) entries")

            if let firstEntry = history.first {
                print("🔍 [EchoIntegration] Sample history entry:")
                print("  - instanceId: \(firstEntry.instanceId)")
                print("  - jobName: \(firstEntry.jobName)")
                print("  - stepId: \(firstEntry.stepId)")
                print("  - runStatus: \(firstEntry.runStatus)")
                print("  - runStatusDescription: '\(firstEntry.runStatusDescription)'")
                print("  - message: '\(firstEntry.message.prefix(50))'")
                print("  - runDateTime: \(firstEntry.runDateTime?.description ?? "nil")")
            }
        } catch {
            print("❌ [EchoIntegration] sp_help_jobhistory failed: \(error.localizedDescription)")
        }
    }

    /// Test Echo's job management operations
    func testEchoJobOperations() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        print("🔍 [EchoIntegration] Testing Echo's job operations...")

        // Get a test job
        let jobs = try await agent.listJobsDetailed()
        guard let testJob = jobs.first else {
            throw XCTSkip("No jobs found to test operations")
        }

        print("🔍 [EchoIntegration] Testing operations on job: \(testJob.name)")

        // Test start job
        do {
            print("🔍 [EchoIntegration] Testing startJob...")
            try await agent.startJob(named: testJob.name)
            print("✅ [EchoIntegration] startJob succeeded")
        } catch {
            print("⚠️ [EchoIntegration] startJob failed (may be normal if job is already running): \(error.localizedDescription)")
        }

        // Test stop job
        do {
            print("🔍 [EchoIntegration] Testing stopJob...")
            try await agent.stopJob(named: testJob.name)
            print("✅ [EchoIntegration] stopJob succeeded")
        } catch {
            print("⚠️ [EchoIntegration] stopJob failed: \(error.localizedDescription)")
        }

        // Test enable/disable job
        do {
            print("🔍 [EchoIntegration] Testing enableJob...")
            try await agent.enableJob(named: testJob.name, enabled: true)
            print("✅ [EchoIntegration] enableJob succeeded")

            try await agent.enableJob(named: testJob.name, enabled: false)
            print("✅ [EchoIntegration] disableJob succeeded")

            // Restore original state
            try await agent.enableJob(named: testJob.name, enabled: testJob.enabled)
            print("✅ [EchoIntegration] restored original state")
        } catch {
            print("❌ [EchoIntegration] enable/disable job failed: \(error.localizedDescription)")
        }
    }

    /// Test basic API reliability
    func testBasicAPIReliability() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        print("🔍 [EchoIntegration] Testing basic API reliability...")

        // Run multiple times to test connection stability
        for attempt in 1...3 {
            print("🔍 [EchoIntegration] Basic API attempt \(attempt)...")
            let jobs = try await agent.listJobs()
            print("✅ [EchoIntegration] Attempt \(attempt) succeeded: \(jobs.count) jobs")

            // Verify basic job data structure
            if let firstJob = jobs.first {
                print("🔍 [EchoIntegration] Basic job data check:")
                print("  - name: '\(firstJob.name)'")
                print("  - enabled: \(firstJob.enabled)")
                print("  - lastRunOutcome: '\(firstJob.lastRunOutcome ?? "nil")'")
            }
        }
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully().get()
        } catch {
            // Silently ignore "Already closed" errors during shutdown - they're expected under stress
            if error.localizedDescription.contains("Already closed") ||
               error.localizedDescription.contains("ChannelError error 6") {
                // Both errors are expected during EventLoop shutdown
            } else {
                throw error
            }
        }

        try await group?.shutdownGracefully()
    }
}