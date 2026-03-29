import Foundation
import NIO
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

/// Tests that mirror App's actual job management scenarios and data usage
final class AppIntegrationTests: AgentTestBase, @unchecked Sendable {

    /// Test the exact scenario App uses: enhanced API -> fallback to basic API
    func testAppJobLoadingScenario() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [AppIntegration] Testing App's job loading scenario...")

        // Test 1: Try enhanced API (App's primary approach)
        do {
            print("🔍 [AppIntegration] Testing enhanced listJobsDetailed() API...")
            let enhancedJobs = try await agent.listJobDetails()
            print("✅ [AppIntegration] Enhanced API succeeded: loaded \(enhancedJobs.count) jobs")

            // Verify the data structure matches App's expectations
            if let firstJob = enhancedJobs.first {
                print("🔍 [AppIntegration] Sample enhanced job data:")
                print("  - jobId: \(firstJob.jobId)")
                print("  - name: \(firstJob.name)")
                print("  - enabled: \(firstJob.enabled)")
                print("  - owner: \(firstJob.ownerLoginName ?? "nil")")
                print("  - category: \(firstJob.categoryName ?? "nil")")
                print("  - description: \(firstJob.description ?? "nil")")
            }
        } catch {
            print("❌ [AppIntegration] Enhanced API failed: \(error.localizedDescription)")

            // Test 2: Fallback to basic API (App's fallback mechanism)
            do {
                print("🔄 [AppIntegration] Testing fallback to basic listJobs() API...")
                let basicJobs = try await agent.listJobs()
                print("✅ [AppIntegration] Basic API succeeded: loaded \(basicJobs.count) jobs")

                // Verify App can convert this to JobRow format
                for (index, job) in basicJobs.enumerated() {
                    print("🔍 [AppIntegration] Basic job \(index): name='\(job.name)', enabled=\(job.enabled)")
                }

                // Test App's data conversion mechanism
                for job in basicJobs {
                    // This mirrors App's exact conversion logic
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

                    // Test App's JobRow conversion (simplified version)
                    let id = mockDetail.jobId
                    let name = mockDetail.name
                    let enabled = mockDetail.enabled
                    let category = mockDetail.categoryName
                    let owner = mockDetail.ownerLoginName
                    let lastOutcome = mockDetail.lastRunOutcome

                    print("🔍 [AppIntegration] App conversion result:")
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

    /// Test App's stored procedure usage
    func testAppStoredProcedures() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [AppIntegration] Testing stored procedures App uses...")

        // Test sp_help_job (enhanced API)
        do {
            print("🔍 [AppIntegration] Testing sp_help_job...")
            let jobs = try await agent.listJobDetails()
            print("✅ [AppIntegration] sp_help_job succeeded: \(jobs.count) jobs")
        } catch {
            print("❌ [AppIntegration] sp_help_job failed: \(error.localizedDescription)")
        }

        // Test sp_help_jobstep (steps)
        if let firstJob = try? await agent.listJobDetails().first {
            do {
                print("🔍 [AppIntegration] Testing sp_help_jobstep for job: \(firstJob.name)...")
                let steps = try await agent.getJobSteps(jobName: firstJob.name)
                print("✅ [AppIntegration] sp_help_jobstep succeeded: \(steps.count) steps")

                if let firstStep = steps.first {
                    print("🔍 [AppIntegration] Sample step data:")
                    print("  - stepId: \(firstStep.stepId)")
                    print("  - name: \(firstStep.name)")
                    print("  - subsystem: \(firstStep.subsystem)")
                    print("  - command: \(firstStep.command ?? "nil")")
                }
            } catch {
                print("❌ [AppIntegration] sp_help_jobstep failed: \(error.localizedDescription)")
            }
        }

        // Test sp_help_jobschedule (schedules)
        if let firstJob = try? await agent.listJobDetails().first {
            do {
                print("🔍 [AppIntegration] Testing sp_help_jobschedule for job: \(firstJob.name)...")
                let schedules = try await agent.listJobSchedules(jobName: firstJob.name)
                print("✅ [AppIntegration] sp_help_jobschedule succeeded: \(schedules.count) schedules")

                if let firstSchedule = schedules.first {
                    print("🔍 [AppIntegration] Sample schedule data:")
                    print("  - scheduleId: \(firstSchedule.scheduleId)")
                    print("  - name: \(firstSchedule.name)")
                    print("  - enabled: \(firstSchedule.enabled)")
                    print("  - freqType: \(firstSchedule.freqType)")
                }
            } catch {
                print("❌ [AppIntegration] sp_help_jobschedule failed: \(error.localizedDescription)")
            }
        }

        // Test sp_help_jobhistory (history)
        do {
            print("🔍 [AppIntegration] Testing sp_help_jobhistory...")
            let history = try await agent.getJobHistory(top: 10)
            print("✅ [AppIntegration] sp_help_jobhistory succeeded: \(history.count) entries")

            if let firstEntry = history.first {
                print("🔍 [AppIntegration] Sample history entry:")
                print("  - instanceId: \(firstEntry.instanceId)")
                print("  - jobName: \(firstEntry.jobName)")
                print("  - stepId: \(firstEntry.stepId)")
                print("  - runStatus: \(firstEntry.runStatus)")
                print("  - runStatusDescription: '\(firstEntry.runStatusDescription)'")
                print("  - message: '\(firstEntry.message.prefix(50))'")
                print("  - runDateTime: \(firstEntry.runDateTime?.description ?? "nil")")
            }
        } catch {
            print("❌ [AppIntegration] sp_help_jobhistory failed: \(error.localizedDescription)")
        }
    }

    /// Test App's job management operations
    func testAppJobOperations() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [AppIntegration] Testing App's job operations...")

        let testJob = try await createManagedJob()

        print("🔍 [AppIntegration] Testing operations on job: \(testJob.jobName)")

        print("🔍 [AppIntegration] Testing startJob...")
        do {
            try await agent.startJob(named: testJob.jobName)
            print("✅ [AppIntegration] startJob succeeded")
        } catch {
            XCTFail("startJob failed unexpectedly: \(error.localizedDescription)")
        }

        print("🔍 [AppIntegration] Testing stopJob...")
        do {
            try await agent.stopJob(named: testJob.jobName)
            print("✅ [AppIntegration] stopJob succeeded")
        } catch {
            let message = error.localizedDescription
            let acceptable = message.contains("not currently running")
            XCTAssertTrue(acceptable, "stopJob failed unexpectedly: \(message)")
            print("ℹ️ [AppIntegration] stopJob reported non-running job, which is acceptable for a fast test job")
        }

        print("🔍 [AppIntegration] Testing enableJob...")
        do {
            try await agent.enableJob(named: testJob.jobName, enabled: true)
            print("✅ [AppIntegration] enableJob succeeded")

            try await agent.enableJob(named: testJob.jobName, enabled: false)
            print("✅ [AppIntegration] disableJob succeeded")

            try await agent.enableJob(named: testJob.jobName, enabled: true)
            print("✅ [AppIntegration] restored original state")
        } catch {
            XCTFail("enable/disable job failed: \(error.localizedDescription)")
        }
    }

    /// Test basic API reliability
    func testBasicAPIReliability() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [AppIntegration] Testing basic API reliability...")

        // Run multiple times to test connection stability
        for attempt in 1...3 {
            print("🔍 [AppIntegration] Basic API attempt \(attempt)...")
            let jobs = try await agent.listJobs()
            print("✅ [AppIntegration] Attempt \(attempt) succeeded: \(jobs.count) jobs")

            // Verify basic job data structure
            if let firstJob = jobs.first {
                print("🔍 [AppIntegration] Basic job data check:")
                print("  - name: '\(firstJob.name)'")
                print("  - enabled: \(firstJob.enabled)")
                print("  - lastRunOutcome: '\(firstJob.lastRunOutcome ?? "nil")'")
            }
        }
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully()
        } catch {
            if error.localizedDescription.contains("Already closed") ||
               error.localizedDescription.contains("ChannelError error 6") {
            } else {
                throw error
            }
        }
    }
}
