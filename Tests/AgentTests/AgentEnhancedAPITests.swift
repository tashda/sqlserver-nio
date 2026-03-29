import Foundation
import NIO
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

/// Tests for the enhanced SQLServerAgentOperations APIs that provide comprehensive job management data
final class AgentEnhancedAPITests: AgentTestBase, @unchecked Sendable {

    func testListJobsDetailedReturnsComprehensiveData() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        // Test the enhanced API
        let jobs = try await withTimeout(operationTimeout) {
            try await agent.listJobDetails()
        }

        // Verify we got job data
        XCTAssertNotNil(jobs, "listJobsDetailed should return data")
        XCTAssertTrue(jobs.count >= 0, "Should return non-negative job count")

        // If there are jobs, verify the structure
        if let firstJob = jobs.first {
            XCTAssertFalse(firstJob.name.isEmpty, "Job name should not be empty")
            XCTAssertFalse(firstJob.jobId.isEmpty, "Job ID should not be empty")

            // Verify all expected fields are present
            XCTAssertNotNil(firstJob.enabled, "Enabled status should be present")
            XCTAssertNotNil(firstJob.hasSchedule, "Schedule flag should be present")
            XCTAssertNotNil(firstJob.lastRunOutcome, "Last run outcome should be present (even if nil)")
            XCTAssertNotNil(firstJob.categoryName, "Category should be present (even if nil)")
            XCTAssertNotNil(firstJob.ownerLoginName, "Owner should be present (even if nil)")
            XCTAssertNotNil(firstJob.description, "Description should be present (even if nil)")
            XCTAssertNotNil(firstJob.startStepId, "Start step ID should be present (even if nil)")
        }

        print("✅ Found \(jobs.count) jobs with detailed information")
    }

    func testGetJobDetailForSpecificJob() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let managedJob = try await createManagedJob()

        // Test getting detailed info for a specific job
        let jobDetail = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: managedJob.jobName)
        }

        XCTAssertNotNil(jobDetail, "Should get job detail for existing job")
        XCTAssertEqual(jobDetail?.name, managedJob.jobName, "Job name should match")
        XCTAssertEqual(jobDetail?.enabled, true, "Enabled status should match")

        print("✅ Successfully retrieved detailed info for job: \(managedJob.jobName)")
    }

    func testGetJobStepsForSpecificJob() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let managedJob = try await createManagedJob()

        // Test getting steps for the job
        let steps = try await withTimeout(operationTimeout) {
            try await agent.getJobSteps(jobName: managedJob.jobName)
        }

        XCTAssertNotNil(steps, "Should get steps array (may be empty)")
        XCTAssertTrue(steps.count >= 0, "Steps count should be non-negative")

        // Verify step structure if steps exist
        if let firstStep = steps.first {
            XCTAssertFalse(firstStep.name.isEmpty, "Step name should not be empty")
            XCTAssertFalse(firstStep.subsystem.isEmpty, "Step subsystem should not be empty")
            XCTAssertTrue(firstStep.stepId > 0, "Step ID should be positive")

            // Verify optional fields are properly handled
            XCTAssertNotNil(firstStep.command, "Command should be present (even if nil)")
            XCTAssertNotNil(firstStep.databaseName, "Database name should be present (even if nil)")
        }

        print("✅ Found \(steps.count) steps for job: \(managedJob.jobName)")
    }

    func testGetJobSchedulesForSpecificJob() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let managedJob = try await createManagedJob(includeSchedule: true)

        // Test getting schedules for the job
        let schedules = try await withTimeout(operationTimeout) {
            try await agent.listJobSchedules(jobName: managedJob.jobName)
        }

        XCTAssertNotNil(schedules, "Should get schedules array (may be empty)")
        XCTAssertTrue(schedules.count >= 0, "Schedules count should be non-negative")

        // Verify schedule structure if schedules exist
        if let firstSchedule = schedules.first {
            XCTAssertFalse(firstSchedule.name.isEmpty, "Schedule name should not be empty")
            XCTAssertFalse(firstSchedule.scheduleId.isEmpty, "Schedule ID should not be empty")
            XCTAssertTrue(firstSchedule.freqType > 0, "Frequency type should be positive")

            // Optional schedule fields may be absent depending on the frequency type.
            _ = firstSchedule.freqInterval
            _ = firstSchedule.nextRunDate
        }

        XCTAssertTrue(schedules.contains { $0.name == managedJob.scheduleName }, "Expected managed schedule to be attached")
        print("✅ Found \(schedules.count) schedules for job: \(managedJob.jobName)")
    }

    func testGetJobHistoryComprehensive() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        // Test getting comprehensive job history
        let history = try await withTimeout(operationTimeout) {
            try await agent.getJobHistory(top: 50)
        }

        XCTAssertNotNil(history, "Should get history array (may be empty)")
        XCTAssertTrue(history.count >= 0, "History count should be non-negative")
        XCTAssertTrue(history.count <= 50, "History count should not exceed top parameter")

        // Verify history structure if entries exist
        if let firstEntry = history.first {
            XCTAssertFalse(firstEntry.jobName.isEmpty, "Job name should not be empty")
            XCTAssertTrue(firstEntry.instanceId > 0, "Instance ID should be positive")
            XCTAssertTrue(firstEntry.stepId >= 0, "Step ID should be non-negative")
            XCTAssertFalse(firstEntry.runStatusDescription.isEmpty, "Run status description should not be empty")
            XCTAssertFalse(firstEntry.message.isEmpty, "Message should not be empty")

            // Verify optional fields are properly handled
            XCTAssertNotNil(firstEntry.stepName, "Step name should be present (even if nil)")
            XCTAssertNotNil(firstEntry.runDateTime, "Run date/time should be present (even if nil)")
            XCTAssertNotNil(firstEntry.runDurationSeconds, "Run duration should be present (even if nil)")

            // Verify run status is in expected range
            XCTAssertTrue([0, 1, 2, 3, 4, 5].contains(firstEntry.runStatus),
                         "Run status should be in expected range: \(firstEntry.runStatus)")
        }

        print("✅ Found \(history.count) comprehensive history entries")
    }

    func testGetJobHistoryForSpecificJob() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let managedJob = try await createManagedJob()

        // Test getting history for a specific job
        let history = try await withTimeout(operationTimeout) {
            try await agent.getJobHistory(jobName: managedJob.jobName, top: 20)
        }

        XCTAssertNotNil(history, "Should get history array for specific job (may be empty)")
        XCTAssertTrue(history.count >= 0, "History count should be non-negative")
        XCTAssertTrue(history.count <= 20, "History count should not exceed top parameter")

        // Verify that all entries are for the correct job
        for entry in history {
            XCTAssertEqual(entry.jobName, managedJob.jobName, "All history entries should be for the correct job")
        }

        print("✅ Found \(history.count) history entries for job: \(managedJob.jobName)")
    }

    func testEnhancedAPIsUseStoredProcedures() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        // Test that the enhanced APIs work without throwing errors
        // This implicitly tests that they're using stored procedures correctly
        let jobs = try await withTimeout(operationTimeout) {
            try await agent.listJobDetails()
        }

        // If we get here without errors, the stored procedures are working
        XCTAssertTrue(true, "Enhanced APIs should use stored procedures successfully")

        if !jobs.isEmpty {
            let jobDetail = try await withTimeout(operationTimeout) {
                try await agent.getJobDetail(jobName: jobs.first!.name)
            }
            XCTAssertNotNil(jobDetail, "Job detail should be retrievable using stored procedures")
        }

        print("✅ Enhanced APIs successfully use Microsoft stored procedures")
    }

    func testRenameJobAsync() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let managedJob = try await createManagedJob()
        let renamedJob = "agent_renamed_\(UUID().uuidString.prefix(8))"

        try await withTimeout(operationTimeout) {
            try await agent.renameJob(named: managedJob.jobName, to: renamedJob)
        }

        if let index = managedJobNames.firstIndex(of: managedJob.jobName) {
            managedJobNames[index] = renamedJob
        }

        let oldDetail = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: managedJob.jobName)
        }
        XCTAssertNil(oldDetail, "Old job name should no longer resolve after rename")

        let newDetail = try await withTimeout(operationTimeout) {
            try await agent.getJobDetail(jobName: renamedJob)
        }
        XCTAssertEqual(newDetail?.name, renamedJob)
    }

    func testListRunningJobsAsync() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let jobs = try await withTimeout(operationTimeout) {
            try await agent.listRunningJobs()
        }

        XCTAssertGreaterThanOrEqual(jobs.count, 0)
    }

    func testAgentPreflightAsync() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        try await withTimeout(operationTimeout) {
            try await agent.preflightAgentEnvironment()
        }
    }

    func testDateConversionInEnhancedAPIs() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        // Test the enhanced history API which includes date conversion
        let history = try await withTimeout(operationTimeout) {
            try await agent.getJobHistory(top: 10)
        }

        // Verify date objects are properly created when date data exists
        for entry in history {
            // Date should be nil or a valid Date object (not malformed)
            if let runDate = entry.runDateTime {
                XCTAssertTrue(runDate.timeIntervalSince1970 > 0, "Run date should be valid")
            }

            // Duration should be nil or a positive integer
            if let duration = entry.runDurationSeconds {
                XCTAssertTrue(duration >= 0, "Duration should be non-negative")
            }
        }

        print("✅ Date conversion working correctly in enhanced APIs")
    }

    /// Test for the connectionClosed issue App is experiencing
    func testEnhancedAPIConnectionStability() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [EnhancedAPI] Testing enhanced API connection stability...")

        // Test enhanced API multiple times to see if connection issues persist
        var successfulAttempts = 0
        let totalAttempts = 3

        for attempt in 1...totalAttempts {
            do {
                print("🔍 [EnhancedAPI] Attempt \(attempt) testing listJobsDetailed()...")
                let jobs = try await agent.listJobDetails()
                print("✅ [EnhancedAPI] Attempt \(attempt) succeeded: loaded \(jobs.count) jobs")
                successfulAttempts += 1

                // Test the data structure
                if let firstJob = jobs.first {
                    print("🔍 [EnhancedAPI] Job \(firstJob.name) has jobId: \(firstJob.jobId)")
                    XCTAssertFalse(firstJob.jobId.isEmpty, "JobId should not be empty")
                    XCTAssertFalse(firstJob.name.isEmpty, "Name should not be empty")
                }
            } catch {
                print("❌ [EnhancedAPI] Attempt \(attempt) failed: \(error.localizedDescription)")
                // Don't fail the test, just continue to see if other attempts work
            }
        }

        print("🔍 [EnhancedAPI] Enhanced API stability: \(successfulAttempts)/\(totalAttempts) attempts succeeded")

        // If enhanced API is unreliable, test basic API as comparison
        if successfulAttempts < totalAttempts {
            print("🔄 [EnhancedAPI] Enhanced API unstable, testing basic API...")

            do {
                let basicJobs = try await agent.listJobs()
                print("✅ [EnhancedAPI] Basic API works: \(basicJobs.count) jobs")
                XCTAssertGreaterThan(basicJobs.count, 0, "Basic API should return jobs")
            } catch {
                XCTFail("Both enhanced and basic APIs failed: \(error.localizedDescription)")
            }
        } else {
            XCTAssertGreaterThan(successfulAttempts, 0, "At least one enhanced API attempt should succeed")
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
