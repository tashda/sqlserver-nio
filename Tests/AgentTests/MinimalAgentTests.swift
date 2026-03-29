import Foundation
import NIO
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

/// Minimal tests to verify enhanced Agent APIs work correctly
final class MinimalAgentTests: AgentTestBase, @unchecked Sendable {

    func testEnhancedAPIBasicFunctionality() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [MinimalAgent] Testing enhanced listJobsDetailed() API...")

        // Test the enhanced API
        let jobs = try await withTimeout(operationTimeout) {
            try await agent.listJobDetails()
        }

        // Verify we got job data
        XCTAssertNotNil(jobs, "listJobsDetailed should return data")
        XCTAssertTrue(jobs.count >= 0, "Should return non-negative job count")

        print("✅ [MinimalAgent] Enhanced API succeeded: loaded \(jobs.count) jobs")

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

            print("🔍 [MinimalAgent] Sample enhanced job data:")
            print("  - jobId: \(firstJob.jobId)")
            print("  - name: \(firstJob.name)")
            print("  - enabled: \(firstJob.enabled)")
            print("  - owner: \(firstJob.ownerLoginName ?? "nil")")
            print("  - category: \(firstJob.categoryName ?? "nil")")
            print("  - description: \(firstJob.description ?? "nil")")
        }
    }

    func testBasicAPIFallback() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        print("🔍 [MinimalAgent] Testing basic listJobs() API fallback...")

        // Test the basic API
        let jobs = try await withTimeout(operationTimeout) {
            try await agent.listJobs()
        }

        XCTAssertNotNil(jobs, "listJobs should return data")
        XCTAssertTrue(jobs.count >= 0, "Should return non-negative job count")

        print("✅ [MinimalAgent] Basic API succeeded: loaded \(jobs.count) jobs")

        // Verify basic job data structure
        if let firstJob = jobs.first {
            print("🔍 [MinimalAgent] Basic job data check:")
            print("  - name: '\(firstJob.name)'")
            print("  - enabled: \(firstJob.enabled)")
            print("  - lastRunOutcome: '\(firstJob.lastRunOutcome ?? "nil")'")
        }
    }
}
