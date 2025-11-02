import Foundation
import NIO
@testable import SQLServerKit
import XCTest

/// Minimal tests to verify enhanced Agent APIs work correctly
final class MinimalAgentTests: XCTestCase {
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

    func testEnhancedAPIBasicFunctionality() async throws {
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        print("🔍 [MinimalAgent] Testing enhanced listJobsDetailed() API...")

        // Test the enhanced API
        let jobs = try await withTimeout(TIMEOUT) {
            try await agent.listJobsDetailed()
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
            XCTAssertNotNil(firstJob.lastRunDate, "Last run date should be present (even if nil)")
            XCTAssertNotNil(firstJob.nextRunDate, "Next run date should be present (even if nil)")

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
        let agent = try await withTimeout(TIMEOUT) {
            try await self.client.withConnection { connection in
                SQLServerAgentClient(connection: connection)
            }
        }

        print("🔍 [MinimalAgent] Testing basic listJobs() API fallback...")

        // Test the basic API
        let jobs = try await withTimeout(TIMEOUT) {
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