import Foundation
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

final class QueryStoreWaitStatsTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    let operationTimeout: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)

        _ = try await withTimeout(operationTimeout) {
            try await self.client.query("SELECT 1")
        }
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully()
        } catch {
            let message = error.localizedDescription
            if !message.contains("Already closed") && !message.contains("ChannelError error 6") {
                throw error
            }
        }
    }

    // MARK: - Wait Stats

    func testWaitStatsReturnsArray() async throws {
        // Query Store may not be enabled on all databases.
        // Use a database that likely has Query Store enabled, or skip gracefully.
        let database = env("TDS_AW_DATABASE") ?? "master"

        do {
            // planId 1 is arbitrary — may not exist, but the API should still return an empty array
            let stats = try await withTimeout(operationTimeout) {
                try await self.client.queryStore.waitStats(database: database, planId: 1)
            }

            XCTAssertNotNil(stats, "waitStats should return a non-nil array")
            XCTAssertTrue(stats.count >= 0, "Should return zero or more wait stat entries")

            if let first = stats.first {
                XCTAssertFalse(first.waitCategory.isEmpty, "Wait category should not be empty")
                XCTAssertTrue(first.totalWaitTimeMs >= 0, "Total wait time should be non-negative")
                XCTAssertTrue(first.avgWaitTimeMs >= 0, "Average wait time should be non-negative")
                XCTAssertTrue(first.waitCount >= 0, "Wait count should be non-negative")
            }
        } catch {
            // Query Store may not be enabled — this is expected on some servers
            let message = "\(error)"
            if message.contains("Query Store") || message.contains("query_store") || message.contains("not enabled") || message.contains("is not supported") {
                print("Query Store not available on this server — skipping: \(message)")
                throw XCTSkip("Query Store is not enabled on the test database")
            }
            throw error
        }
    }

    // MARK: - Type Structure

    func testWaitStatTypeFields() {
        // Verify the type can be constructed with all expected fields
        let stat = SQLServerQueryStoreClient.SQLServerQueryStoreWaitStat(
            waitCategory: "CPU",
            totalWaitTimeMs: 123.45,
            avgWaitTimeMs: 6.78,
            waitCount: 100
        )

        XCTAssertEqual(stat.waitCategory, "CPU")
        XCTAssertEqual(stat.totalWaitTimeMs, 123.45, accuracy: 0.001)
        XCTAssertEqual(stat.avgWaitTimeMs, 6.78, accuracy: 0.001)
        XCTAssertEqual(stat.waitCount, 100)
        XCTAssertEqual(stat.id, "CPU", "id should equal waitCategory")
    }

    func testWaitStatEquatable() {
        let a = SQLServerQueryStoreClient.SQLServerQueryStoreWaitStat(
            waitCategory: "IO",
            totalWaitTimeMs: 10.0,
            avgWaitTimeMs: 2.0,
            waitCount: 5
        )
        let b = SQLServerQueryStoreClient.SQLServerQueryStoreWaitStat(
            waitCategory: "IO",
            totalWaitTimeMs: 10.0,
            avgWaitTimeMs: 2.0,
            waitCount: 5
        )
        XCTAssertEqual(a, b)
    }
}
