import Foundation
import SQLServerKit
import SQLServerKitTesting
import XCTest

final class IndexUsageStatsTests: XCTestCase, @unchecked Sendable {
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

    // MARK: - Index Usage Stats

    func testIndexUsageStatsReturnsArray() async throws {
        let stats = try await withTimeout(operationTimeout) {
            try await self.client.tuning.indexUsageStats()
        }

        XCTAssertNotNil(stats, "indexUsageStats should return a non-nil array")
        XCTAssertTrue(stats.count >= 0, "Should return zero or more index usage stat entries")

        if let first = stats.first {
            XCTAssertFalse(first.schemaName.isEmpty, "Schema name should not be empty")
            XCTAssertFalse(first.tableName.isEmpty, "Table name should not be empty")
            XCTAssertFalse(first.indexName.isEmpty, "Index name should not be empty")
            XCTAssertFalse(first.indexType.isEmpty, "Index type should not be empty")
            XCTAssertTrue(first.userSeeks >= 0, "User seeks should be non-negative")
            XCTAssertTrue(first.userScans >= 0, "User scans should be non-negative")
            XCTAssertTrue(first.userLookups >= 0, "User lookups should be non-negative")
            XCTAssertTrue(first.userUpdates >= 0, "User updates should be non-negative")
        }
    }

    func testIndexUsageStatsWithMinUpdatesFilter() async throws {
        // Fetch all stats first
        let allStats = try await withTimeout(operationTimeout) {
            try await self.client.tuning.indexUsageStats(minUpdates: 0)
        }

        // Fetch with a high minimum — should return a subset (or empty)
        let filteredStats = try await withTimeout(operationTimeout) {
            try await self.client.tuning.indexUsageStats(minUpdates: 1_000_000)
        }

        XCTAssertTrue(filteredStats.count <= allStats.count, "Filtered results should be a subset of all results")
    }

    // MARK: - Type Structure

    func testIndexUsageStatTypeFields() {
        let stat = SQLServerTuningClient.SQLServerIndexUsageStat(
            schemaName: "dbo",
            tableName: "Orders",
            indexName: "IX_Orders_Date",
            indexType: "NONCLUSTERED",
            userSeeks: 500,
            userScans: 100,
            userLookups: 50,
            userUpdates: 25,
            lastUserSeek: "2026-01-15 10:30:00.000",
            lastUserScan: nil
        )

        XCTAssertEqual(stat.schemaName, "dbo")
        XCTAssertEqual(stat.tableName, "Orders")
        XCTAssertEqual(stat.indexName, "IX_Orders_Date")
        XCTAssertEqual(stat.indexType, "NONCLUSTERED")
        XCTAssertEqual(stat.userSeeks, 500)
        XCTAssertEqual(stat.userScans, 100)
        XCTAssertEqual(stat.userLookups, 50)
        XCTAssertEqual(stat.userUpdates, 25)
        XCTAssertEqual(stat.lastUserSeek, "2026-01-15 10:30:00.000")
        XCTAssertNil(stat.lastUserScan)
        XCTAssertEqual(stat.id, "dbo.Orders.IX_Orders_Date", "id should be schema.table.index")
    }

    func testIndexUsageStatEquatable() {
        let a = SQLServerTuningClient.SQLServerIndexUsageStat(
            schemaName: "dbo",
            tableName: "T",
            indexName: "IX",
            indexType: "CLUSTERED",
            userSeeks: 1,
            userScans: 2,
            userLookups: 3,
            userUpdates: 4,
            lastUserSeek: nil,
            lastUserScan: nil
        )
        let b = SQLServerTuningClient.SQLServerIndexUsageStat(
            schemaName: "dbo",
            tableName: "T",
            indexName: "IX",
            indexType: "CLUSTERED",
            userSeeks: 1,
            userScans: 2,
            userLookups: 3,
            userUpdates: 4,
            lastUserSeek: nil,
            lastUserScan: nil
        )
        XCTAssertEqual(a, b)
    }
}
