import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

/// Integration tests for the Change Tracking and CDC namespace (`client.changeTracking`).
final class ChangeTrackingTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(
            configuration: config,
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - Change Tracking Status

    func testChangeTrackingStatusReturnsArray() async throws {
        // changeTrackingStatus queries sys.change_tracking_databases.
        // On a fresh server this may be empty, but the call must succeed.
        let statuses = try await client.changeTracking.changeTrackingStatus()
        XCTAssertNotNil(statuses)

        // If any databases have CT enabled, validate structure
        for status in statuses {
            XCTAssertFalse(status.databaseName.isEmpty, "Database name must not be empty")
            XCTAssertGreaterThanOrEqual(status.retentionPeriod, 0)
            XCTAssertFalse(status.retentionPeriodUnits.isEmpty)
        }
    }

    // MARK: - Change Tracking Tables

    func testListChangeTrackingTablesReturnsArray() async throws {
        // On a server without CT-enabled tables this returns an empty array.
        let tables = try await client.changeTracking.listChangeTrackingTables()
        XCTAssertNotNil(tables)

        for table in tables {
            XCTAssertFalse(table.schemaName.isEmpty)
            XCTAssertFalse(table.tableName.isEmpty)
        }
    }

    // MARK: - CDC Tables

    func testListCDCTablesReturnsArray() async throws {
        // CDC requires SQL Server Agent and explicit enablement.
        // On a test instance without CDC this may return empty or throw;
        // both are acceptable.
        do {
            let tables = try await client.changeTracking.listCDCTables()
            XCTAssertNotNil(tables)

            for table in tables {
                XCTAssertFalse(table.schemaName.isEmpty)
                XCTAssertFalse(table.tableName.isEmpty)
                XCTAssertTrue(table.isTrackedByCDC, "Only CDC-tracked tables should be returned")
            }
        } catch {
            // CDC system tables (cdc.change_tables) may not exist if CDC
            // has never been enabled on the database. Skip gracefully.
            throw XCTSkip("CDC tables not available on this instance: \(error)")
        }
    }

    // MARK: - Identifiable Conformance

    func testCDCTableIdentifiable() {
        let table = SQLServerCDCTable(
            schemaName: "dbo",
            tableName: "Orders",
            captureInstance: "dbo_Orders",
            isTrackedByCDC: true
        )
        XCTAssertEqual(table.id, "dbo.Orders")
    }

    func testChangeTrackingStatusEquatable() {
        let a = SQLServerChangeTrackingStatus(
            databaseName: "TestDB",
            isAutoCleanupOn: true,
            retentionPeriod: 2,
            retentionPeriodUnits: "DAYS"
        )
        let b = SQLServerChangeTrackingStatus(
            databaseName: "TestDB",
            isAutoCleanupOn: true,
            retentionPeriod: 2,
            retentionPeriodUnits: "DAYS"
        )
        XCTAssertEqual(a, b)
    }
}
