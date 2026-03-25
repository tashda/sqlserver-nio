@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerTemporalClientTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - Temporal Tables

    @available(macOS 12.0, *)
    func testListSystemVersionedTables() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_sv") { db in
                let table = "sv_\(UUID().uuidString.prefix(6))"
                try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.createSystemVersionedTable(name: table, schema: "dbo", database: db)
                }

                let temporal = try await self.client.temporal.listSystemVersionedTables(database: db)
                XCTAssertFalse(temporal.isEmpty, "Expected at least one system-versioned table")

                let found = temporal.first(where: { $0.name == table })
                XCTAssertNotNil(found, "Expected to find created temporal table")
                XCTAssertEqual(found?.schema, "dbo")
                XCTAssertFalse(found?.historyTable.isEmpty ?? true, "Expected history table name")
                XCTAssertEqual(found?.periodStartColumn, "ValidFrom")
                XCTAssertEqual(found?.periodEndColumn, "ValidTo")
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during temporal client test") }
            throw e
        }
    }

    @available(macOS 12.0, *)
    func testEnableDisableSystemVersioning() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_ed") { db in
                let table = "ed_\(UUID().uuidString.prefix(6))"
                try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.createSystemVersionedTable(name: table, schema: "dbo", database: db)
                }

                // Verify it starts as temporal
                var temporal = try await self.client.temporal.listSystemVersionedTables(database: db)
                XCTAssertTrue(temporal.contains(where: { $0.name == table }))

                // Disable
                try await self.client.temporal.disableSystemVersioning(database: db, schema: "dbo", table: table)

                temporal = try await self.client.temporal.listSystemVersionedTables(database: db)
                XCTAssertFalse(temporal.contains(where: { $0.name == table }), "Table should no longer be system-versioned")

                // Re-enable
                try await self.client.temporal.enableSystemVersioning(database: db, schema: "dbo", table: table)

                temporal = try await self.client.temporal.listSystemVersionedTables(database: db)
                XCTAssertTrue(temporal.contains(where: { $0.name == table }), "Table should be system-versioned again")
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during temporal enable/disable test") }
            throw e
        }
    }

    @available(macOS 12.0, *)
    func testTableMetadataTemporalFields() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_mt") { db in
                let table = "mt_\(UUID().uuidString.prefix(6))"
                try await withDbConnection(client: self.client, database: db) { conn in
                    try await conn.createSystemVersionedTable(name: table, schema: "dbo", database: db)
                }

                let tables = try await self.client.metadata.listTables(database: db, schema: "dbo", includeComments: false)
                let found = tables.first(where: { $0.name == table })
                XCTAssertNotNil(found, "Expected to find table in metadata listing")
                XCTAssertTrue(found?.isSystemVersioned ?? false, "Expected isSystemVersioned = true")
                XCTAssertEqual(found?.temporalType, 2)
                XCTAssertNotNil(found?.historyTableName)
                XCTAssertEqual(found?.periodStartColumn, "ValidFrom")
                XCTAssertEqual(found?.periodEndColumn, "ValidTo")

                // Find the history table
                let historyTable = tables.first(where: { $0.isHistoryTable && $0.name.contains(table) })
                if let historyTable {
                    XCTAssertTrue(historyTable.isHistoryTable)
                    XCTAssertEqual(historyTable.temporalType, 1)
                }
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during metadata temporal test") }
            throw e
        }
    }

    // MARK: - In-Memory OLTP

    @available(macOS 12.0, *)
    func testListMemoryOptimizedTablesEmpty() async throws {
        // Most test servers don't have memory-optimized filegroups.
        // Verify the query runs without error and returns an array.
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_mo") { db in
                let tables = try await self.client.temporal.listMemoryOptimizedTables(database: db)
                // Just verify it returns without error — may be empty
                XCTAssertTrue(tables.isEmpty || tables.allSatisfy { $0.durability == .schemaAndData || $0.durability == .schemaOnly })
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during in-memory test") }
            throw e
        }
    }

    @available(macOS 12.0, *)
    func testTableMetadataMemoryOptimizedFields() async throws {
        // Verify that non-memory-optimized tables have isMemoryOptimized = false
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_mm") { db in
                let tables = try await self.client.metadata.listTables(database: db, schema: "dbo")
                for table in tables where table.kind == .table {
                    XCTAssertFalse(table.isMemoryOptimized, "Regular table should not be memory-optimized")
                    XCTAssertNil(table.durabilityDescription)
                }
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during metadata memory test") }
            throw e
        }
    }

    // MARK: - Add Period Columns + Enable Versioning

    @available(macOS 12.0, *)
    func testAddPeriodColumnsAndEnableVersioning() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_ap") { db in
                let table = "ap_\(UUID().uuidString.prefix(6))"

                // Create a plain table with a primary key
                try await withDbConnection(client: self.client, database: db) { connection in
                    _ = try await connection.execute("""
                    CREATE TABLE [dbo].[\(table)] (
                        id INT NOT NULL PRIMARY KEY,
                        name NVARCHAR(100)
                    )
                    """)
                }

                // Add period columns and enable versioning via API
                try await self.client.temporal.addPeriodColumnsAndEnableVersioning(
                    database: db,
                    schema: "dbo",
                    table: table,
                    historySchema: "dbo",
                    historyTable: "\(table)_History"
                )

                // Verify it's now system-versioned
                let temporal = try await self.client.temporal.listSystemVersionedTables(database: db)
                let found = temporal.first(where: { $0.name == table })
                XCTAssertNotNil(found, "Table should be system-versioned")
                XCTAssertEqual(found?.historyTable, "\(table)_History")
                XCTAssertEqual(found?.periodStartColumn, "ValidFrom")
                XCTAssertEqual(found?.periodEndColumn, "ValidTo")

                // Clean up: disable versioning first (required before drop)
                try await self.client.temporal.disableSystemVersioning(database: db, schema: "dbo", table: table)
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during add period columns test") }
            throw e
        }
    }
}
