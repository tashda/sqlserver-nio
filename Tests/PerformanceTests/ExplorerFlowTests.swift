@testable import SQLServerKit
import XCTest
import NIO
import Logging

/// End-to-end flow that mirrors a typical client explorer startup:
/// - Connect
/// - List databases
/// - Choose a target database
/// - List schemas in target
/// - For a subset of schemas: list tables/views with comments
/// - For a subset of tables/views: list columns with comments, primary keys, indexes, triggers
/// - List procedures/functions with comments
/// - Optionally fetch an object definition
///
/// This test validates the complete metadata exploration flow.
final class SQLServerExplorerFlowTests: XCTestCase {
    private var connection: SQLServerConnection!
    private var group: EventLoopGroup!

    private var TIMEOUT: TimeInterval {
        if let s = env("TDS_TEST_OPERATION_TIMEOUT_SECONDS"), let v = TimeInterval(s) { return v }
        return 90
    }

    override func setUp() async throws {
        try await super.setUp()
        _ = isLoggingConfigured
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

      
        // Create event loop group
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        // Bound the initial connect with the same global TIMEOUT used by the rest of the test.
        connection = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
    }

    override func tearDown() async throws {
        // Ensure connection is fully closed before shutting down the EventLoopGroup.
        if let conn = connection {
            _ = try? await conn.close().get()
        }
        connection = nil

        // Shut down event loop group
        try await group?.shutdownGracefully()
        group = nil

        try await super.tearDown()
    }

    func testExplorerStartupFlow() async throws {
        // 1) List databases
        let databases = try await withTimeout(TIMEOUT) {
            try await self.connection.listDatabases().get()
        }
        XCTAssertFalse(databases.isEmpty, "Expected at least one database")

        // Prefer configured database; else a known sample; else first user database; else master
        let configured = env("TDS_DATABASE")
        let availableNames = Set(databases.map { $0.name })
        let targetDatabase = configured
            ?? (availableNames.contains("AdventureWorks2022") ? "AdventureWorks2022" : nil)
            ?? databases.first?.name
            ?? "master"

        // 2) List schemas in target database
        let schemas = try await withTimeout(TIMEOUT) {
            try await self.connection.listSchemas(in: targetDatabase).get()
        }
        // It is valid to have zero user schemas (system-only), but calls must complete
        // Limit further work to a few schemas for runtime parity
        let schemaSubset = Array(schemas.prefix(3))

        // 3) For each schema, list tables/views with comments
        var allTables: [TableMetadata] = []
        for s in schemaSubset {
            let tables = try await withTimeout(TIMEOUT) {
                try await self.connection.listTables(database: targetDatabase, schema: s.name, includeComments: true).get()
            }
            allTables.append(contentsOf: tables)
        }

        // 4) For a subset of tables/views: columns (+comments), PKs, indexes, triggers
        let tableSubset = Array(allTables.prefix(3))
        for t in tableSubset {
            let cols = try await withTimeout(TIMEOUT) {
                try await self.connection.listColumns(
                    database: targetDatabase,
                    schema: t.schema,
                    table: t.name,
                    objectTypeHint: t.type,
                    includeComments: true
                ).get()
            }
            // Calls should succeed; some tables may have zero columns in degenerate cases, but don't assert non-empty.
            _ = cols

            let pks = try await withTimeout(TIMEOUT) {
                try await self.connection.listPrimaryKeys(database: targetDatabase, schema: t.schema, table: t.name).get()
            }
            _ = pks

            let indexes = try await withTimeout(TIMEOUT) {
                try await self.connection.listIndexes(database: targetDatabase, schema: t.schema, table: t.name).get()
            }
            _ = indexes

            let triggers = try await withTimeout(TIMEOUT) {
                try await self.connection.listTriggers(database: targetDatabase, schema: t.schema, table: t.name, includeComments: true).get()
            }
            _ = triggers
        }

        // 5) Routines: procedures and functions with comments
        // Use at most first schema to limit runtime
        if let firstSchema = schemaSubset.first?.name {
            let procs = try await withTimeout(TIMEOUT) {
                try await self.connection.listProcedures(database: targetDatabase, schema: firstSchema, includeComments: true).get()
            }
            _ = procs

            let funcs = try await withTimeout(TIMEOUT) {
                try await self.connection.listFunctions(database: targetDatabase, schema: firstSchema, includeComments: true).get()
            }
            _ = funcs
        }

        // Optional: fetch a definition for any object discovered
        // TODO: Implement fetchObjectDefinition method
        /*
        if let anyTable = allTables.first {
            let def = try await withTimeout(TIMEOUT) {
                try await self.connection.fetchObjectDefinition(
                    database: targetDatabase,
                    schema: anyTable.schema,
                    name: anyTable.name,
                    kind: ObjectKind.table
                ).get()
            }
            _ = def
        }
        */
    }

    func testExplorerStartupFlowViaClient() async throws {
        // Build a pooled client on the same group
        let client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()

        // 1) List databases
        let databases = try await withTimeout(TIMEOUT) {
            try await client.listDatabases().get()
        }
        XCTAssertFalse(databases.isEmpty, "Expected at least one database")

        // Choose target
        let configured = env("TDS_DATABASE")
        let availableNames = Set(databases.map { $0.name })
        let targetDatabase = configured
            ?? (availableNames.contains("AdventureWorks2022") ? "AdventureWorks2022" : nil)
            ?? databases.first?.name
            ?? "master"

        // 2) List schemas in target database
        let schemas = try await withTimeout(TIMEOUT) {
            try await client.listSchemas(in: targetDatabase).get()
        }
        let schemaSubset = Array(schemas.prefix(3))

        // 3) For each schema, list tables/views with comments
        var allTables: [TableMetadata] = []
        for s in schemaSubset {
            let tables = try await withTimeout(TIMEOUT) {
                try await client.listTables(database: targetDatabase, schema: s.name, includeComments: true).get()
            }
            allTables.append(contentsOf: tables)
        }

        // 4) For a subset of tables/views, gather detailed metadata
        let tableSubset = Array(allTables.prefix(3))
        for t in tableSubset {
            let cols = try await withTimeout(TIMEOUT) {
                try await client.listColumns(
                    database: targetDatabase,
                    schema: t.schema,
                    table: t.name,
                    objectTypeHint: t.type,
                    includeComments: true
                ).get()
            }
            _ = cols

            let pks = try await withTimeout(TIMEOUT) {
                try await client.listPrimaryKeys(database: targetDatabase, schema: t.schema, table: t.name).get()
            }
            _ = pks

            let indexes = try await withTimeout(TIMEOUT) {
                try await client.listIndexes(database: targetDatabase, schema: t.schema, table: t.name).get()
            }
            _ = indexes

            let triggers = try await withTimeout(TIMEOUT) {
                try await client.listTriggers(database: targetDatabase, schema: t.schema, table: t.name, includeComments: true).get()
            }
            _ = triggers
        }

        // 5) Routines on first schema
        if let firstSchema = schemaSubset.first?.name {
            let procs = try await withTimeout(TIMEOUT) {
                try await client.listProcedures(database: targetDatabase, schema: firstSchema, includeComments: true).get()
            }
            _ = procs

            let funcs = try await withTimeout(TIMEOUT) {
                try await client.listFunctions(database: targetDatabase, schema: firstSchema, includeComments: true).get()
            }
            _ = funcs
        }

        // 6) Optional object definition via client facade
        // TODO: Implement fetchObjectDefinition method
        /*
        if let anyTable = allTables.first {
            let def = try await withTimeout(TIMEOUT) {
                try await client.fetchObjectDefinition(
                    database: targetDatabase,
                    schema: anyTable.schema,
                    name: anyTable.name,
                    kind: ObjectKind.table
                ).get()
            }
            _ = def
        }
        */

        // Cleanup
        _ = try? await client.shutdownGracefully().get()
    }
}
