import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerTemporalMatrixTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testTemporalDefaultHistoryScripting() async throws {
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "tmpx") { db in
            let table = "temporal_def_\(UUID().uuidString.prefix(6))"
            try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.createSystemVersionedTable(name: String(table), schema: "dbo", database: db)
            }
            let def = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.objectDefinition(schema: "dbo", name: table, kind: .table)
            }
            guard let def, let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("PERIOD FOR SYSTEM_TIME"))
            XCTAssertTrue(ddl.contains("SYSTEM_VERSIONING = ON"))
        }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Skipping due to server closing connections during temporal matrix (default history)") }
            throw e
        }
    }

    @available(macOS 12.0, *)
    func testTemporalExplicitHistoryScripting() async throws {
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "tmph") { db in
            let table = "temporal_exp_\(UUID().uuidString.prefix(6))"
            let hist = "\(table)_History"
            try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.createSystemVersionedTable(name: String(table), historyTableName: String(hist), schema: "dbo", database: db)
            }
            let def = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.objectDefinition(schema: "dbo", name: table, kind: .table)
            }
            guard let def, let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("PERIOD FOR SYSTEM_TIME"))
            XCTAssertTrue(ddl.contains("HISTORY_TABLE = [dbo].[\(hist)]"))
        }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Skipping due to server closing connections during temporal matrix (explicit history)") }
            throw e
        }
    }
}
