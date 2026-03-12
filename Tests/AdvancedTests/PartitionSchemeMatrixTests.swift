@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerPartitionSchemeMatrixTests: XCTestCase, @unchecked Sendable {
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
    func testPartitionSchemeOnTableAndIndex() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "psmx") { db in
            let pf = "pfInt_\(UUID().uuidString.prefix(6))"
            let ps = "psInt_\(UUID().uuidString.prefix(6))"
            let table = "ps_tbl_\(UUID().uuidString.prefix(6))"
            let ix = "ix_ps_\(UUID().uuidString.prefix(6))"
            try await withDbClient(for: db) { dbClient in
                let indexClient = SQLServerIndexClient(client: dbClient)
                try await withDbConnection(client: dbClient, database: db) { connection in
                    try await connection.createPartitionFunction(name: String(pf), dataType: .int, values: ["100", "1000"])
                    try await connection.createPartitionScheme(name: String(ps), functionName: String(pf))
                    try await connection.createPartitionedTable(
                        name: String(table),
                        columns: [
                            SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                            SQLServerColumnDefinition(name: "Code", definition: .standard(.init(dataType: .int)))
                        ],
                        partitionScheme: String(ps),
                        partitionColumn: "Id",
                        database: db
                    )
                }
                try await indexClient.createIndex(
                    name: String(ix),
                    table: String(table),
                    columns: [IndexColumn(name: "Code")],
                    options: IndexOptions(partitionScheme: String(ps), partitionColumns: ["Code"])
                )
            }

            let def = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.objectDefinition(schema: "dbo", name: table, kind: .table)
            }
            guard let def, let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("ON [\(ps)]([Id])"), "Table storage clause should target partition scheme")
            XCTAssertTrue(ddl.contains("CREATE NONCLUSTERED INDEX [\(ix)]"), "Index should be scripted")
            XCTAssertTrue(ddl.contains("ON [\(ps)]"), "Index storage clause should target partition scheme")
        }
    }
}
