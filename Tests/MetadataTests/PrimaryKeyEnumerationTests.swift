import SQLServerKit
import SQLServerKitTesting
import XCTest

final class SQLServerPrimaryKeySchemaEnumerationTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    @available(macOS 12.0, *)
    func testListPrimaryKeysBySchemaDoesNotRequireTableName() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "pkschema") { db in
            let table = "pk_" + UUID().uuidString.prefix(8)
            try await withDbConnection(client: self.client, database: db) { connection in
                try await connection.createTable(
                    name: String(table),
                    columns: [
                        SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                        SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                    ]
                )
            }

            // When querying by schema only, we should still receive our PK without error
            let bySchema = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listPrimaryKeys(schema: "dbo")
            }
            XCTAssertTrue(bySchema.contains(where: { $0.schema.caseInsensitiveCompare("dbo") == .orderedSame && $0.table.caseInsensitiveCompare(String(table)) == .orderedSame }),
                          "Expected primary key for \(table) when listing by schema only")

            // And querying with explicit table returns a single matching PK entry marked as clustered
            let byTable = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listPrimaryKeys(schema: "dbo", table: String(table))
            }
            guard let pk = byTable.first(where: { $0.table.caseInsensitiveCompare(String(table)) == .orderedSame }) else {
                XCTFail("Missing primary key entry for \(table)")
                return
            }
            XCTAssertTrue(pk.isClustered, "Expected clustered PK for \(table)")
            XCTAssertEqual(pk.columns.map { $0.column }, ["id"]) // verify key column order
        }
    }

    @available(macOS 12.0, *)
    func testSchemaEnumerationSkipsViewsAndHandlesMultipleTables() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "pkschema2") { db in
            let tables = (0..<3).map { _ in "tbl_" + String(UUID().uuidString.prefix(8)) }
            try await withDbConnection(client: self.client, database: db) { connection in
                for (index, name) in tables.enumerated() {
                    try await connection.createTable(
                        name: name,
                        columns: [
                            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                            SQLServerColumnDefinition(name: "payload", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                        ]
                    )
                    if index != 0 {
                        try await connection.dropPrimaryKey(name: "PK_\(name)", table: name)
                        try await connection.addPrimaryKey(name: "PK_\(name)", table: name, columns: ["id"], clustered: false)
                    }
                }

                try await connection.createView(
                    name: "view_with_no_pk",
                    query: "SELECT id, payload FROM [dbo].[\(tables[0])]"
                )
            }

            let metadata = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listPrimaryKeys(database: db, schema: "dbo")
            }

            XCTAssertEqual(metadata.count, tables.count, "Expected one primary key per base table")
            let names = Set(metadata.map { $0.table })
            XCTAssertEqual(names, Set(tables))
            XCTAssertFalse(metadata.contains { $0.table.caseInsensitiveCompare("view_with_no_pk") == .orderedSame })
        }
    }
}
