@testable import SQLServerKit
import XCTest
import NIO

final class SQLServerPrimaryKeySchemaEnumerationTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testListPrimaryKeysBySchemaDoesNotRequireTableName() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "pkschema") { db in
            let table = "pk_" + UUID().uuidString.prefix(8)
            // Simple table with clustered PK to verify metadata shape
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(table)] (
                    id INT NOT NULL,
                    name NVARCHAR(50) NOT NULL,
                    CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([id])
                );
            """)

            // When querying by schema only, we should still receive our PK without error
            let bySchema = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listPrimaryKeys(schema: "dbo").get()
            }
            XCTAssertTrue(bySchema.contains(where: { $0.schema.caseInsensitiveCompare("dbo") == .orderedSame && $0.table.caseInsensitiveCompare(String(table)) == .orderedSame }),
                          "Expected primary key for \(table) when listing by schema only")

            // And querying with explicit table returns a single matching PK entry marked as clustered
            let byTable = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listPrimaryKeys(schema: "dbo", table: String(table)).get()
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
            for (index, name) in tables.enumerated() {
                let constraint = "PK_\(name)"
                _ = try await executeInDb(client: self.client, database: db, """
                    CREATE TABLE [dbo].[\(name)] (
                        id INT NOT NULL,
                        payload NVARCHAR(50) NOT NULL,
                        CONSTRAINT [\(constraint)] \(index == 0 ? "PRIMARY KEY CLUSTERED" : "PRIMARY KEY NONCLUSTERED") (id)
                    );
                """)
            }

            _ = try await executeInDb(client: self.client, database: db, """
                CREATE VIEW [dbo].[view_with_no_pk] AS SELECT id, payload FROM [dbo].[\(tables[0])];
            """)

            let metadata = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.listPrimaryKeys(database: db, schema: "dbo").get()
            }

            XCTAssertEqual(metadata.count, tables.count, "Expected one primary key per base table")
            let names = Set(metadata.map { $0.table })
            XCTAssertEqual(names, Set(tables))
            XCTAssertFalse(metadata.contains { $0.table.caseInsensitiveCompare("view_with_no_pk") == .orderedSame })
        }
    }
}
