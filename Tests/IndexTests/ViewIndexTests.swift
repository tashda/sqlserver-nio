@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO
import Logging

final class SQLServerViewIndexMatrixTests: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { throw error }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testIndexedViewScripting() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "vimx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let dbAdminClient = SQLServerAdministrationClient(client: dbClient)
                let viewClient = SQLServerViewClient(client: dbClient)

                // Base table using SQLServerKit APIs
                let tableName = "view_test_table_\(UUID().uuidString.prefix(6))"
                let viewName = "test_view_\(UUID().uuidString.prefix(6))"
                let indexName = "ix_\(UUID().uuidString.prefix(6))"

                let tableColumns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
                    SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .int)))
                ]
                try await dbAdminClient.createTable(name: tableName, columns: tableColumns)

                // Schema-bound view with unique clustered index using SQLServerKit APIs
                try await viewClient.createIndexedView(
                    name: viewName,
                    query: """
                        SELECT id, SUM(amount) AS total, COUNT_BIG(*) AS cnt
                        FROM [dbo].[\(tableName)]
                        GROUP BY id
                    """,
                    indexName: indexName,
                    indexColumns: ["id"]
                )

                // Fetch view scripting and assert index appears
                guard let def = try await withDbConnection(client: self.client, database: db, operation: { conn in
                    try await conn.objectDefinition(schema: "dbo", name: viewName, kind: .view)
                }), let ddl = def.definition else { XCTFail("No view DDL returned"); return }
                XCTAssertTrue(ddl.contains("CREATE UNIQUE CLUSTERED INDEX [\(indexName)]"), "Scripted view should include clustered index DDL")
            }
        }
    }
}
