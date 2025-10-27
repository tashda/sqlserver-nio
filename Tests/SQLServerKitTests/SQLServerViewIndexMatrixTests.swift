@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerViewIndexMatrixTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testIndexedViewScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "vimx") { db in
            // Base table
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[T] (id INT NOT NULL, amount INT NOT NULL);")
            // Schema-bound view (required for indexed view)
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE VIEW [dbo].[V] WITH SCHEMABINDING AS
                SELECT id, SUM(amount) AS total, COUNT_BIG(*) AS cnt
                FROM [dbo].[T]
                GROUP BY id;
            """)
            // Unique clustered index on view
            _ = try await executeInDb(client: self.client, database: db, "CREATE UNIQUE CLUSTERED INDEX [IXV] ON [dbo].[V](id);")

            // Fetch view scripting and assert index appears
            guard let def = try await withDbConnection(client: self.client, database: db, { conn in
                try await conn.fetchObjectDefinition(schema: "dbo", name: "V", kind: .view).get()
            }), let ddl = def.definition else { XCTFail("No view DDL returned"); return }
            XCTAssertTrue(ddl.contains("CREATE UNIQUE CLUSTERED INDEX [IXV]"), "Scripted view should include clustered index DDL")
        }
    }
}
