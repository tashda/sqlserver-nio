@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerColumnstoreIndexTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testColumnstoreIndexScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "csmx") { db in
            let table = "cs_tbl_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)] (Id INT NOT NULL, C1 INT, C2 INT);")
            let ccs = "CCS_\(UUID().uuidString.prefix(6))"
            let nccs = "NCCS_\(UUID().uuidString.prefix(6))"
            // Clustered columnstore index
            _ = try await executeInDb(client: self.client, database: db, "CREATE CLUSTERED COLUMNSTORE INDEX [\(ccs)] ON [dbo].[\(table)];")
            // Nonclustered columnstore index
            _ = try await executeInDb(client: self.client, database: db, "CREATE NONCLUSTERED COLUMNSTORE INDEX [\(nccs)] ON [dbo].[\(table)] (C1, C2);")

            guard let def = try await withDbConnection(client: self.client, database: db, { conn in
                try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
            }), let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("CLUSTERED COLUMNSTORE"), "Expected clustered columnstore index in script")
            XCTAssertTrue(ddl.contains("NONCLUSTERED COLUMNSTORE"), "Expected nonclustered columnstore index in script")
        }
    }
}
