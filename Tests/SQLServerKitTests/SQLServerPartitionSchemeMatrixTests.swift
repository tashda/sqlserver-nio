@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerPartitionSchemeMatrixTests: XCTestCase {
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
    }

    @available(macOS 12.0, *)
    func testPartitionSchemeOnTableAndIndex() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "psmx") { db in
            let pf = "pfInt_\(UUID().uuidString.prefix(6))"
            let ps = "psInt_\(UUID().uuidString.prefix(6))"
            try? await withRetry(attempts: 3) {
                _ = try await executeInDb(client: self.client, database: db, "CREATE PARTITION FUNCTION [\(pf)] (INT) AS RANGE RIGHT FOR VALUES (100, 1000);")
            }
            try? await withRetry(attempts: 3) {
                _ = try await executeInDb(client: self.client, database: db, "CREATE PARTITION SCHEME [\(ps)] AS PARTITION [\(pf)] ALL TO ([PRIMARY]);")
            }
            let table = "ps_tbl_\(UUID().uuidString.prefix(6))"
            try? await withRetry(attempts: 3) {
                _ = try await executeInDb(client: self.client, database: db, """
                    CREATE TABLE [dbo].[\(table)] (
                        [Id] INT NOT NULL,
                        [Code] INT NOT NULL,
                        CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([Id])
                    ) ON [\(ps)]([Id]);
                """)
            }
            // Create nonclustered index stored on the partition scheme by another column
            let ix = "ix_ps_\(UUID().uuidString.prefix(6))"
            try? await withRetry(attempts: 3) {
                _ = try await executeInDb(client: self.client, database: db, "CREATE NONCLUSTERED INDEX [\(ix)] ON [dbo].[\(table)] ([Code]) ON [\(ps)]([Code]);")
            }

            let def = try await withReliableConnection(client: self.client, attempts: 5) { conn in
                _ = try await conn.changeDatabase(db).get()
                return try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
            }
            guard let def, let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("ON [\(ps)]([Id])"), "Table storage clause should target partition scheme")
            XCTAssertTrue(ddl.contains("CREATE NONCLUSTERED INDEX [\(ix)]"), "Index should be scripted")
            XCTAssertTrue(ddl.contains("ON [\(ps)]"), "Index storage clause should target partition scheme")
        }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Skipping due to server closing connections during partition scheme test") }
            throw e
        }
    }
}
