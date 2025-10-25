@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTemporalMatrixTests: XCTestCase {
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
    func testTemporalDefaultHistoryScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tmpx") { db in
            let table = "temporal_def_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(table)] (
                    [Id] INT NOT NULL,
                    [ValidFrom] DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,
                    [ValidTo] DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
                    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),
                    CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([Id])
                ) WITH (SYSTEM_VERSIONING = ON);
            """)
            guard let def = try await withDbConnection(client: self.client, database: db, { conn in
                try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
            }), let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("PERIOD FOR SYSTEM_TIME"))
            XCTAssertTrue(ddl.contains("SYSTEM_VERSIONING = ON"))
        }
    }

    @available(macOS 12.0, *)
    func testTemporalExplicitHistoryScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tmph") { db in
            let table = "temporal_exp_\(UUID().uuidString.prefix(6))"
            let hist = "\(table)_History"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(table)] (
                    [Id] INT NOT NULL,
                    [ValidFrom] DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,
                    [ValidTo] DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
                    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),
                    CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([Id])
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[\(hist)]));
            """)
            guard let def = try await withDbConnection(client: self.client, database: db, { conn in
                try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
            }), let ddl = def.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl.contains("PERIOD FOR SYSTEM_TIME"))
            XCTAssertTrue(ddl.contains("HISTORY_TABLE = [dbo].[\(hist)]"))
        }
    }
}
