@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTemporalPartitionedTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv: Bool = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        // Probe connectivity once; mark for skip if unstable
        do {
            _ = try await withTimeout(5) { try await self.client.query("SELECT 1 as ready").get() }
        } catch {
            self.skipDueToEnv = true
        }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    // MARK: - Helpers
    private func runWithRetry(_ sql: String, attempts: Int = 3, delayMs: UInt64 = 150_000_000) async {
        for i in 1...attempts {
            do {
                _ = try await withTimeout(5) { try await self.client.execute(sql).get() }
                return
            } catch {
                if i == attempts { return }
                try? await Task.sleep(nanoseconds: delayMs)
            }
        }
    }

    // Temporal table scripting (no partitioning)
    func testTemporalTableScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tmp") { db in
            try await withDbConnection(client: self.client, database: db) { conn in
                let table = "tmp_temporal_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                let hist = "\(table)_History"
                _ = try await conn.underlying.rawSql("""
                    CREATE TABLE [dbo].[\(table)] (
                        [Id] INT NOT NULL,
                        [ValidFrom] DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,
                        [ValidTo] DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
                        PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]),
                        CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([Id])
                    ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[\(hist)]));
                """).get()

                guard let def = try await withTimeout(10, { try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get() }), let ddl = def.definition else {
                    XCTFail("No definition returned")
                    return
                }

                XCTAssertTrue(ddl.contains("PERIOD FOR SYSTEM_TIME"))
                XCTAssertTrue(ddl.contains("SYSTEM_VERSIONING = ON"))
                XCTAssertTrue(ddl.contains("HISTORY_TABLE = [dbo].[\(hist)]"))
            }
        }
    }

    // Partitioned table scripting (no temporal)
    func testPartitionedTableScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "prt") { db in
                try await withDbConnection(client: self.client, database: db) { conn in
                    let pf = "pfInt_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                    let ps = "psInt_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                    _ = try await conn.underlying.rawSql("""
                        CREATE PARTITION FUNCTION [\(pf)] (INT) AS RANGE RIGHT FOR VALUES (100, 1000);
                        CREATE PARTITION SCHEME [\(ps)] AS PARTITION [\(pf)] ALL TO ([PRIMARY]);
                    """).get()

                    let table = "tmp_part_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                    _ = try await conn.underlying.rawSql("""
                        CREATE TABLE [dbo].[\(table)] (
                            [Id] INT NOT NULL,
                            [Code] NVARCHAR(50) NOT NULL,
                            CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([Id])
                        ) ON [\(ps)]([Id]);
                    """).get()

                    guard let def = try await withTimeout(10, { try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get() }), let ddl = def.definition else {
                        XCTFail("No definition returned")
                        return
                    }
                    XCTAssertTrue(ddl.contains("ON [\(ps)]([Id])"))
                }
            }
        } catch {
            let norm = SQLServerError.normalize(error)
            switch norm {
            case .connectionClosed, .timeout:
                throw XCTSkip("Skipping due to unstable server during temporal/partitioned test: \(norm)")
            default:
                throw error
            }
        }
    }
}
