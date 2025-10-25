@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTransactionIsolationMatrixTests: XCTestCase {
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
        client = nil
        group = nil
    }

    private func deep() -> Bool { env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1" }

    @available(macOS 12.0, *)
    func testSerializableRangeLockBlocksInsert() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "txmx") { db in
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[T] (id INT PRIMARY KEY, category NVARCHAR(5));")
            _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [dbo].[T] (id, category) VALUES (1, N'A'), (2, N'B');")

            let holder = Task {
                try await withDbConnection(client: self.client, database: db) { conn in
                    _ = try await conn.underlying.rawSql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").get()
                    _ = try await conn.underlying.rawSql("BEGIN TRANSACTION").get()
                    _ = try await conn.underlying.rawSql("SELECT COUNT(*) FROM [dbo].[T] WHERE category = N'A'").get()
                    try await Task.sleep(nanoseconds: 600_000_000)
                    _ = try await conn.underlying.rawSql("COMMIT").get()
                }
            }

            try await Task.sleep(nanoseconds: 150_000_000)
            let elapsed = try await withDbConnection(client: self.client, database: db) { conn in
                let start = DispatchTime.now()
                _ = try await conn.underlying.rawSql("INSERT INTO [dbo].[T] (id, category) VALUES (3, N'A')").get()
                return DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            }
            _ = try? await holder.value
            XCTAssertGreaterThan(elapsed, 300_000_000 as UInt64, "INSERT should have been blocked until SERIALIZABLE txn finished")
        }
    }

    @available(macOS 12.0, *)
    func testReadCommittedSelectBlocksOnWriter() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "txmx") { db in
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[T] (id INT PRIMARY KEY, value NVARCHAR(20));")
            _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [dbo].[T] (id, value) VALUES (1, N'Original');")

            let writer = Task {
                try await withDbConnection(client: self.client, database: db) { conn in
                    _ = try await conn.underlying.rawSql("BEGIN TRANSACTION").get()
                    _ = try await conn.underlying.rawSql("UPDATE [dbo].[T] SET value = N'Updated' WHERE id = 1").get()
                    try await Task.sleep(nanoseconds: 600_000_000)
                    _ = try await conn.underlying.rawSql("ROLLBACK").get()
                }
            }

            try await Task.sleep(nanoseconds: 100_000_000)
            // Under READ COMMITTED (default), the reader is blocked until writer commits/rolls back
            let elapsed = try await withDbConnection(client: self.client, database: db) { conn in
                let start = DispatchTime.now()
                _ = try await conn.underlying.rawSql("SELECT value FROM [dbo].[T] WHERE id = 1").get()
                return DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            }
            _ = try? await writer.value
            XCTAssertGreaterThan(elapsed, 300_000_000 as UInt64, "READ COMMITTED reader should block on writer")
        }
    }
}
