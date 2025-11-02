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
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        _ = try await client?.shutdownGracefully().get()
        if let group = group {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                group.shutdownGracefully { error in
                    if let error { cont.resume(throwing: error) } else { cont.resume(returning: ()) }
                }
            }
        }
        client = nil
        group = nil
    }

    private func deep() -> Bool { env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1" }

    @available(macOS 12.0, *)
    func testSerializableRangeLockBlocksInsert() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "txmx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                // Create table using SQLServerAdministrationClient
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(5)))))
                ]
                try await adminClient.createTable(name: "T", columns: columns)

                // Insert initial data
                _ = try await dbClient.execute("INSERT INTO [dbo].[T] (id, category) VALUES (1, N'A'), (2, N'B')").get()

                let holder = Task {
                    try await dbClient.withConnection { conn in
                        _ = try await conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").get()
                        try await conn.beginTransaction()
                        _ = try await conn.query("SELECT COUNT(*) FROM [dbo].[T] WHERE category = N'A'").get()
                        try await Task.sleep(nanoseconds: 600_000_000)
                        try await conn.commit()
                    }
                }

                try await Task.sleep(nanoseconds: 150_000_000)
                let elapsed = try await dbClient.withConnection { conn in
                    let start = DispatchTime.now()
                    _ = try await conn.execute("INSERT INTO [dbo].[T] (id, category) VALUES (3, N'A')").get()
                    return DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
                }
                _ = try? await withTimeout(5) { try await holder.value }
                XCTAssertGreaterThan(elapsed, 300_000_000 as UInt64, "INSERT should have been blocked until SERIALIZABLE txn finished")
            }
        }
    }

    @available(macOS 12.0, *)
    func testReadCommittedSelectBlocksOnWriter() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "txmx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                // Create table using SQLServerAdministrationClient
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
                ]
                try await adminClient.createTable(name: "T", columns: columns)

                // Insert initial data
                _ = try await dbClient.execute("INSERT INTO [dbo].[T] (id, value) VALUES (1, N'Original')").get()

                let writer = Task {
                    try await dbClient.withConnection { conn in
                        try await conn.beginTransaction()
                        _ = try await conn.execute("UPDATE [dbo].[T] SET value = N'Updated' WHERE id = 1").get()
                        try await Task.sleep(nanoseconds: 600_000_000)
                        try await conn.rollback()
                    }
                }

                try await Task.sleep(nanoseconds: 100_000_000)
                // Under READ COMMITTED (default), the reader is blocked until writer commits/rolls back
                let elapsed = try await dbClient.withConnection { conn in
                    let start = DispatchTime.now()
                    _ = try await conn.query("SELECT value FROM [dbo].[T] WHERE id = 1").get()
                    return DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
                }
                _ = try? await withTimeout(5) { try await writer.value }
                XCTAssertGreaterThan(elapsed, 300_000_000 as UInt64, "READ COMMITTED reader should block on writer")
            }
        }
    }
}
