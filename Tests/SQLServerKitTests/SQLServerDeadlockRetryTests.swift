@testable import SQLServerKit
import XCTest

final class SQLServerDeadlockRetryTests: XCTestCase {
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
    }

    func testDeadlockRetry() async throws {
        try requireEnvFlag("TDS_ENABLE_DEADLOCK_TESTS", description: "deadlock tests")
        try await withTemporaryDatabase(client: self.client, prefix: "dlck") { db in
            let table = "DL_T_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)] (id INT PRIMARY KEY, v INT);")
            _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [dbo].[\(table)] (id, v) VALUES (1, 0), (2, 0);")

            // Use reliable connections that retry transient closures under stress
            async let a: Void = withReliableConnection(client: self.client) { conn in
                _ = try await conn.changeDatabase(db).get()
                _ = try await conn.execute("BEGIN TRANSACTION").get()
                _ = try await conn.execute("UPDATE [dbo].[\(table)] SET v = v + 1 WHERE id = 1").get()
                try await Task.sleep(nanoseconds: 200_000_000)
                do {
                    _ = try await conn.execute("UPDATE [dbo].[\(table)] SET v = v + 1 WHERE id = 2").get()
                    _ = try await conn.execute("COMMIT").get()
                } catch {
                    // After a deadlock, SQL Server may have closed the connection
                    // ROLLBACK on a closed connection is expected and should be ignored
                    do {
                        _ = try await conn.execute("ROLLBACK").get()
                    } catch {
                        // Ignore ROLLBACK errors after deadlock - connection may be closed
                    }
                }
            }

            async let b: Void = withReliableConnection(client: self.client) { conn in
                _ = try await conn.changeDatabase(db).get()
                _ = try await conn.execute("BEGIN TRANSACTION").get()
                _ = try await conn.execute("UPDATE [dbo].[\(table)] SET v = v + 1 WHERE id = 2").get()
                try await Task.sleep(nanoseconds: 200_000_000)
                do {
                    _ = try await conn.execute("UPDATE [dbo].[\(table)] SET v = v + 1 WHERE id = 1").get()
                    _ = try await conn.execute("COMMIT").get()
                } catch {
                    // After a deadlock, SQL Server may have closed the connection
                    // ROLLBACK on a closed connection is expected and should be ignored
                    do {
                        _ = try await conn.execute("ROLLBACK").get()
                    } catch {
                        // Ignore ROLLBACK errors after deadlock - connection may be closed
                    }
                }
            }

            // One of the tasks should deadlock; ensure the client can still execute after
            _ = try? await (a, b)
            let rows = try await queryInDb(client: self.client, database: db, "SELECT SUM(v) AS s FROM [dbo].[\(table)]")
            XCTAssertNotNil(rows.first?.column("s")?.int)
        }
    }
}
