@testable import SQLServerKit
import XCTest

final class SQLServerDeadlockRetryTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testDeadlockRetry() async throws {
        let dbName = "dlckdb_\(UUID().uuidString.prefix(8))"
        let table = "DL_T_\(UUID().uuidString.prefix(6))"
        let qualifiedTable = "[\(dbName)].[dbo].[\(table)]"

        let dropSql = """
        IF DB_ID(N'\(dbName)') IS NOT NULL
        BEGIN
            ALTER DATABASE [\(dbName)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [\(dbName)];
        END
        """

        _ = try await self.client.execute("CREATE DATABASE [\(dbName)];")
        _ = try await self.client.execute("CREATE TABLE \(qualifiedTable) (id INT PRIMARY KEY, v INT);")
        _ = try await self.client.execute("INSERT INTO \(qualifiedTable) (id, v) VALUES (1, 0), (2, 0);")

        do {
            // Use two independent connections that may deadlock and recover
            async let a: Void = self.client.withConnection { conn in
                _ = try await conn.execute("BEGIN TRANSACTION").get()
                _ = try await conn.execute("UPDATE \(qualifiedTable) SET v = v + 1 WHERE id = 1").get()
                try await Task.sleep(nanoseconds: 200_000_000)
                do {
                    _ = try await conn.execute("UPDATE \(qualifiedTable) SET v = v + 1 WHERE id = 2").get()
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

            async let b: Void = self.client.withConnection { conn in
                _ = try await conn.execute("BEGIN TRANSACTION").get()
                _ = try await conn.execute("UPDATE \(qualifiedTable) SET v = v + 1 WHERE id = 2").get()
                try await Task.sleep(nanoseconds: 200_000_000)
                do {
                    _ = try await conn.execute("UPDATE \(qualifiedTable) SET v = v + 1 WHERE id = 1").get()
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
            let rows = try await self.client.query("SELECT SUM(v) AS s FROM \(qualifiedTable)")
            XCTAssertNotNil(rows.first?.column("s")?.int)
        } catch {
            _ = try? await self.client.execute(dropSql)
            throw error
        }

        _ = try? await self.client.execute(dropSql)
    }
}
