@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerForeignKeyCascadeMatrixTests: XCTestCase {
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

    private func deep() -> Bool { env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1" }

    @available(macOS 12.0, *)
    func testForeignKeyCascadeMatrix() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "fkmx") { db in
            let parent = "fk_parent_\(UUID().uuidString.prefix(6))"
            let child = "fk_child_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(parent)](id INT PRIMARY KEY, d INT DEFAULT 0);
                CREATE TABLE [dbo].[\(child)](id INT PRIMARY KEY, pid INT NULL DEFAULT 0, v NVARCHAR(10));
            """)
            struct Case { let del: String; let upd: String; let expectNullOnDelete: Bool; let expectDefaultOnDelete: Bool }
            var cases: [Case] = [
                Case(del: "NO ACTION", upd: "NO ACTION", expectNullOnDelete: false, expectDefaultOnDelete: false),
                Case(del: "CASCADE", upd: "NO ACTION", expectNullOnDelete: false, expectDefaultOnDelete: false),
                Case(del: "SET NULL", upd: "NO ACTION", expectNullOnDelete: true, expectDefaultOnDelete: false),
            ]
            if self.deep() { cases.append(Case(del: "SET DEFAULT", upd: "NO ACTION", expectNullOnDelete: false, expectDefaultOnDelete: true)) }

            for (i, c) in cases.enumerated() {
                let fk = "FK_\(i)_\(UUID().uuidString.prefix(4))"
                _ = try await executeInDb(client: self.client, database: db, "ALTER TABLE [dbo].[\(child)] ADD CONSTRAINT [\(fk)] FOREIGN KEY(pid) REFERENCES [dbo].[\(parent)](id) ON DELETE \(c.del) ON UPDATE \(c.upd);")
                // Seed
                _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [dbo].[\(parent)](id) VALUES (1);")
                _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [dbo].[\(child)](id, pid, v) VALUES (11, 1, N'x');")
                // Delete parent
                _ = try await executeInDb(client: self.client, database: db, "DELETE FROM [dbo].[\(parent)] WHERE id = 1;")
                // Check child
                let rows = try await queryInDb(client: self.client, database: db, "SELECT COUNT(*) AS cnt, SUM(CASE WHEN pid IS NULL THEN 1 ELSE 0 END) AS nulls, SUM(CASE WHEN pid = 0 THEN 1 ELSE 0 END) AS defs FROM [dbo].[\(child)]")
                guard let r = rows.first else { XCTFail("Missing row"); continue }
                let cnt = r.column("cnt")?.int ?? 0
                let nulls = r.column("nulls")?.int ?? 0
                let defs = r.column("defs")?.int ?? 0
                if c.del == "CASCADE" { XCTAssertEqual(cnt, 0, "CASCADE should remove child") }
                if c.expectNullOnDelete { XCTAssertEqual(nulls, 1, "SET NULL should null pid") }
                if c.expectDefaultOnDelete { XCTAssertEqual(defs, 1, "SET DEFAULT should set pid=0") }
                // Reset child table for next case
                _ = try await executeInDb(client: self.client, database: db, "TRUNCATE TABLE [dbo].[\(child)]")
            }
        }
    }
}
