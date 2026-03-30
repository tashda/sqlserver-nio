import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO
import Dispatch

final class TransactionConcurrencyTests: TransactionTestBase, @unchecked Sendable {
    func testConcurrentTransactions() async throws {
        let tableName = "tx_con_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        await withTaskGroup(of: Void.self) { group in
            for i in 1...5 {
                group.addTask {
                    do {
                        try await self.client.withConnection { connection in
                            try await connection.beginTransaction()
                            try await connection.insertRow(into: tableName, values: ["id": .int(i), "value": .nString("Value\(i)")])
                            try await connection.commit()
                        }
                    } catch {
                        XCTFail("Concurrent transaction failed: \(error)")
                    }
                }
            }
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        }
        XCTAssertEqual(result.first?.column("count")?.int, 5)
    }

    func testTransactionWithDeadlock() async throws {
        let t1 = "tx_dead1_\(UUID().uuidString.prefix(8))"
        let t2 = "tx_dead2_\(UUID().uuidString.prefix(8))"

        try await self.client.withConnection { connection in
            let columns = [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
            try await connection.createTable(name: t1, columns: columns)
            try await connection.createTable(name: t2, columns: columns)
            try await connection.insertRow(into: t1, values: ["id": .int(1), "value": .nString("T1")])
            try await connection.insertRow(into: t2, values: ["id": .int(1), "value": .nString("T2")])
        }

        try await self.client.withConnection { connection1 in
            try await connection1.beginTransaction()
            try await connection1.updateRows(in: t1, set: ["value": .nString("M1")], where: "id = 1")
            try await connection1.commit()
        }

        try await self.client.withConnection { connection2 in
            try await connection2.beginTransaction()
            try await connection2.updateRows(in: t2, set: ["value": .nString("M2")], where: "id = 1")
            try await connection2.commit()
        }
    }

    func testTransactionTimeout() async throws {
        let tableName = "tx_timeout_\(UUID().uuidString.prefix(8))"

        try await self.client.withConnection { connection in
            try await connection.createTable(
                name: tableName,
                columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
            )
        }

        try await self.client.withConnection { connection in
            try await connection.setLockTimeout(milliseconds: 1000)
            try await connection.beginTransaction()
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Test")])
            try await connection.commit()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1)
    }

    func testConcurrentUpdateBlocksUntilCommit() async throws {
        let tableName = "tx_blk_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        try await self.client.withConnection { connection in
            try await connection.createTable(
                name: tableName,
                columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
            )
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Original")])
        }

        let currentDb = try await self.client.withConnection { conn in
            let rows = try await conn.query("SELECT DB_NAME() AS db").get()
            return rows.first?.column("db")?.string ?? ""
        }
        let secondaryGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let secondaryConn: SQLServerConnection = try await {
            var connCfg = makeSQLServerConnectionConfiguration()
            connCfg.login.database = currentDb
            return try await SQLServerConnection.connect(configuration: connCfg, on: secondaryGroup.next()).get()
        }()

        let (lockReady, lockReadyCont) = AsyncStream.makeStream(of: Void.self)

        let holder = Task {
            try await self.client.withConnection { connection in
                try await connection.beginTransaction()
                try await connection.updateRows(in: tableName, set: ["value": .nString("Locked")], where: "id = 1")
                lockReadyCont.yield(())
                try await Task.sleep(nanoseconds: 800_000_000)
                try await connection.commit()
            }
        }

        var iter = lockReady.makeAsyncIterator()
        _ = await iter.next()
        lockReadyCont.finish()

        let elapsed = try await withTimeout(10) {
            try await {
                let script = """
                SET NOCOUNT ON;
                DECLARE @start DATETIME2(7) = SYSDATETIME();
                UPDATE [\(tableName)] SET value = N'Updated' WHERE id = 1;
                DECLARE @finish DATETIME2(7) = SYSDATETIME();
                SELECT DATEDIFF_BIG(millisecond, @start, @finish) AS elapsed_ms;
                """
                let result = try await secondaryConn.execute(script).get()
                var ms64: Int64 = 0
                for row in result.rows {
                    if let v = row.column("elapsed_ms")?.int64 { ms64 = v }
                    else if let vi = row.column("elapsed_ms")?.int { ms64 = Int64(vi) }
                }
                return UInt64(ms64 > 0 ? ms64 : 0) * 1_000_000
            }()
        }

        _ = try await withTimeout(10) { try await holder.value }
        XCTAssertGreaterThan(elapsed, 120_000_000 as UInt64, "Second update should wait for first transaction to commit")

        let final = try await self.client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        }
        XCTAssertEqual(final.first?.column("value")?.string, "Updated")

        _ = try? await secondaryConn.close().get()
        try? await secondaryGroup.shutdownGracefully()
    }
}
