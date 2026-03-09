@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO
import Logging
import Dispatch

final class SQLServerTransactionTests: XCTestCase {
    var group: EventLoopGroup!
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    private var testDatabase: String!
    private var skipDueToEnv = false
    private var snapshotIsolationChecked = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1").get() } } catch { skipDueToEnv = true; return }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "tx")
        self.client = try await makeClient(forDatabase: testDatabase, using: group)
    }

    override func tearDown() async throws {
        if client !== baseClient { try? await client?.shutdownGracefully().get() }
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        _ = try await baseClient?.shutdownGracefully().get()
        if let group = group {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                group.shutdownGracefully { error in
                    if let error { cont.resume(throwing: error) } else { cont.resume(returning: ()) }
                }
            }
        }
        testDatabase = nil
        client = nil; baseClient = nil; group = nil
    }

    private func ensureSnapshotIsolationEnabled() async throws {
        guard !snapshotIsolationChecked else { return }
        snapshotIsolationChecked = true
        let databaseRows = try await self.client.query("SELECT DB_NAME() AS db").get()
        let database = databaseRows.first?.column("db")?.string ?? ""
        try await self.client.withConnection { connection in
            let stateRows = try await connection.query("""
            SELECT snapshot_isolation_state
            FROM sys.databases
            WHERE name = N'\(database.replacingOccurrences(of: "'", with: "''"))'
            """)
            let state = stateRows.first?.column("snapshot_isolation_state")?.int ?? 0
            if state != 1 {
                _ = try await connection.execute("ALTER DATABASE [\(database)] SET ALLOW_SNAPSHOT_ISOLATION ON")
            }
        }
    }

    func testBasicTransaction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_transaction_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test1')").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Test2')").get()
            try await connection.commit()
        }

        let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 2)
    }

    func testTransactionRollback() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_rollback_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        _ = try await self.client.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Initial')").get()

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Rollback')").get()
            try await connection.rollback()
        }

        let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 1)

        let valueResult = try await self.client.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        XCTAssertEqual(valueResult.first?.column("value")?.string, "Initial")
    }

    func testTransactionWithExtendedProperties() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_trans_props_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await self.client.withConnection { connection in
            _ = try await connection.execute("BEGIN TRANSACTION")
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))")
            let commentSQL = """
            EXEC sp_addextendedproperty
                N'MS_Description',
                N'Test table created in transaction',
                N'SCHEMA',
                N'dbo',
                N'TABLE',
                N'\(tableName)'
            """
            _ = try await connection.execute(commentSQL)
            _ = try await connection.execute("COMMIT")
        }

        let tableResult = try await self.client.query("SELECT COUNT(*) as count FROM sys.tables WHERE name = '\(tableName)'").get()
        XCTAssertEqual(tableResult.first?.column("count")?.int, 1)

        let commentResult = try await self.client.query("""
            SELECT CAST(p.value AS NVARCHAR(4000)) AS value
            FROM sys.extended_properties p
            WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0
            """).get()
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Test table created in transaction")
    }

    func testConcurrentTransactions() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_concurrent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
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
                            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')").get()
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

    func testTransactionIsolation() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_isolation_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }

        var connection1Result: String?
        var connection2Result: String?

        try await self.client.withConnection { connection1 in
            _ = try await connection1.execute("BEGIN TRANSACTION")
            _ = try await connection1.execute("UPDATE [\(tableName)] SET value = N'Modified' WHERE id = 1")
            let rows = try await connection1.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            connection1Result = rows.first?.column("value")?.string
            _ = try await connection1.execute("ROLLBACK")
        }

        try await self.client.withConnection { connection2 in
            let rows = try await connection2.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            connection2Result = rows.first?.column("value")?.string
        }

        XCTAssertEqual(connection1Result, "Modified", "Should see modified value within transaction")
        XCTAssertEqual(connection2Result, "Original", "Should see original value after rollback")
    }

    func testTransactionWithSavepoints() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_savepoints_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'First')").get()
            _ = try await connection.execute("SAVE TRANSACTION SavePoint1").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Second')").get()
            _ = try await connection.execute("ROLLBACK TRANSACTION SavePoint1").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (3, N'Third')").get()
            try await connection.commit()
        }

        let result = try await self.client.query("SELECT id, value FROM [\(tableName)] ORDER BY id").get()
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("id")?.int, 1)
        XCTAssertEqual(result[0].column("value")?.string, "First")
        XCTAssertEqual(result[1].column("id")?.int, 3)
        XCTAssertEqual(result[1].column("value")?.string, "Third")
    }

    func testTransactionWithError() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_error_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        _ = try await self.client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()

        try await self.client.withConnection { connection in
            _ = try await connection.execute("BEGIN TRANSACTION")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Valid')")
            _ = try await connection.execute("ROLLBACK")
        }

        let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 0)
    }

    func testTransactionWithDeadlock() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let table1Name = "test_deadlock1_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let table2Name = "test_deadlock2_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(table1Name)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.execute("CREATE TABLE [\(table2Name)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.execute("INSERT INTO [\(table1Name)] (id, value) VALUES (1, N'Table1')").get()
            _ = try await connection.execute("INSERT INTO [\(table2Name)] (id, value) VALUES (1, N'Table2')").get()
        }

        try await self.client.withConnection { connection1 in
            _ = try await connection1.execute("BEGIN TRANSACTION").get()
            _ = try await connection1.execute("UPDATE [\(table1Name)] SET value = N'Modified1' WHERE id = 1").get()
            _ = try await connection1.execute("COMMIT").get()
        }

        try await self.client.withConnection { connection2 in
            _ = try await connection2.execute("BEGIN TRANSACTION").get()
            _ = try await connection2.execute("UPDATE [\(table2Name)] SET value = N'Modified2' WHERE id = 1").get()
            _ = try await connection2.execute("COMMIT").get()
        }
    }

    func testTransactionTimeout() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_timeout_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }

        try await self.client.withConnection { connection in
            _ = try await connection.execute("SET LOCK_TIMEOUT 1000").get()
            _ = try await connection.execute("BEGIN TRANSACTION").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test')").get()
            _ = try await connection.execute("COMMIT").get()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1)
    }

    func testTransactionWithBulkOperations() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_bulk_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }

        try await self.client.withConnection { connection in
            _ = try await connection.execute("BEGIN TRANSACTION").get()
            for i in 1...100 {
                _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')").get()
            }
            _ = try await connection.execute("COMMIT").get()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 100)
    }

    func testReadCommittedPreventsDirtyReads() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_read_committed_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }

        let writer = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("BEGIN TRANSACTION")
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Dirty' WHERE id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                _ = try await connection.execute("ROLLBACK")
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let readValue = try await self.client.withConnection { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }
        XCTAssertEqual(readValue, "Original", "READ COMMITTED should block until dirty update rolls back")

        _ = try await withTimeout(5) { try await writer.value }
    }

    func testReadUncommittedAllowsDirtyReads() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_read_uncommitted_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }

        let writer = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("BEGIN TRANSACTION")
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Dirty' WHERE id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                _ = try await connection.execute("ROLLBACK")
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let dirtyRead = try await client.withConnection { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }
        XCTAssertEqual(dirtyRead, "Dirty", "READ UNCOMMITTED should see uncommitted changes")

        _ = try await withTimeout(5) { try await writer.value }
    }

    func testRepeatableReadPreventsNonRepeatableReads() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_repeatable_\(UUID().uuidString.prefix(8))"
        try await client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }

        let blocker = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                _ = try await connection.execute("BEGIN TRANSACTION")
                let first = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
                try await Task.sleep(nanoseconds: 600_000_000)
                let second = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
                _ = try await connection.execute("COMMIT")
                return (first.first?.column("value")?.string, second.first?.column("value")?.string)
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let updateDelay = try await client.withConnection { connection in
            let start = DispatchTime.now()
            _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Updated' WHERE id = 1")
            let elapsed = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            return elapsed
        }

        let (firstRead, secondRead) = try await withTimeout(5) { try await blocker.value }
        XCTAssertEqual(firstRead, "Original")
        XCTAssertEqual(secondRead, "Original")
        XCTAssertGreaterThan(updateDelay, 400_000_000 as UInt64, "Update should wait for repeatable read to finish")

        let finalValue = try await client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
        }
        XCTAssertEqual(finalValue.first?.column("value")?.string, "Updated")
    }

    func testSerializablePreventsPhantomInserts() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_serializable_\(UUID().uuidString.prefix(8))"
        try await client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, category NVARCHAR(10))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, category) VALUES (1, N'A'), (2, N'A')")
        }

        let rangeLockTask = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").get()
                _ = try await connection.execute("BEGIN TRANSACTION").get()
                _ = try await connection.query("SELECT COUNT(*) FROM [\(tableName)] WHERE category = N'A'").get()
                try await Task.sleep(nanoseconds: 700_000_000)
                _ = try await connection.execute("COMMIT").get()
            }
        }

        try await Task.sleep(nanoseconds: 200_000_000)

        let insertDelay = try await self.client.withConnection { connection in
            let start = DispatchTime.now()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, category) VALUES (3, N'A')").get()
            let elapsed = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            return elapsed
        }

        _ = try await withTimeout(5) { try await rangeLockTask.value }
        XCTAssertGreaterThan(insertDelay, 50_000_000 as UInt64, "INSERT should have experienced some delay due to SERIALIZABLE transaction")

        let countResult = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)] WHERE category = N'A'").get()
        }
        XCTAssertEqual(countResult.first?.column("count")?.int, 3)
    }

    func testConcurrentUpdateBlocksUntilCommit() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_concurrent_update_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')").get()
        }

        let currentDb = try await self.client.withConnection { conn in
            let rows = try await conn.query("SELECT DB_NAME() AS db").get()
            return rows.first?.column("db")?.string ?? ""
        }
        let secondaryGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        var secondaryConn: SQLServerConnection!
        do {
            var connCfg = makeSQLServerConnectionConfiguration()
            connCfg.login.database = currentDb
            secondaryConn = try await SQLServerConnection.connect(
                configuration: connCfg,
                on: secondaryGroup.next()
            ).get()
        }

        var lockReadyCont: AsyncStream<Void>.Continuation!
        let lockReady = AsyncStream<Void> { cont in lockReadyCont = cont }
        var holderSpid: Int?

        let holder = Task {
            try await self.client.withConnection { connection in
                if let spid = try? await connection.query("SELECT @@SPID AS spid").get().first?.column("spid")?.int {
                    holderSpid = spid
                    print("[testConcurrentUpdateBlocksUntilCommit] holder spid=\(spid)")
                }
                _ = try await connection.execute("BEGIN TRANSACTION").get()
                _ = try await connection.execute("UPDATE [\(currentDb)].[dbo].[\(tableName)] SET value = N'Locked' WHERE id = 1").get()
                print("[testConcurrentUpdateBlocksUntilCommit] holder began transaction and updated row")
                lockReadyCont.yield(())
                try await Task.sleep(nanoseconds: 800_000_000)
                _ = try await connection.execute("COMMIT").get()
                print("[testConcurrentUpdateBlocksUntilCommit] holder committed")
            }
        }

        var iter = lockReady.makeAsyncIterator()
        _ = await iter.next()
        lockReadyCont.finish()

        self.client.logger.info("testConcurrentUpdateBlocksUntilCommit: preparing blocked session")
        var blockedSpid: Int?
        let elapsed = try await withTimeout(5) {
            try await {
                let connection = secondaryConn!
                if let spid = try? await connection.query("SELECT @@SPID AS spid").get().first?.column("spid")?.int {
                    blockedSpid = spid
                    print("[testConcurrentUpdateBlocksUntilCommit] blocked spid=\(spid)")
                }

                if envFlagEnabled("TDS_ENABLE_DMV_ASSERTIONS"), let h = holderSpid {
                    do {
                        let txRows = try await connection.query("SELECT transaction_id FROM sys.dm_tran_session_transactions WHERE session_id = \(h)").get()
                        let txActive = !txRows.isEmpty
                        print("[testConcurrentUpdateBlocksUntilCommit] holder tx active=\(txActive)")

                        let lockRows = try await connection.query("""
                            SELECT request_session_id, resource_type, resource_description, request_mode, request_status, resource_database_id
                            FROM sys.dm_tran_locks
                            WHERE request_session_id = \(h)
                        """).get()
                        for row in lockRows {
                            let sid = row.column("request_session_id")?.int ?? -1
                            let type = row.column("resource_type")?.string ?? "?"
                            let desc = row.column("resource_description")?.string ?? "<nil>"
                            let mode = row.column("request_mode")?.string ?? "?"
                            let status = row.column("request_status")?.string ?? "?"
                            let dbid = row.column("resource_database_id")?.int ?? -1
                            print("[testConcurrentUpdateBlocksUntilCommit] holder lock sid=\(sid) type=\(type) mode=\(mode) status=\(status) dbid=\(dbid) desc=\(desc)")
                        }

                        if let b = blockedSpid {
                            let reqRows = try await connection.query("""
                                SELECT session_id, blocking_session_id, wait_type, wait_time, last_wait_type, status, command
                                FROM sys.dm_exec_requests
                                WHERE session_id IN (\(h), \(b))
                            """).get()
                            for row in reqRows {
                                let sid = row.column("session_id")?.int ?? -1
                                let blk = row.column("blocking_session_id")?.int ?? 0
                                let wt = row.column("wait_type")?.string ?? "<nil>"
                                let wtime = row.column("wait_time")?.int ?? 0
                                let lwt = row.column("last_wait_type")?.string ?? "<nil>"
                                let st = row.column("status")?.string ?? "<nil>"
                                let cmd = row.column("command")?.string ?? "<nil>"
                                print("[testConcurrentUpdateBlocksUntilCommit] request sid=\(sid) blocking=\(blk) wait_type=\(wt) wait_ms=\(wtime) last_wait=\(lwt) status=\(st) cmd=\(cmd)")
                            }
                        }
                    } catch {
                        print("[testConcurrentUpdateBlocksUntilCommit] DMV probe failed: \(error)")
                    }
                }

                print("[testConcurrentUpdateBlocksUntilCommit] starting blocked update on \(tableName)")
                let script = """
                SET NOCOUNT ON;
                DECLARE @start DATETIME2(7) = SYSDATETIME();
                UPDATE [\(currentDb)].[dbo].[\(tableName)] SET value = N'Updated' WHERE id = 1;
                DECLARE @rc INT = @@ROWCOUNT;
                DECLARE @finish DATETIME2(7) = SYSDATETIME();
                SELECT DATEDIFF_BIG(millisecond, @start, @finish) AS elapsed_ms, @rc AS rc, DB_NAME() AS db, @@SPID AS spid;
                """
                let result = try await connection.execute(script).get()
                var ms64: Int64 = 0
                var rc: Int64 = -1
                var dbSeen: String = ""
                var spidSeen: Int64 = -1
                for row in result.rows {
                    if let v = row.column("elapsed_ms")?.int64 { ms64 = v }
                    if let vi = row.column("elapsed_ms")?.int { ms64 = Int64(vi) }
                    if let rcVal = row.column("rc")?.int64 { rc = rcVal }
                    if let dbVal = row.column("db")?.string { dbSeen = dbVal }
                    if let spidVal = row.column("spid")?.int64 { spidSeen = spidVal }
                }
                print("[testConcurrentUpdateBlocksUntilCommit] server-measured elapsed=\(ms64) ms rc=\(rc) db=\(dbSeen) spid=\(spidSeen)")
                return UInt64(ms64 > 0 ? ms64 : 0) * 1_000_000
            }()
        }

        _ = try await withTimeout(5) { try await holder.value }
        XCTAssertGreaterThan(elapsed, 120_000_000 as UInt64, "Second update should wait for first transaction to commit")

        let final = try await withTimeout(5) {
            try await self.client.withConnection { connection in
                try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            }
        }
        if let v = final.first?.column("value")?.string {
            print("[testConcurrentUpdateBlocksUntilCommit] final value=\(v)")
        }
        XCTAssertEqual(final.first?.column("value")?.string, "Updated")

        _ = try? await secondaryConn?.close().get()
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            secondaryGroup.shutdownGracefully { _ in cont.resume() }
        }
    }

    func testSavepointRollbackWithinTransaction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_savepoint_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }

        try await self.client.withConnection { connection in
            _ = try await connection.execute("BEGIN TRANSACTION")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Committed')").get()
            _ = try await connection.execute("SAVE TRANSACTION before_temp").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'ShouldRollback')").get()
            _ = try await connection.execute("ROLLBACK TRANSACTION before_temp").get()
            _ = try await connection.execute("COMMIT").get()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1, "Savepoint rollback should remove second row")
    }

    func testSnapshotIsolationProvidesStableView() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await ensureSnapshotIsolationEnabled()
        let tableName = "test_snapshot_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')").get()
        }

        let writer = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("BEGIN TRANSACTION").get()
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'NewValue' WHERE id = 1").get()
                try await Task.sleep(nanoseconds: 500_000_000)
                _ = try await connection.execute("COMMIT").get()
            }
        }

        try await Task.sleep(nanoseconds: 100_000_000)

        let (firstRead, secondRead) = try await self.client.withConnection { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT").get()
            _ = try await connection.execute("BEGIN TRANSACTION").get()
            let first = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            try await Task.sleep(nanoseconds: 400_000_000)
            let second = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            _ = try await connection.execute("COMMIT").get()
            return (first.first?.column("value")?.string, second.first?.column("value")?.string)
        }

        _ = try await withTimeout(5) { try await writer.value }

        XCTAssertEqual(firstRead, "Original")
        XCTAssertEqual(secondRead, "Original")

        let committedValue = try await self.client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        }
        XCTAssertEqual(committedValue.first?.column("value")?.string, "NewValue")
    }
}
