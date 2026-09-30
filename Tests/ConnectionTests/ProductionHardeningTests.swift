import XCTest
import Foundation
import Logging
import NIOConcurrencyHelpers
import SQLServerKit
import SQLServerKitTesting

/// Live-server tests for the reliability contract Echo depends on:
/// cancellation, deadlines, streaming back-pressure, pooled session reset and
/// connection loss. Each test asserts that the connection is usable (or was
/// correctly discarded) afterwards, because a request pipeline that drifts
/// out of step silently returns one query's results to another.
final class ProductionHardeningTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private var watchdog: DispatchWorkItem?

    override func setUp() async throws {
        // A hang in these tests is itself a defect. Abort with the test's name
        // instead of letting the run stall until the CI job times out.
        let name = self.name
        let watchdog = DispatchWorkItem {
            fatalError("\(name) did not finish within 300 seconds")
        }
        self.watchdog = watchdog
        DispatchQueue.global().asyncAfter(deadline: .now() + 300, execute: watchdog)
        _ = isLoggingConfigured
        TestEnvironmentManager.loadEnvironmentVariables()
        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.maximumConcurrentConnections = 1
        config.poolConfiguration.connectionIdleTimeout = nil
        client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully()
        client = nil
        watchdog?.cancel()
    }

    private func dedicatedConnection() async throws -> SQLServerConnection {
        try await SQLServerConnection.connect(configuration: makeSQLServerConnectionConfiguration())
    }

    // MARK: - Cancellation

    func testTaskCancellationCancelsServerRequestAndConnectionStaysInStep() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }

        let started = ContinuousClock.now
        let task = Task {
            try await connection.execute("WAITFOR DELAY '00:00:30'; SELECT 1 AS never;")
        }
        try await Task.sleep(for: .milliseconds(300))
        task.cancel()
        do {
            _ = try await task.value
            XCTFail("Cancelled query should not succeed")
        } catch is CancellationError {
            // Expected.
        }
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(10), "Cancellation must not wait for the query")

        // The next request must receive its own result, not the tail of the
        // cancelled one.
        for value in 1...5 {
            let rows = try await connection.query("SELECT \(value) AS v;")
            XCTAssertEqual(rows.first?.column("v")?.int, value)
        }
    }

    func testCancellingAfterCompletionDoesNotAffectNextRequest() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }

        _ = try await connection.execute("SELECT 1;")
        // A stale cancellation must be a no-op, not an ATTENTION that the
        // next request absorbs.
        connection.cancelActiveRequest()
        let rows = try await connection.query("SELECT 42 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 42)
    }

    func testQueryTimeoutCancelsOnServerAndKeepsConnection() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }

        let started = ContinuousClock.now
        do {
            _ = try await connection.execute("WAITFOR DELAY '00:00:20';", timeout: 1)
            XCTFail("Query should time out")
        } catch let error as SQLServerError {
            guard case .timeout = error else { return XCTFail("Expected timeout, got \(error)") }
        }
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(8))

        let rows = try await connection.query("SELECT 7 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 7)
    }

    func testTimeoutInsideTransactionRollsBackWithXactAbort() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }

        _ = try await connection.execute("CREATE TABLE #t (id INT);")
        _ = try await connection.execute("BEGIN TRANSACTION; INSERT INTO #t VALUES (1);")
        do {
            _ = try await connection.execute("WAITFOR DELAY '00:00:20';", timeout: 1)
            XCTFail("Query should time out")
        } catch let error as SQLServerError {
            guard case .timeout = error else { return XCTFail("Expected timeout, got \(error)") }
        }
        // XACT_ABORT ON (the default session option) rolls back the
        // transaction when a request is cancelled.
        let rows = try await connection.query("SELECT @@TRANCOUNT AS tc, (SELECT COUNT(*) FROM #t) AS n;")
        XCTAssertEqual(rows.first?.column("tc")?.int, 0)
        XCTAssertEqual(rows.first?.column("n")?.int, 0)
    }

    // MARK: - Streaming

    func testAbandonedStreamCancelsQueryAndConnectionStaysInStep() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }

        var seen = 0
        for try await event in connection.streamQuery(Self.largeResultSQL(rows: 200_000)) {
            if case .row = event {
                seen += 1
                if seen == 10 { break }
            }
        }
        XCTAssertEqual(seen, 10)

        let rows = try await connection.query("SELECT 99 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 99)
    }

    func testSlowStreamConsumerAppliesBackPressureToServer() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }
        let spid = try await connection.queryScalar("SELECT @@SPID", as: Int.self)!

        var iterator = connection.streamQuery(Self.largeResultSQL(rows: 500_000)).makeAsyncIterator()
        var rows = 0
        while rows < 100, let event = try await iterator.next() {
            if case .row = event { rows += 1 }
        }
        // Stop consuming. With back-pressure the driver stops reading the
        // socket, TCP buffers fill, and SQL Server waits on the client.
        try await Task.sleep(for: .seconds(2))
        let wait = try await client.queryScalar(
            "SELECT wait_type FROM sys.dm_exec_requests WHERE session_id = \(spid)",
            as: String.self
        )
        XCTAssertEqual(wait, "ASYNC_NETWORK_IO", "Server should be blocked on the paused client")

        var total = rows
        while let event = try await iterator.next() {
            if case .row = event { total += 1 }
        }
        XCTAssertEqual(total, 500_000)
        let after = try await connection.query("SELECT 5 AS v;")
        XCTAssertEqual(after.first?.column("v")?.int, 5)
    }

    func testClientScopedStreamReturnsSessionToPool() async throws {
        let count = try await client.withStreamQuery(Self.largeResultSQL(rows: 1_000)) { stream in
            var count = 0
            for try await event in stream {
                if case .row = event { count += 1 }
                if count == 20 { break }
            }
            return count
        }
        XCTAssertEqual(count, 20)
        // With a single-session pool this only succeeds if the session was
        // returned and reset after the abandoned stream.
        let rows = try await client.query("SELECT 3 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 3)
    }

    func testStoppingBatchStreamNeverRunsLaterBatches() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }
        _ = try await connection.execute("CREATE TABLE #later (id INT);")

        for try await event in connection.streamBatches(["SELECT 1 AS first;", "INSERT INTO #later VALUES (1);"]) {
            if case .batchCompleted(let index) = event, index == 0 { break }
        }
        let rows = try await connection.query("SELECT COUNT(*) AS n FROM #later;")
        XCTAssertEqual(rows.first?.column("n")?.int, 0, "A batch after the consumer stopped must not run")

        var completed: [Int] = []
        for try await event in connection.streamBatches(["SELECT 1;", "SELECT 1/0;", "INSERT INTO #later VALUES (2);"]) {
            if case .batchCompleted(let index) = event { completed.append(index) }
            if case .batchFailed(let index, let error, _) = event {
                XCTAssertEqual(index, 1)
                XCTAssertEqual((error as? SQLServerError)?.serverErrorNumber, 8134)
            }
        }
        XCTAssertEqual(completed, [0, 2], "A server error in one batch does not stop later batches")
    }

    // MARK: - Pooled session reset

    func testPooledSessionStateDoesNotLeakBetweenLeases() async throws {
        let firstSpid = try await client.withConnection { connection -> Int in
            _ = try await connection.execute("""
                CREATE TABLE #leak (id INT);
                SET LOCK_TIMEOUT 1234;
                SET DATEFORMAT dmy;
                SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
                SET ANSI_NULLS OFF;
                EXEC sp_set_session_context @key = N'tenant', @value = N'acme';
                SET CONTEXT_INFO 0x01020304;
                USE tempdb;
                BEGIN TRANSACTION;
                """)
            return try await connection.queryScalar("SELECT @@SPID", as: Int.self)!
        }

        let rows = try await client.withConnection { connection in
            try await connection.query("""
                SELECT
                    @@SPID AS spid,
                    OBJECT_ID('tempdb..#leak') AS temp_table,
                    @@LOCK_TIMEOUT AS lock_timeout,
                    (SELECT date_format FROM sys.dm_exec_sessions WHERE session_id = @@SPID) AS date_format,
                    (SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID) AS isolation,
                    (SELECT ansi_nulls FROM sys.dm_exec_sessions WHERE session_id = @@SPID) AS ansi_nulls,
                    CAST(SESSION_CONTEXT(N'tenant') AS NVARCHAR(20)) AS tenant,
                    CONTEXT_INFO() AS context_info,
                    DB_NAME() AS db,
                    @@TRANCOUNT AS trancount;
                """)
        }
        let row = try XCTUnwrap(rows.first)
        XCTAssertEqual(row.column("spid")?.int, firstSpid, "The physical session should be reused")
        XCTAssertTrue(row.column("temp_table")?.isNull ?? false, "Temp table leaked")
        XCTAssertEqual(row.column("lock_timeout")?.int, -1, "SET LOCK_TIMEOUT leaked")
        XCTAssertEqual(row.column("date_format")?.string, "mdy", "SET DATEFORMAT leaked")
        XCTAssertEqual(row.column("isolation")?.int, 2, "Isolation level leaked")
        XCTAssertEqual(row.column("ansi_nulls")?.bool, true, "SET ANSI_NULLS leaked")
        XCTAssertTrue(row.column("tenant")?.isNull ?? false, "SESSION_CONTEXT leaked")
        XCTAssertTrue(row.column("context_info")?.isNull ?? false, "CONTEXT_INFO leaked")
        XCTAssertEqual(row.column("db")?.string?.lowercased(), makeSQLServerConnectionConfiguration().login.database.lowercased(), "Database context leaked")
        XCTAssertEqual(row.column("trancount")?.int, 0, "Open transaction leaked")
    }

    func testUnrevertedImpersonationIsNeverReused() async throws {
        let login = "nio_hardening_\(UInt32.random(in: 0...UInt32.max))"
        _ = try await client.execute("CREATE LOGIN [\(login)] WITH PASSWORD = N'Aa1!\(UUID().uuidString)', CHECK_POLICY = OFF;")
        do {
            _ = try await client.withConnection { connection in
                try await connection.execute("EXECUTE AS LOGIN = N'\(login)';")
            }
            // SQL Server refuses to reset an impersonated session (error
            // 18059); the pool must discard it and open a fresh session.
            let rows = try await client.query("SELECT SUSER_SNAME() AS who;")
            XCTAssertNotEqual(rows.first?.column("who")?.string, login)
        } catch {
            _ = try? await client.execute("DROP LOGIN [\(login)];")
            throw error
        }
        _ = try await client.execute("DROP LOGIN [\(login)];")
    }

    // MARK: - Connection loss and errors

    func testKilledSessionIsReportedAsLostAndPoolRecovers() async throws {
        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.maximumConcurrentConnections = 2
        let pooled = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        defer { Task { try? await pooled.shutdownGracefully() } }
        let observer = try await dedicatedConnection()
        defer { Task { try? await observer.close() } }

        let victimSpid = NIOLockedValueBox<Int?>(nil)
        let work = Task {
            try await pooled.withConnection { connection in
                victimSpid.withLockedValue { $0 = nil }
                let spid = try await connection.queryScalar("SELECT @@SPID", as: Int.self)
                victimSpid.withLockedValue { $0 = spid }
                return try await connection.execute("WAITFOR DELAY '00:00:30';")
            }
        }
        var spid: Int?
        for _ in 0..<50 where spid == nil {
            try await Task.sleep(for: .milliseconds(100))
            spid = victimSpid.withLockedValue { $0 }
        }
        let victim = try XCTUnwrap(spid)
        try await Task.sleep(for: .milliseconds(300))
        _ = try await observer.execute("KILL \(victim);")

        do {
            _ = try await work.value
            XCTFail("Killed session should fail")
        } catch let error as SQLServerError {
            XCTAssertTrue(error.isConnectionLost, "Expected a lost connection, got \(error)")
        }

        for value in 1...3 {
            let rows = try await pooled.query("SELECT \(value) AS v;")
            XCTAssertEqual(rows.first?.column("v")?.int, value)
        }
    }

    func testServerErrorCarriesStructuredDetails() async throws {
        do {
            _ = try await client.execute("PRINT 'before'; RAISERROR('custom failure', 16, 3);")
            XCTFail("Expected an error")
        } catch let error as SQLServerError {
            let details = try XCTUnwrap(error.serverDetails)
            XCTAssertEqual(details.number, 50000)
            XCTAssertEqual(details.severity, 16)
            XCTAssertEqual(details.state, 3)
            XCTAssertEqual(details.primary.message, "custom failure")
            XCTAssertTrue(details.messages.contains { $0.kind == .info && $0.message == "before" })
            XCTAssertFalse(error.isConnectionLost)
        }
        let rows = try await client.query("SELECT 1 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 1)
    }

    func testCurrentDatabaseFollowsUseInsideBatch() async throws {
        let connection = try await dedicatedConnection()
        defer { Task { try? await connection.close() } }
        _ = try await connection.execute("USE tempdb;")
        XCTAssertEqual(connection.currentDatabase.lowercased(), "tempdb")
        _ = try await connection.execute("USE master;")
        XCTAssertEqual(connection.currentDatabase.lowercased(), "master")
    }

    func testConcurrentOperationsOnSmallPoolReturnTheirOwnResults() async throws {
        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.maximumConcurrentConnections = 3
        let pooled = try await SQLServerClient.connect(configuration: config, numberOfThreads: 2)
        defer { Task { try? await pooled.shutdownGracefully() } }

        try await withThrowingTaskGroup(of: Void.self) { group in
            for value in 0..<60 {
                group.addTask {
                    let rows = try await pooled.query("SELECT \(value) AS v, REPLICATE('x', \(value * 50)) AS pad;")
                    XCTAssertEqual(rows.first?.column("v")?.int, value)
                }
            }
            try await group.waitForAll()
        }
    }

    func testUnreachableServerFailsWithinConnectTimeout() async throws {
        var config = makeSQLServerConnectionConfiguration()
        config.hostname = "10.255.255.1"
        config.connectTimeoutSeconds = 2
        config.retryConfiguration = .init(maximumAttempts: 1)
        let started = ContinuousClock.now
        do {
            _ = try await SQLServerConnection.connect(configuration: config)
            XCTFail("Connection to an unroutable address should fail")
        } catch {
            // Expected.
        }
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(6))
    }

    // MARK: - Helpers

    private static func largeResultSQL(rows: Int) -> String {
        """
        WITH n AS (
            SELECT TOP (\(rows)) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
            FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
        )
        SELECT i, REPLICATE(N'r', 100) AS payload FROM n;
        """
    }
}
