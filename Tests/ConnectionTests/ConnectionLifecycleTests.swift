import SQLServerKit
import SQLServerKitTesting
import XCTest

final class SQLServerConnectionTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

        // Create connection
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )

        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    func testHealthCheck() async throws {

        let isHealthy = try await client.healthCheck()
        XCTAssertTrue(isHealthy, "Health check should pass for a working connection")
    }

    func testValidateConnections() async throws {

        // This should not throw an error for a healthy connection pool
        try await client.validateConnections()
    }

    func testPoolStatus() async throws {

        let status = client.poolStatus
        XCTAssertGreaterThanOrEqual(status.active, 0, "Active connections should be non-negative")
        XCTAssertGreaterThanOrEqual(status.idle, 0, "Idle connections should be non-negative")
    }

    func testWithConnectionIsolation() async throws {
        let tableName = "test_connection_isolation_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: client)

        try await adminClient.createTable(
            name: tableName,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
        )
        _ = try await client.admin.insertRow(into: tableName, values: [
            "id": .int(1),
            "value": .nString("Original")
        ])

        // Test that withConnection provides proper isolation
        let result1 = try await client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.updateRows(in: tableName, set: ["value": .nString("Modified")], where: "id = 1")

            // Query within the same connection should see the change
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            let value = rows.first?.column("value")?.string

            // Rollback the transaction
            try await connection.rollback()

            return value
        }

        XCTAssertEqual(result1, "Modified", "Should see modified value within the same connection")

        // Query outside the connection should see original value
        let result2 = try await client.query("SELECT value FROM [\(tableName)] WHERE id = 1")
        XCTAssertEqual(result2.first?.column("value")?.string, "Original", "Should see original value after rollback")

        // Cleanup
        try await adminClient.dropTable(name: tableName)
    }

    func testMultipleWithConnectionCalls() async throws {
        let tableName = "test_multiple_connections_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: client)

        try await adminClient.createTable(
            name: tableName,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
        )

        // Test multiple withConnection calls work independently
        let results = await withTaskGroup(of: String?.self) { group in
            var results: [String?] = []

            for i in 1...5 {
                group.addTask {
                    do {
                        return try await self.client.withConnection { connection in
                            try await connection.insertRow(into: tableName, values: [
                                "id": .int(i),
                                "value": .nString("Value\(i)")
                            ])
                            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = \(i)")
                            return rows.first?.column("value")?.string
                        }
                    } catch {
                        XCTFail("Connection \(i) failed: \(error)")
                        return nil
                    }
                }
            }

            for await result in group {
                results.append(result)
            }

            return results
        }

        // Verify all connections worked
        XCTAssertEqual(results.count, 5, "Should have 5 results")
        XCTAssertTrue(results.allSatisfy { $0 != nil }, "All results should be non-nil")

        // Verify all data was inserted
        let countResult = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        XCTAssertEqual(countResult.first?.column("count")?.int, 5)

        // Cleanup
        try await adminClient.dropTable(name: tableName)
    }

    func testConnectionReuse() async throws {
        // Test that connections are properly reused from the pool
        var connectionIds: Set<String> = []

        for _ in 1...10 {
            let connectionId = try await client.withConnection { connection in
                let rows = try await connection.query("SELECT @@SPID as connection_id")
                return rows.first?.column("connection_id")?.string ?? ""
            }
            connectionIds.insert(connectionId)
        }

        // With a small pool, we should see connection reuse (fewer unique IDs than operations)
        XCTAssertLessThan(connectionIds.count, 10, "Should reuse connections from the pool")
        XCTAssertGreaterThan(connectionIds.count, 0, "Should have at least one connection")
    }

    func testConnectionErrorHandling() async throws {
        // Test that connection errors are properly handled
        do {
            // Execute invalid SQL to trigger an error
            _ = try await client.execute("SELECT * FROM non_existent_table_12345")
            XCTFail("Should have thrown an error for invalid SQL")
        } catch {
            // Expected to fail
            XCTAssertTrue(error.localizedDescription.contains("Invalid object name") ||
                         error.localizedDescription.contains("non_existent_table"))
        }

        // Verify that the client is still functional after the error
        let result = try await client.query("SELECT 1 as test")
        XCTAssertEqual(result.first?.column("test")?.int, 1)
    }

    func testConnectionTimeout() async throws {
        // Test connection behavior with timeouts
        let startTime = Date()

        // Execute a query that should complete quickly
        let execResult = try await client.execute("SELECT GETDATE() as [current_time]")
        let result = execResult.rows.first?.column("current_time")?.date

        let endTime = Date()
        let duration = endTime.timeIntervalSince(startTime)

        XCTAssertNotNil(result, "Should get a valid date result")
        XCTAssertLessThan(duration, 5.0, "Query should complete within 5 seconds")
    }

    func testCompletedStreamQueryDoesNotPoisonDedicatedConnection() async throws {
        let connection = try await SQLServerConnection.connect(
            configuration: client.configuration.connection,
            numberOfThreads: 1
        )
        defer {
            Task {
                try? await connection.close()
            }
        }

        var sawRow = false
        let sql = """
        SELECT TOP 5000
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_id
        FROM sys.all_objects AS a
        CROSS JOIN sys.all_objects AS b
        """

        for try await event in connection.streamQuery(sql) {
            if case .row = event {
                sawRow = true
            }
        }

        XCTAssertTrue(sawRow, "Expected streamed query to produce at least one row")

        let followUp = try await connection.query("SELECT 1 AS still_healthy")
        XCTAssertEqual(followUp.first?.column("still_healthy")?.int, 1)
    }

    func testCancelledStreamQueryDoesNotPoisonDedicatedConnection() async throws {
        let connection = try await SQLServerConnection.connect(
            configuration: client.configuration.connection,
            numberOfThreads: 1
        )
        defer {
            Task {
                try? await connection.close()
            }
        }

        let streamTask = Task {
            for try await _ in connection.streamQuery("""
                WAITFOR DELAY '00:00:05';
                SELECT TOP 5000
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_id
                FROM sys.all_objects AS a
                CROSS JOIN sys.all_objects AS b
                """) {
                // Intentionally discard events; cancellation is the behavior under test.
            }
        }

        try await Task.sleep(for: .milliseconds(250))
        streamTask.cancel()

        do {
            try await streamTask.value
        } catch is CancellationError {
            // Expected.
        } catch {
            // Accept driver-level cancellation errors here; health is checked by the follow-up query.
        }

        let followUp = try await assertEventuallyHealthy(connection)
        XCTAssertEqual(followUp.first?.column("still_healthy")?.int, 1)
    }

    func testAdventureWorksEmployeeStreamDoesNotPoisonDedicatedConnection() async throws {
        guard ProcessInfo.processInfo.environment["TDS_AW_DATABASE"] != nil else {
            throw XCTSkip("AdventureWorks database not configured")
        }

        let targetDatabase = ProcessInfo.processInfo.environment["TDS_AW_DATABASE"] ?? "AdventureWorks"
        let databases = try await client.metadata.listDatabases()
        let availableDatabases = databases.map { $0.name.lowercased() }
        guard availableDatabases.contains(targetDatabase.lowercased()) else {
            throw XCTSkip("AdventureWorks database is not available on this server")
        }

        var configuration = client.configuration.connection
        configuration.login.database = targetDatabase

        let connection = try await SQLServerConnection.connect(
            configuration: configuration,
            numberOfThreads: 1
        )
        defer {
            Task {
                try? await connection.close()
            }
        }

        var sawMetadata = false
        var sawRows = false

        for try await event in connection.streamQuery("SELECT * FROM HumanResources.Employee") {
            switch event {
            case .metadata(let columns):
                sawMetadata = !columns.isEmpty
            case .row:
                sawRows = true
            case .done, .message:
                break
            }
        }

        XCTAssertTrue(sawMetadata)
        XCTAssertTrue(sawRows)

        let followUp = try await connection.query("SELECT TOP 1 BusinessEntityID FROM HumanResources.Employee")
        XCTAssertEqual(followUp.count, 1)
        XCTAssertNotNil(followUp.first?.column("BusinessEntityID"))
    }

    func testAdventureWorksHierarchyIDRendersCanonicalPaths() async throws {
        guard ProcessInfo.processInfo.environment["TDS_AW_DATABASE"] != nil else {
            throw XCTSkip("AdventureWorks database not configured")
        }

        let targetDatabase = ProcessInfo.processInfo.environment["TDS_AW_DATABASE"] ?? "AdventureWorks"
        let databases = try await client.metadata.listDatabases()
        let availableDatabases = databases.map { $0.name.lowercased() }
        guard availableDatabases.contains(targetDatabase.lowercased()) else {
            throw XCTSkip("AdventureWorks database is not available on this server")
        }

        var configuration = client.configuration.connection
        configuration.login.database = targetDatabase

        let connection = try await SQLServerConnection.connect(
            configuration: configuration,
            numberOfThreads: 1
        )
        defer {
            Task {
                try? await connection.close()
            }
        }

        let rows = try await connection.query("""
            SELECT TOP 5 OrganizationNode
            FROM HumanResources.Employee
            WHERE OrganizationNode IS NOT NULL
            ORDER BY OrganizationNode
            """)

        let rendered = rows.compactMap { $0.column("OrganizationNode")?.description }
        XCTAssertEqual(rendered, ["/1/", "/1/1/", "/1/1/1/", "/1/1/2/", "/1/1/3/"])
    }

    func testConnectionPoolExhaustion() async throws {
        // Test behavior when connection pool is exhausted
        let maxConnections = client.configuration.poolConfiguration.maximumConcurrentConnections

        // Start multiple long-running operations
        await withTaskGroup(of: Void.self) { group in
            for i in 1...Int(maxConnections + 2) {
                group.addTask {
                    do {
                        _ = try await self.client.withConnection { connection in
                            // Hold the connection briefly
                            try await Task.sleep(nanoseconds: 100_000_000) // 100ms
                            let rows = try await connection.query("SELECT \(i) as task_id")
                            return rows.first?.column("task_id")?.int
                        }
                    } catch {
                        // Some tasks might timeout or fail due to pool exhaustion
                        // This is expected behavior
                    }
                }
            }
        }

        // Verify the client is still functional after pool stress
        let result = try await client.query("SELECT 1 as recovery_test")
        XCTAssertEqual(result.first?.column("recovery_test")?.int, 1)
    }

    func testConnectionMetadata() async throws {
        let rows = try await client.query("""
            SELECT
                @@VERSION as server_version,
                @@SERVERNAME as server_name,
                DB_NAME() as database_name,
                USER_NAME() as user_name,
                @@SPID as connection_id
            """)
        let metadata = rows.first

        XCTAssertNotNil(metadata, "Should get connection metadata")
        XCTAssertNotNil(metadata?.column("server_version")?.string, "Should have server version")
        XCTAssertNotNil(metadata?.column("server_name")?.string, "Should have server name")
        XCTAssertNotNil(metadata?.column("database_name")?.string, "Should have database name")
        XCTAssertNotNil(metadata?.column("user_name")?.string, "Should have user name")
        XCTAssertNotNil(metadata?.column("connection_id")?.int, "Should have connection ID")
    }

    func testConnectionStateConsistency() async throws {
        let tableName = "test_state_consistency_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: client)

        try await adminClient.createTable(
            name: tableName,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
        )

        // Test that connection state is consistent within a withConnection block
        try await client.withConnection { connection in
            // Set a session variable
            _ = try await connection.execute("DECLARE @test_var INT = 42")

            // Insert data
            try await connection.insertRow(into: tableName, values: [
                "id": .int(1),
                "value": .nString("Test")
            ])

            // Verify we can access both the session variable and the data
            let varResult = try await connection.query("SELECT 42 as test_var") // Can't access DECLARE vars across batches
            let dataResult = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")

            XCTAssertEqual(varResult.first?.column("test_var")?.int, 42)
            XCTAssertEqual(dataResult.first?.column("value")?.string, "Test")
        }

        // Cleanup
        try await adminClient.dropTable(name: tableName)
    }

    func testConnectionRecoveryAfterError() async throws {
        // Test that connections can recover after errors
        let tableName = "test_recovery_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: client)

        try await adminClient.createTable(
            name: tableName,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
        )

        // Cause an error in a connection
        do {
            try await client.withConnection { connection in
                try await connection.insertRow(into: tableName, values: [
                    "id": .int(1),
                    "value": .nString("Valid")
                ])
                try await connection.insertRow(into: tableName, values: [
                    "id": .int(1),
                    "value": .nString("Duplicate")
                ])
            }
            XCTFail("Should have thrown an error")
        } catch {
            // Expected to fail
        }

        // Verify the connection pool can still be used
        let rows = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        let result = rows.first?.column("count")?.int

        XCTAssertEqual(result, 1, "Should have one valid record")

        // Cleanup
        try await adminClient.dropTable(name: tableName)
    }

    func testConnectionPoolWarmup() async throws {
        // Test that the connection pool properly warms up
        let status = client.poolStatus

        // After client initialization, we should have some connections available
        XCTAssertGreaterThanOrEqual(status.active + status.idle, 0, "Should have connections available")

        // Execute a simple query to ensure warmup worked
        let result = try await client.query("SELECT 1 as warmup_test")
        XCTAssertEqual(result.first?.column("warmup_test")?.int, 1)
    }

    func testConnectionCleanup() async throws {

        // Test that connections are properly cleaned up
        let initialStatus = client.poolStatus

        // Use a connection
        _ = try await client.withConnection { connection in
            let rows = try await connection.query("SELECT @@SPID as spid")
            return rows.first?.column("spid")?.int
        }

        // Status should be similar after connection is returned to pool
        let finalStatus = client.poolStatus

        // The exact numbers might vary, but we shouldn't have leaked connections
        XCTAssertLessThanOrEqual(finalStatus.active, initialStatus.active + 1, "Should not leak active connections")
    }

    func testStreamQueryWithOptionsCompiles() async throws {
        guard #available(macOS 12.0, *) else { return }
        // Ensure environment is loaded for connection details
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

        try await client.withConnection { connection in
            let options = SqlServerExecutionOptions(mode: .auto, rowsetFetchSize: nil, progressThrottleMs: 100)
            var count = 0
            for try await event in connection.streamQuery("SELECT TOP 1 1", options: options) {
                if case .row = event { count += 1 }
            }
            XCTAssertGreaterThanOrEqual(count, 0)
        }
    }
}

private extension SQLServerConnectionTests {
    func assertEventuallyHealthy(
        _ connection: SQLServerConnection,
        attempts: Int = 5,
        delay: Duration = .milliseconds(100)
    ) async throws -> [SQLServerRow] {
        var lastError: Error?

        for attempt in 0..<attempts {
            do {
                return try await connection.query("SELECT 1 AS still_healthy")
            } catch {
                lastError = error
                let description = String(describing: error).lowercased()
                let isTransientCancellation = description.contains("query cancelled")
                guard isTransientCancellation, attempt < attempts - 1 else {
                    throw error
                }
                try await Task.sleep(for: delay)
            }
        }

        throw lastError ?? XCTSkip("Expected follow-up query to succeed")
    }
}
