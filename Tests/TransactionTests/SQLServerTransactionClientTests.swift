@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTransactionClientTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

        // Create connection
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()

        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        group = nil
    }

    @available(macOS 12.0, *)
    func testBasicTransactionOperations() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_basic") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                let tableName = "tx_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                // Test basic transaction
                try await txClient.beginTransaction()
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test')").get()
                try await txClient.commitTransaction()

                // Verify data was committed
                let result = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result.first?.column("count")?.int, 1)

                // Test rollback
                try await txClient.beginTransaction()
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Rollback')").get()
                try await txClient.rollbackTransaction()

                // Verify data was not committed
                let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result2.first?.column("count")?.int, 1)
            }
        }
    }

    @available(macOS 12.0, *)
    func testSavepointOperations() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_sp") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                let tableName = "tx_sp_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                // Test savepoints
                try await txClient.beginTransaction()

                // Insert initial data
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Initial')").get()

                // Create savepoint
                try await txClient.createSavepoint(name: "sp1")
                XCTAssertTrue(txClient.isSavepointActive(name: "sp1"))

                // Insert more data
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'After SP1')").get()

                // Verify both records exist
                let result1 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result1.first?.column("count")?.int, 2)

                // Rollback to savepoint
                try await txClient.rollbackToSavepoint(name: "sp1")
                XCTAssertFalse(txClient.isSavepointActive(name: "sp1"))

                // Verify only first record exists
                let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result2.first?.column("count")?.int, 1)

                // Insert new data after rollback
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (3, N'After Rollback')").get()

                try await txClient.commitTransaction()

                // Verify final state
                let result3 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result3.first?.column("count")?.int, 2)
            }
        }
    }

    @available(macOS 12.0, *)
    func testMultipleSavepoints() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_multi_sp") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                let tableName = "tx_multi_sp_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "step", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                try await txClient.beginTransaction()

                // Initial data
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, step) VALUES (1, N'Step 1')").get()

                // First savepoint
                try await txClient.createSavepoint(name: "sp1")
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, step) VALUES (2, N'Step 2')").get()

                // Second savepoint
                try await txClient.createSavepoint(name: "sp2")
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, step) VALUES (3, N'Step 3')").get()

                // Verify all three records exist
                let result1 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result1.first?.column("count")?.int, 3)

                // Rollback to first savepoint (should remove sp2 and sp1, keeping data up to sp1)
                try await txClient.rollbackToSavepoint(name: "sp1")

                // Verify only first two records exist
                let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result2.first?.column("count")?.int, 2)

                try await txClient.commitTransaction()
            }
        }
    }

    @available(macOS 12.0, *)
    func testExecuteInTransaction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_exec") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                let tableName = "tx_exec_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                // Test successful operation in transaction
                let result = try await txClient.executeInTransaction {
                    _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Success')").get()
                    _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Also Success')").get()
                    return "Transaction completed successfully"
                }

                XCTAssertEqual(result, "Transaction completed successfully")

                // Verify data was committed
                let countResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(countResult.first?.column("count")?.int, 2)

                // Test failed operation in transaction
                var errorThrown = false
                do {
                    _ = try await txClient.executeInTransaction {
                        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (3, N'Before Error')").get()
                        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (999, N'Should Not Exist')").get()
                        _ = try await dbClient.execute("INVALID SQL STATEMENT").get() // This will fail
                        return "Should not reach here"
                    }
                } catch {
                    errorThrown = true
                }

                XCTAssertTrue(errorThrown)

                // Verify error case didn't commit
                let finalCountResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(finalCountResult.first?.column("count")?.int, 2)
            }
        }
    }

    @available(macOS 12.0, *)
    func testExecuteInSavepoint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_sp_exec") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)
                let adminClient = SQLServerAdministrationClient(client: dbClient)

                let tableName = "tx_sp_exec_test_\(UUID().uuidString.prefix(8))"
                let columns = [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "operation", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
                try await adminClient.createTable(name: tableName, columns: columns)

                try await txClient.beginTransaction()

                // Initial data
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, operation) VALUES (1, N'Initial')").get()

                // Execute in savepoint - success case
                let result1 = try await txClient.executeInSavepoint(named: "sp1") {
                    _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, operation) VALUES (2, N'Savepoint 1')").get()
                    return "Savepoint 1 completed"
                }

                XCTAssertEqual(result1, "Savepoint 1 completed")

                // Execute in savepoint - failure case
                var errorThrown = false
                do {
                    _ = try await txClient.executeInSavepoint(named: "sp2") {
                        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, operation) VALUES (3, N'Before Error')").get()
                        _ = try await dbClient.execute("INVALID SQL").get() // This will fail
                        return "Should not reach here"
                    }
                } catch {
                    errorThrown = true
                }

                XCTAssertTrue(errorThrown)

                // Verify savepoint 1 data remains but savepoint 2 data was rolled back
                let result = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
                XCTAssertEqual(result.first?.column("count")?.int, 2)

                try await txClient.commitTransaction()
            }
        }
    }

    @available(macOS 12.0, *)
    func testIsolationLevel() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_iso") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)

                // Begin a transaction to set isolation level in the context of the transaction
                try await txClient.beginTransaction()

                // Test setting isolation level
                try await txClient.setIsolationLevel(.readCommitted)

                // Verify the isolation level was set
                let currentLevel = try await txClient.getCurrentIsolationLevel().get()
                let validLevels = ["READ COMMITTED", "REPEATABLE READ", "SNAPSHOT"]
                XCTAssertTrue(validLevels.contains(currentLevel?.uppercased() ?? ""),
                             "Expected one of \(validLevels), got: \(currentLevel ?? "nil")")

                // Test other isolation levels
                try await txClient.setIsolationLevel(.serializable)
                let serializableLevel = try await txClient.getCurrentIsolationLevel().get()
                let serializableValidLevels = ["SERIALIZABLE", "SNAPSHOT"]
                XCTAssertTrue(serializableValidLevels.contains(serializableLevel?.uppercased() ?? ""),
                             "Expected SERIALIZABLE or SNAPSHOT, got: \(serializableLevel ?? "nil")")

                try await txClient.commitTransaction()
            }
        }
    }

    @available(macOS 12.0, *)
    func testGetTransactionInfo() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "tx_info") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let txClient = SQLServerTransactionClient(client: dbClient)

                // No transaction initially
                let infoBefore = try await txClient.getTransactionInfo().get()
                XCTAssertNil(infoBefore)

                // Start transaction
                try await txClient.beginTransaction()

                // Should have transaction info now
                let infoDuring = try await txClient.getTransactionInfo().get()
                XCTAssertNotNil(infoDuring)
                XCTAssertNotNil(infoDuring?.id)
                XCTAssertEqual(infoDuring?.type?.uppercased(), "WRITE")

                try await txClient.commitTransaction()

                // No transaction after commit
                let infoAfter = try await txClient.getTransactionInfo().get()
                XCTAssertNil(infoAfter)
            }
        }
    }
}