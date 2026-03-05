@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTransactionClientTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    var dbClient: SQLServerClient!
    private var testDatabase: String!
    private var skipDueToEnv = false

    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true; return }
        testDatabase = try await createTemporaryDatabase(client: client, prefix: "txc")
        dbClient = try await makeClient(forDatabase: testDatabase, using: group, maxConnections: 1)
    }

    override func tearDown() async throws {
        try? await dbClient?.shutdownGracefully().get()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: client, name: db) }
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        dbClient = nil; testDatabase = nil; group = nil
    }

    @available(macOS 12.0, *)
    func testBasicTransactionOperations() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test')").get()
        try await txClient.commitTransaction()

        let result = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 1)

        try await txClient.beginTransaction()
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Rollback')").get()
        try await txClient.rollbackTransaction()

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result2.first?.column("count")?.int, 1)
    }

    @available(macOS 12.0, *)
    func testSavepointOperations() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_sp_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Initial')").get()

        try await txClient.createSavepoint(name: "sp1")
        XCTAssertTrue(txClient.isSavepointActive(name: "sp1"))
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'After SP1')").get()

        let result1 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result1.first?.column("count")?.int, 2)

        try await txClient.rollbackToSavepoint(name: "sp1")
        XCTAssertFalse(txClient.isSavepointActive(name: "sp1"))

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result2.first?.column("count")?.int, 1)

        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (3, N'After Rollback')").get()
        try await txClient.commitTransaction()

        let result3 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result3.first?.column("count")?.int, 2)
    }

    @available(macOS 12.0, *)
    func testMultipleSavepoints() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_multi_sp_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "step", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, step) VALUES (1, N'Step 1')").get()

        try await txClient.createSavepoint(name: "sp1")
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, step) VALUES (2, N'Step 2')").get()

        try await txClient.createSavepoint(name: "sp2")
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, step) VALUES (3, N'Step 3')").get()

        let result1 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result1.first?.column("count")?.int, 3)

        try await txClient.rollbackToSavepoint(name: "sp1")

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result2.first?.column("count")?.int, 1)

        try await txClient.commitTransaction()
    }

    @available(macOS 12.0, *)
    func testExecuteInTransaction() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_exec_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        let result = try await txClient.executeInTransaction {
            _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Success')").get()
            _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Also Success')").get()
            return "Transaction completed successfully"
        }
        XCTAssertEqual(result, "Transaction completed successfully")

        let countResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(countResult.first?.column("count")?.int, 2)

        var errorThrown = false
        do {
            _ = try await txClient.executeInTransaction {
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (3, N'Before Error')").get()
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, value) VALUES (999, N'Should Not Exist')").get()
                _ = try await dbClient.execute("INVALID SQL STATEMENT").get()
                return "Should not reach here"
            }
        } catch {
            errorThrown = true
        }
        XCTAssertTrue(errorThrown)

        let finalCountResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(finalCountResult.first?.column("count")?.int, 2)
    }

    @available(macOS 12.0, *)
    func testExecuteInSavepoint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_sp_exec_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "operation", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, operation) VALUES (1, N'Initial')").get()

        let result1 = try await txClient.executeInSavepoint(named: "sp1") {
            _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, operation) VALUES (2, N'Savepoint 1')").get()
            return "Savepoint 1 completed"
        }
        XCTAssertEqual(result1, "Savepoint 1 completed")

        var errorThrown = false
        do {
            _ = try await txClient.executeInSavepoint(named: "sp2") {
                _ = try await dbClient.execute("INSERT INTO [\(tableName)] (id, operation) VALUES (3, N'Before Error')").get()
                _ = try await dbClient.execute("INVALID SQL").get()
                return "Should not reach here"
            }
        } catch {
            errorThrown = true
        }
        XCTAssertTrue(errorThrown)

        let result = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 2)

        try await txClient.commitTransaction()
    }

    @available(macOS 12.0, *)
    func testIsolationLevel() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)

        try await txClient.beginTransaction()
        try await txClient.setIsolationLevel(.readCommitted)

        let currentLevel = try await txClient.getCurrentIsolationLevel().get()
        let validLevels = ["READ COMMITTED", "REPEATABLE READ", "SNAPSHOT"]
        XCTAssertTrue(validLevels.contains(currentLevel?.uppercased() ?? ""),
                     "Expected one of \(validLevels), got: \(currentLevel ?? "nil")")

        try await txClient.setIsolationLevel(.serializable)
        let serializableLevel = try await txClient.getCurrentIsolationLevel().get()
        let serializableValidLevels = ["SERIALIZABLE", "SNAPSHOT"]
        XCTAssertTrue(serializableValidLevels.contains(serializableLevel?.uppercased() ?? ""),
                     "Expected SERIALIZABLE or SNAPSHOT, got: \(serializableLevel ?? "nil")")

        try await txClient.commitTransaction()
    }

    @available(macOS 12.0, *)
    func testGetTransactionInfo() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let txClient = SQLServerTransactionClient(client: dbClient)

        let infoBefore = try await txClient.getTransactionInfo().get()
        if let infoBefore = infoBefore {
            XCTAssertTrue(infoBefore.name == "SELECT" || infoBefore.name == nil,
                          "Unexpected transaction name: \(infoBefore.name ?? "nil")")
        }

        try await txClient.beginTransaction()

        let infoDuring = try await txClient.getTransactionInfo().get()
        XCTAssertNotNil(infoDuring)
        XCTAssertNotNil(infoDuring?.id)
        XCTAssertTrue(infoDuring?.type?.uppercased() == "READ" || infoDuring?.type?.uppercased() == "WRITE",
                      "Transaction should be READ or WRITE, got: \(infoDuring?.type ?? "nil")")

        try await txClient.commitTransaction()

        let infoAfter = try await txClient.getTransactionInfo().get()
        if let infoAfter = infoAfter {
            XCTAssertTrue(infoAfter.name == "SELECT" || infoAfter.name == nil,
                          "Unexpected transaction name after commit: \(infoAfter.name ?? "nil")")
        }
    }
}
