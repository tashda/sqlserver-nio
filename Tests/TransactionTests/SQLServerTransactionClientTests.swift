@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO
import Logging

final class SQLServerTransactionClientTests: XCTestCase, @unchecked Sendable {
    private static let sharedGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    @MainActor
    private static var sharedClient: SQLServerClient?
    var group: EventLoopGroup!
    var client: SQLServerClient!
    var dbClient: SQLServerClient!
    private var testDatabase: String!
    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        self.group = Self.sharedGroup
        if let sharedClient = await MainActor.run(body: { Self.sharedClient }) {
            self.client = sharedClient
        } else {
            let sharedClient = try await SQLServerClient.connect(
                configuration: makeSQLServerClientConfiguration(),
                eventLoopGroupProvider: .shared(group)
            ).get()
            await MainActor.run {
                Self.sharedClient = sharedClient
            }
            self.client = sharedClient
        }
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { throw error }
        testDatabase = try await createTemporaryDatabase(client: client, prefix: "txc")
        dbClient = try await makeClient(forDatabase: testDatabase, using: group, maxConnections: 1)
    }

    override func tearDown() async throws {
        try? await dbClient?.shutdownGracefully().get()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: client, name: db) }
        dbClient = nil; testDatabase = nil; group = nil
    }

    @available(macOS 12.0, *)
    func testBasicTransactionOperations() async throws {
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Test")])
        }
        try await txClient.commitTransaction()

        let result = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 1)

        try await txClient.beginTransaction()
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("Rollback")])
        }
        try await txClient.rollbackTransaction()

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result2.first?.column("count")?.int, 1)
    }

    @available(macOS 12.0, *)
    func testSavepointOperations() async throws {
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_sp_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Initial")])
        }

        try await txClient.createSavepoint(name: "sp1")
        XCTAssertTrue(txClient.isSavepointActive(name: "sp1"))
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("After SP1")])
        }

        let result1 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result1.first?.column("count")?.int, 2)

        try await txClient.rollbackToSavepoint(name: "sp1")
        XCTAssertFalse(txClient.isSavepointActive(name: "sp1"))

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result2.first?.column("count")?.int, 1)

        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(3), "value": .nString("After Rollback")])
        }
        try await txClient.commitTransaction()

        let result3 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result3.first?.column("count")?.int, 2)
    }

    @available(macOS 12.0, *)
    func testMultipleSavepoints() async throws {
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_multi_sp_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "step", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "step": .nString("Step 1")])
        }

        try await txClient.createSavepoint(name: "sp1")
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "step": .nString("Step 2")])
        }

        try await txClient.createSavepoint(name: "sp2")
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(3), "step": .nString("Step 3")])
        }

        let result1 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result1.first?.column("count")?.int, 3)

        try await txClient.rollbackToSavepoint(name: "sp1")

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result2.first?.column("count")?.int, 1)

        try await txClient.commitTransaction()
    }

    @available(macOS 12.0, *)
    func testExecuteInTransaction() async throws {
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_exec_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        let result = try await txClient.executeInTransaction {
            try await self.dbClient.withConnection { connection in
                try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Success")])
                try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("Also Success")])
            }
            return "Transaction completed successfully"
        }
        XCTAssertEqual(result, "Transaction completed successfully")

        let countResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(countResult.first?.column("count")?.int, 2)

        var errorThrown = false
        do {
            _ = try await txClient.executeInTransaction {
                try await self.dbClient.withConnection { connection in
                    try await connection.insertRow(into: tableName, values: ["id": .int(3), "value": .nString("Before Error")])
                    try await connection.insertRow(into: tableName, values: ["id": .int(999), "value": .nString("Should Not Exist")])
                }
                _ = try await self.dbClient.execute("INVALID SQL STATEMENT").get()
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
        let txClient = SQLServerTransactionClient(client: dbClient)
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_sp_exec_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "operation", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await txClient.beginTransaction()
        try await dbClient.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "operation": .nString("Initial")])
        }

        let result1 = try await txClient.executeInSavepoint(named: "sp1") {
            try await self.dbClient.withConnection { connection in
                try await connection.insertRow(into: tableName, values: ["id": .int(2), "operation": .nString("Savepoint 1")])
            }
            return "Savepoint 1 completed"
        }
        XCTAssertEqual(result1, "Savepoint 1 completed")

        var errorThrown = false
        do {
            _ = try await txClient.executeInSavepoint(named: "sp2") {
                try await self.dbClient.withConnection { connection in
                    try await connection.insertRow(into: tableName, values: ["id": .int(3), "operation": .nString("Before Error")])
                }
                _ = try await self.dbClient.execute("INVALID SQL").get()
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
