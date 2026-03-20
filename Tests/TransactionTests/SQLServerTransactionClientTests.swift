@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerTransactionClientTests: XCTestCase, @unchecked Sendable {
    @MainActor
    private static var sharedClient: SQLServerClient?
    var client: SQLServerClient!
    var dbClient: SQLServerClient!
    private var testDatabase: String!
    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        if let sharedClient = await MainActor.run(body: { Self.sharedClient }) {
            self.client = sharedClient
        } else {
            let sharedClient = try await SQLServerClient.connect(
                configuration: makeSQLServerClientConfiguration(),
                numberOfThreads: 1
            )
            await MainActor.run {
                Self.sharedClient = sharedClient
            }
            self.client = sharedClient
        }
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
        testDatabase = try await createTemporaryDatabase(client: client, prefix: "txc")
        dbClient = try await makeClient(forDatabase: testDatabase, maxConnections: 1)
    }

    override func tearDown() async throws {
        try? await dbClient?.shutdownGracefully()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: client, name: db) }
        dbClient = nil; testDatabase = nil
    }

    @available(macOS 12.0, *)
    func testBasicTransactionOperations() async throws {
        let adminClient = SQLServerAdministrationClient(client: dbClient)

        let tableName = "tx_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        // Use withConnection to pin a single connection for the entire transaction
        try await dbClient.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                let txClient = SQLServerTransactionClient(client: self.dbClient)
                try await txClient.beginTransaction()
                try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Test")])
                try await txClient.commitTransaction()
            }
        }

        let result = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        XCTAssertEqual(result.first?.column("count")?.int, 1)

        // Test rollback with pinned connection
        try await dbClient.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                let txClient = SQLServerTransactionClient(client: self.dbClient)
                try await txClient.beginTransaction()
                try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("Rollback")])
                try await txClient.rollbackTransaction()
            }
        }

        let result2 = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        XCTAssertEqual(result2.first?.column("count")?.int, 1)
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

        let countResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        XCTAssertEqual(countResult.first?.column("count")?.int, 2)

        // Test rollback on error
        var errorThrown = false
        do {
            _ = try await txClient.executeInTransaction {
                try await self.dbClient.withConnection { connection in
                    try await connection.insertRow(into: tableName, values: ["id": .int(3), "value": .nString("Before Error")])
                }
                _ = try await self.dbClient.execute("INVALID SQL STATEMENT")
                return "Should not reach here"
            }
        } catch {
            errorThrown = true
        }
        XCTAssertTrue(errorThrown)

        let finalCountResult = try await dbClient.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        XCTAssertEqual(finalCountResult.first?.column("count")?.int, 2)
    }

    @available(macOS 12.0, *)
    func testExecuteInSavepoint() async throws {
        var configuration = makeSQLServerClientConfiguration()
        configuration.connection.sessionOptions.xactAbort = false
        let savepointClient = try await SQLServerClient.connect(
            configuration: configuration,
            numberOfThreads: 1
        )
        defer {
            Task {
                try? await savepointClient.shutdownGracefully()
            }
        }

        let txClient = SQLServerTransactionClient(client: savepointClient)
        let adminClient = SQLServerAdministrationClient(client: savepointClient)

        let tableName = "tx_sp_exec_test_\(UUID().uuidString.prefix(8))"
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "operation", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        // Use pinned connection for the outer transaction
        try await savepointClient.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                try await txClient.beginTransaction()
                try await connection.insertRow(into: tableName, values: ["id": .int(1), "operation": .nString("Initial")])

                let result1 = try await txClient.executeInSavepoint(named: "sp1") {
                    try await connection.insertRow(into: tableName, values: ["id": .int(2), "operation": .nString("Savepoint 1")])
                    return "Savepoint 1 completed"
                }
                XCTAssertEqual(result1, "Savepoint 1 completed")

                var errorThrown = false
                do {
                    _ = try await txClient.executeInSavepoint(named: "sp2") {
                        try await connection.insertRow(into: tableName, values: ["id": .int(3), "operation": .nString("Before Error")])
                        _ = try await savepointClient.execute("INVALID SQL")
                        return "Should not reach here"
                    }
                } catch {
                    errorThrown = true
                }
                XCTAssertTrue(errorThrown)

                let result = try await savepointClient.query("SELECT COUNT(*) as count FROM [\(tableName)]")
                XCTAssertEqual(result.first?.column("count")?.int, 2)

                try await txClient.commitTransaction()
            }
        }
    }

    @available(macOS 12.0, *)
    func testIsolationLevel() async throws {
        // Use pinned connection for the transaction
        try await dbClient.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                let txClient = SQLServerTransactionClient(client: self.dbClient)

                try await txClient.beginTransaction()
                try await txClient.setIsolationLevel(.readCommitted)

                let currentLevel = try await txClient.getCurrentIsolationLevel()
                let validLevels = ["READ COMMITTED", "REPEATABLE READ", "SNAPSHOT"]
                XCTAssertTrue(validLevels.contains(currentLevel?.uppercased() ?? ""),
                             "Expected one of \(validLevels), got: \(currentLevel ?? "nil")")

                try await txClient.setIsolationLevel(.serializable)
                let serializableLevel = try await txClient.getCurrentIsolationLevel()
                let serializableValidLevels = ["SERIALIZABLE", "SNAPSHOT"]
                XCTAssertTrue(serializableValidLevels.contains(serializableLevel?.uppercased() ?? ""),
                             "Expected SERIALIZABLE or SNAPSHOT, got: \(serializableLevel ?? "nil")")

                try await txClient.commitTransaction()
            }
        }
    }

    @available(macOS 12.0, *)
    func testGetTransactionInfo() async throws {
        try await dbClient.withConnection { connection in
            try await ClientScopedConnection.$current.withValue(connection) {
                let txClient = SQLServerTransactionClient(client: self.dbClient)

                let infoBefore = try await txClient.getTransactionInfo()
                if let infoBefore = infoBefore {
                    XCTAssertTrue(infoBefore.name == "SELECT" || infoBefore.name == nil,
                                  "Unexpected transaction name: \(infoBefore.name ?? "nil")")
                }

                try await txClient.beginTransaction()

                let infoDuring = try await txClient.getTransactionInfo()
                XCTAssertNotNil(infoDuring)
                XCTAssertNotNil(infoDuring?.id)
                XCTAssertTrue(infoDuring?.type?.uppercased() == "READ" || infoDuring?.type?.uppercased() == "WRITE",
                              "Transaction should be READ or WRITE, got: \(infoDuring?.type ?? "nil")")

                try await txClient.commitTransaction()

                let infoAfter = try await txClient.getTransactionInfo()
                if let infoAfter = infoAfter {
                    XCTAssertTrue(infoAfter.name == "SELECT" || infoAfter.name == nil,
                                  "Unexpected transaction name after commit: \(infoAfter.name ?? "nil")")
                }
            }
        }
    }
}
