@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO

final class TransactionSavepointTests: TransactionTestBase, @unchecked Sendable {
    func testTransactionWithSavepoints() async throws {
        let tableName = "tx_sp_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("First")])
            try await connection.createSavepoint("SavePoint1")
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("Second")])
            try await connection.rollbackToSavepoint("SavePoint1")
            try await connection.insertRow(into: tableName, values: ["id": .int(3), "value": .nString("Third")])
            try await connection.commit()
        }

        let result = try await self.client.query("SELECT id, value FROM [\(tableName)] ORDER BY id").get()
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("id")?.int, 1)
        XCTAssertEqual(result[0].column("value")?.string, "First")
        XCTAssertEqual(result[1].column("id")?.int, 3)
        XCTAssertEqual(result[1].column("value")?.string, "Third")
    }

    func testSavepointRollbackWithinTransaction() async throws {
        let tableName = "tx_sp_rb_\(UUID().uuidString.prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        try await adminClient.createTable(name: tableName, columns: [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Committed")])
            try await connection.createSavepoint("before_temp")
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("ShouldRollback")])
            try await connection.rollbackToSavepoint("before_temp")
            try await connection.commit()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1, "Savepoint rollback should remove second row")
    }
}
