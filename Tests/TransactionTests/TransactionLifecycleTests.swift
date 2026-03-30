import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO

final class TransactionLifecycleTests: TransactionTestBase, @unchecked Sendable {
    func testBasicTransaction() async throws {
        let tableName = "tx_basic_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Test1")])
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("Test2")])
            try await connection.commit()
        }

        let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 2)
    }

    func testTransactionRollback() async throws {
        let tableName = "tx_rollback_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Initial")])
        }

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.insertRow(into: tableName, values: ["id": .int(2), "value": .nString("Rollback")])
            try await connection.rollback()
        }

        let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 1)

        let valueResult = try await self.client.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        XCTAssertEqual(valueResult.first?.column("value")?.string, "Initial")
    }

    func testTransactionWithExtendedProperties() async throws {
        let tableName = "tx_props_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.createTable(
                name: tableName,
                columns: [
                    .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    .init(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
            )
            try await connection.addTableComment(tableName: tableName, comment: "Test table created in transaction")
            try await connection.commit()
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

    func testTransactionWithError() async throws {
        let tableName = "tx_error_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        try await SQLServerAdministrationClient(client: self.client).createTable(
            name: tableName,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
        )

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            try await connection.insertRow(into: tableName, values: ["id": .int(1), "value": .nString("Valid")])
            try await connection.rollback()
        }

        let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 0)
    }

    func testTransactionWithBulkOperations() async throws {
        let tableName = "tx_bulk_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let adminClient = SQLServerAdministrationClient(client: self.client)

        try await adminClient.createTable(
            name: tableName,
            columns: [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
            ]
        )

        try await self.client.withConnection { connection in
            try await connection.beginTransaction()
            for i in 1...100 {
                try await connection.insertRow(into: tableName, values: ["id": .int(i), "value": .nString("Value\(i)")])
            }
            try await connection.commit()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 100)
    }
}
