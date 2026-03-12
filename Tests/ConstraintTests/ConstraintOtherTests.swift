import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class ConstraintOtherTests: ConstraintTestBase, @unchecked Sendable {
    // MARK: - Unique Constraint Tests

    func testAddUniqueConstraint() async throws {
        let tableName = "co_uq_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_email"

        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addUniqueConstraint(name: constraintName, table: tableName, columns: ["email"])

        let uniqueConstraintExists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(uniqueConstraintExists)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: [
                "id": .int(1),
                "name": .nString("J"),
                "email": .nString("j@t.c"),
                "age": .int(25),
                "status": .nString("active")
            ])
        }

        do {
            try await self.client.withConnection { connection in
                try await connection.insertRow(into: tableName, values: [
                    "id": .int(2),
                    "name": .nString("K"),
                    "email": .nString("j@t.c"),
                    "age": .int(30),
                    "status": .nString("active")
                ])
            }
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropUniqueConstraint() async throws {
        let tableName = "co_uq_dr_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_email"

        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addUniqueConstraint(name: constraintName, table: tableName, columns: ["email"])
        let uniqueConstraintBeforeDrop = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(uniqueConstraintBeforeDrop)

        try await self.constraintClient.dropUniqueConstraint(name: constraintName, table: tableName)
        let uniqueConstraintAfterDrop = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(uniqueConstraintAfterDrop)
    }

    // MARK: - Primary Key Constraint Tests

    func testAddPrimaryKeyConstraint() async throws {
        let tableName = "co_pk_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "PK_\(tableName)"

        try await self.createTestTable(name: tableName, withPrimaryKey: false)
        try await self.constraintClient.addPrimaryKey(name: constraintName, table: tableName, columns: ["id"])

        let primaryKeyExists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(primaryKeyExists)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: [
                "id": .int(1),
                "name": .nString("J"),
                "email": .nString("j@example.com"),
                "age": .int(25),
                "status": .nString("active")
            ])
        }

        do {
            try await self.client.withConnection { connection in
                try await connection.insertRow(into: tableName, values: [
                    "id": .int(1),
                    "name": .nString("K"),
                    "email": .nString("k@example.com"),
                    "age": .int(26),
                    "status": .nString("active")
                ])
            }
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    // MARK: - Default Constraint Tests

    func testAddDefaultConstraint() async throws {
        let tableName = "co_df_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "DF_\(tableName)_status"

        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addDefaultConstraint(name: constraintName, table: tableName, column: "status", defaultValue: "'pending'")

        let defaultConstraintExists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(defaultConstraintExists)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: [
                "id": .int(1),
                "name": .nString("J"),
                "email": .nString("j@example.com"),
                "age": .int(25)
            ])
        }

        let result = try await self.client.query("SELECT status FROM [\(tableName)] WHERE id = 1")
        XCTAssertEqual(result.first?.column("status")?.string, "pending")
    }
}
