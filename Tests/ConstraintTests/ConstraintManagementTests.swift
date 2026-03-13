import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class ConstraintManagementTests: ConstraintTestBase, @unchecked Sendable {
    func testListTableConstraints() async throws {
        let tableName = "cm_ls_t_\(UUID().uuidString.prefix(8))"
        try await self.createTestTable(name: tableName, withPrimaryKey: true)
        try await self.constraintClient.addCheckConstraint(name: "CK_\(tableName)_age", table: tableName, expression: "age >= 0")

        let constraints = try await self.constraintClient.listTableConstraints(table: tableName)
        XCTAssertGreaterThanOrEqual(constraints.count, 2)
        XCTAssertTrue(constraints.contains { $0.type == .check })
    }

    func testEnableDisableConstraint() async throws {
        let tableName = "cm_ed_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_age"
        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addCheckConstraint(name: constraintName, table: tableName, expression: "age >= 0 AND age <= 150")

        try await self.constraintClient.disableConstraint(name: constraintName, table: tableName)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: tableName, values: [
                "id": .int(1),
                "name": .nString("J"),
                "email": .nString("j@example.com"),
                "age": .int(200),
                "status": .nString("active")
            ])
        }

        try await self.constraintClient.enableConstraint(name: constraintName, table: tableName)
        do {
            try await self.client.withConnection { connection in
                try await connection.insertRow(into: tableName, values: [
                    "id": .int(2),
                    "name": .nString("K"),
                    "email": .nString("k@example.com"),
                    "age": .int(300),
                    "status": .nString("active")
                ])
            }
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddDuplicateConstraint() async throws {
        let tableName = "cm_dup_t_\(UUID().uuidString.prefix(8))"
        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addCheckConstraint(name: "CK_dup", table: tableName, expression: "age >= 0")
        do {
            try await self.constraintClient.addCheckConstraint(name: "CK_dup", table: tableName, expression: "age <= 100")
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentConstraint() async throws {
        let tableName = "cm_no_t_\(UUID().uuidString.prefix(8))"
        try await self.createTestTable(name: tableName)
        do {
            try await self.constraintClient.dropCheckConstraint(name: "CK_no", table: tableName)
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
