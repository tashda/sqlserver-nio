import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class ConstraintLifecycleTests: ConstraintTestBase, @unchecked Sendable {
    // MARK: - Foreign Key Constraint Tests

    func testAddForeignKeyConstraint() async throws {
        let parent = "cl_fk_p_\(UUID().uuidString.prefix(8))"
        let child = "cl_fk_c_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(child)_\(parent)"

        try await self.createReferenceTable(name: parent)
        try await self.adminClient.createTable(name: child, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ])

        try await self.constraintClient.addForeignKey(name: constraintName, table: child, columns: ["parent_id"], referencedTable: parent, referencedColumns: ["id"])

        let foreignKeyExists = try await self.constraintClient.constraintExists(name: constraintName, table: child)
        XCTAssertTrue(foreignKeyExists)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: child, values: [
                "id": .int(1),
                "parent_id": .int(1),
                "description": .nString("Valid")
            ])
        }

        do {
            try await self.client.withConnection { connection in
                try await connection.insertRow(into: child, values: [
                    "id": .int(2),
                    "parent_id": .int(999),
                    "description": .nString("Invalid")
                ])
            }
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddForeignKeyConstraintWithCascadeOptions() async throws {
        let parent = "cl_fk_cp_\(UUID().uuidString.prefix(8))"
        let child = "cl_fk_cc_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(child)_cascade"

        try await self.createReferenceTable(name: parent)
        try await self.adminClient.createTable(name: child, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int)))
        ])

        try await self.constraintClient.addForeignKey(name: constraintName, table: child, columns: ["parent_id"], referencedTable: parent, referencedColumns: ["id"], options: ForeignKeyOptions(onDelete: .cascade, onUpdate: .cascade))
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: child, values: ["id": .int(1), "parent_id": .int(1)])
            try await connection.deleteRows(from: parent, where: "id = 1")
        }

        let count = try await self.client.queryScalar("SELECT COUNT(*) FROM [\(child)] WHERE parent_id = 1", as: Int.self)
        XCTAssertEqual(count, 0)
    }

    func testDropForeignKeyConstraint() async throws {
        let parent = "cl_fk_dp_\(UUID().uuidString.prefix(8))"
        let child = "cl_fk_dc_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(child)_drop"

        try await self.createReferenceTable(name: parent)
        try await self.adminClient.createTable(name: child, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int)))
        ])

        try await self.constraintClient.addForeignKey(name: constraintName, table: child, columns: ["parent_id"], referencedTable: parent, referencedColumns: ["id"])
        let cascadeForeignKeyExists = try await self.constraintClient.constraintExists(name: constraintName, table: child)
        XCTAssertTrue(cascadeForeignKeyExists)

        try await self.constraintClient.dropForeignKey(name: constraintName, table: child)
        let droppedForeignKeyExists = try await self.constraintClient.constraintExists(name: constraintName, table: child)
        XCTAssertFalse(droppedForeignKeyExists)
    }

    // MARK: - Check Constraint Tests

    func testAddCheckConstraint() async throws {
        let tableName = "cl_ck_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_age"

        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addCheckConstraint(name: constraintName, table: tableName, expression: "age >= 0 AND age <= 150")

        let checkConstraintExists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(checkConstraintExists)
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
                    "id": .int(2),
                    "name": .nString("J"),
                    "email": .nString("bad@example.com"),
                    "age": .int(200),
                    "status": .nString("active")
                ])
            }
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropCheckConstraint() async throws {
        let tableName = "cl_ck_dr_t_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_st"

        try await self.createTestTable(name: tableName)
        try await self.constraintClient.addCheckConstraint(name: constraintName, table: tableName, expression: "status IN ('A', 'I')")
        let dropCheckConstraintExists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(dropCheckConstraintExists)

        try await self.constraintClient.dropCheckConstraint(name: constraintName, table: tableName)
        let checkConstraintStillExists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(checkConstraintStillExists)
    }
}
