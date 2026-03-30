import XCTest
import SQLServerKit
import SQLServerKitTesting

final class SQLServerCRUDOperationsTests: XCTestCase, @unchecked Sendable {
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var testDatabase: String!

    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1") }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "crud")
        self.client = try await makeClient(forDatabase: testDatabase)
        self.adminClient = SQLServerAdministrationClient(client: self.client)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try? await baseClient?.shutdownGracefully()
        testDatabase = nil
        client = nil
        baseClient = nil
    }

    // MARK: - Insert Tests

    func testInsertRowReturnsAffectedCount() async throws {
        let tableName = "test_insert_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])

        let count = try await adminClient.insertRow(into: tableName, values: [
            "id": .int(1),
            "name": .nString("Alice")
        ])
        XCTAssertEqual(count, 1, "insertRow should return 1 affected row")

        try await adminClient.dropTable(name: tableName)
    }

    func testInsertRowWithAllLiteralTypes() async throws {
        let tableName = "test_insert_types_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "int_col", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "bigint_col", definition: .standard(.init(dataType: .bigint))),
            SQLServerColumnDefinition(name: "float_col", definition: .standard(.init(dataType: .float(mantissa: 53)))),
            SQLServerColumnDefinition(name: "bit_col", definition: .standard(.init(dataType: .bit))),
            SQLServerColumnDefinition(name: "nvarchar_col", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "varchar_col", definition: .standard(.init(dataType: .varchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "decimal_col", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2)))),
            SQLServerColumnDefinition(name: "guid_col", definition: .standard(.init(dataType: .uniqueidentifier, isNullable: true)))
        ])

        let testUUID = UUID()
        let count = try await adminClient.insertRow(into: tableName, values: [
            "int_col": .int(42),
            "bigint_col": .int64(9_000_000_000),
            "float_col": .double(3.14),
            "bit_col": .bool(true),
            "nvarchar_col": .nString("Hello World"),
            "varchar_col": .string("ASCII text"),
            "decimal_col": .decimal("123.45"),
            "guid_col": .uuid(testUUID)
        ])
        XCTAssertEqual(count, 1)

        let rows = try await client.query("SELECT * FROM [dbo].[\(tableName)]")
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows.first?.column("int_col")?.int, 42)
        XCTAssertEqual(rows.first?.column("bit_col")?.bool, true)

        try await adminClient.dropTable(name: tableName)
    }

    func testInsertRowWithNull() async throws {
        let tableName = "test_insert_null_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: true)))
        ])

        let count = try await adminClient.insertRow(into: tableName, values: [
            "id": .int(1),
            "name": .null
        ])
        XCTAssertEqual(count, 1)

        let rows = try await client.query("SELECT * FROM [dbo].[\(tableName)]")
        XCTAssertEqual(rows.count, 1)
        XCTAssertNil(rows.first?.column("name")?.string)

        try await adminClient.dropTable(name: tableName)
    }

    func testInsertRowWithSpecialCharacters() async throws {
        let tableName = "test_insert_special_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "text", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ])

        let count = try await adminClient.insertRow(into: tableName, values: [
            "id": .int(1),
            "text": .nString("O'Reilly's \"test\" with 'quotes'")
        ])
        XCTAssertEqual(count, 1)

        try await adminClient.dropTable(name: tableName)
    }

    // MARK: - Batch Insert Tests

    func testInsertRowsBatchReturnsAffectedCount() async throws {
        let tableName = "test_batch_insert_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])

        let count = try await adminClient.insertRows(
            into: tableName,
            columns: ["id", "name"],
            values: [
                [.int(1), .nString("Alice")],
                [.int(2), .nString("Bob")],
                [.int(3), .nString("Charlie")]
            ]
        )
        XCTAssertEqual(count, 3, "insertRows should return 3 affected rows")

        let rows = try await client.query("SELECT COUNT(*) AS cnt FROM [dbo].[\(tableName)]")
        XCTAssertEqual(rows.first?.column("cnt")?.int, 3)

        try await adminClient.dropTable(name: tableName)
    }

    func testInsertRowsWithMismatchedColumnsThrows() async throws {
        let tableName = "test_batch_mismatch_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])

        do {
            _ = try await adminClient.insertRows(
                into: tableName,
                columns: ["id", "name"],
                values: [
                    [.int(1)]  // Only 1 value for 2 columns
                ]
            )
            XCTFail("Should have thrown for mismatched column count")
        } catch {
            // Expected
        }

        try await adminClient.dropTable(name: tableName)
    }

    func testInsertRowsEmptyValuesThrows() async throws {
        do {
            _ = try await adminClient.insertRows(
                into: "any_table",
                columns: ["id"],
                values: []
            )
            XCTFail("Should have thrown for empty values")
        } catch {
            // Expected
        }
    }

    func testInsertRowsEmptyColumnsThrows() async throws {
        do {
            _ = try await adminClient.insertRows(
                into: "any_table",
                columns: [],
                values: [[.int(1)]]
            )
            XCTFail("Should have thrown for empty columns")
        } catch {
            // Expected
        }
    }

    // MARK: - Update Tests

    func testUpdateRowsReturnsAffectedCount() async throws {
        let tableName = "test_update_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .int)))
        ])

        try await adminClient.insertRows(into: tableName, columns: ["id", "name", "value"], values: [
            [.int(1), .nString("a"), .int(10)],
            [.int(2), .nString("b"), .int(20)],
            [.int(3), .nString("c"), .int(30)]
        ])

        let count = try await adminClient.updateRows(
            in: tableName,
            set: ["value": .int(99)],
            where: "[value] < 25"
        )
        XCTAssertEqual(count, 2, "updateRows should return 2 affected rows")

        try await adminClient.dropTable(name: tableName)
    }

    func testUpdateRowsWithMultipleAssignments() async throws {
        let tableName = "test_update_multi_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
            SQLServerColumnDefinition(name: "active", definition: .standard(.init(dataType: .bit)))
        ])

        try await adminClient.insertRow(into: tableName, values: [
            "id": .int(1),
            "name": .nString("original"),
            "active": .bool(true)
        ])

        let count = try await adminClient.updateRows(
            in: tableName,
            set: [
                "name": .nString("updated"),
                "active": .bool(false)
            ],
            where: "[id] = 1"
        )
        XCTAssertEqual(count, 1)

        let rows = try await client.query("SELECT * FROM [dbo].[\(tableName)] WHERE [id] = 1")
        XCTAssertEqual(rows.first?.column("name")?.string, "updated")
        XCTAssertEqual(rows.first?.column("active")?.bool, false)

        try await adminClient.dropTable(name: tableName)
    }

    func testUpdateRowsReturnsZeroWhenNoneMatch() async throws {
        let tableName = "test_update_zero_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .int)))
        ])

        try await adminClient.insertRow(into: tableName, values: ["id": .int(1), "value": .int(10)])

        let count = try await adminClient.updateRows(
            in: tableName,
            set: ["value": .int(99)],
            where: "[id] = 999"
        )
        XCTAssertEqual(count, 0, "No rows should be affected")

        try await adminClient.dropTable(name: tableName)
    }

    // MARK: - Delete Tests

    func testDeleteRowsReturnsAffectedCount() async throws {
        let tableName = "test_delete_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])

        try await adminClient.insertRows(into: tableName, columns: ["id", "name"], values: [
            [.int(1), .nString("Alice")],
            [.int(2), .nString("Bob")],
            [.int(3), .nString("Charlie")]
        ])

        let count = try await adminClient.deleteRows(from: tableName, where: "[id] <= 2")
        XCTAssertEqual(count, 2, "deleteRows should return 2 affected rows")

        let remaining = try await client.query("SELECT COUNT(*) AS cnt FROM [dbo].[\(tableName)]")
        XCTAssertEqual(remaining.first?.column("cnt")?.int, 1)

        try await adminClient.dropTable(name: tableName)
    }

    func testDeleteAllRows() async throws {
        let tableName = "test_delete_all_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))
        ])

        try await adminClient.insertRows(into: tableName, columns: ["id"], values: [
            [.int(1)], [.int(2)], [.int(3)]
        ])

        let count = try await adminClient.deleteRows(from: tableName)
        XCTAssertEqual(count, 3, "deleteRows without predicate should return all 3 rows")

        try await adminClient.dropTable(name: tableName)
    }

    func testDeleteReturnsZeroWhenNoneMatch() async throws {
        let tableName = "test_delete_zero_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))
        ])

        try await adminClient.insertRow(into: tableName, values: ["id": .int(1)])

        let count = try await adminClient.deleteRows(from: tableName, where: "[id] = 999")
        XCTAssertEqual(count, 0, "No rows should be affected")

        try await adminClient.dropTable(name: tableName)
    }

    // MARK: - Connection-Level CRUD Tests

    func testConnectionLevelInsertRow() async throws {
        let tableName = "test_conn_insert_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ])

        let count = try await client.admin.insertRow(into: tableName, values: [
            "id": .int(1),
            "name": .nString("Direct")
        ])
        XCTAssertEqual(count, 1)

        try await adminClient.dropTable(name: tableName)
    }

    func testConnectionLevelBatchInsert() async throws {
        let tableName = "test_conn_batch_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .int)))
        ])

        let count = try await client.admin.insertRows(into: tableName, columns: ["id", "value"], values: [
            [.int(1), .int(100)],
            [.int(2), .int(200)],
            [.int(3), .int(300)],
            [.int(4), .int(400)],
            [.int(5), .int(500)]
        ])
        XCTAssertEqual(count, 5)

        try await adminClient.dropTable(name: tableName)
    }

    func testConnectionLevelUpdateRows() async throws {
        let tableName = "test_conn_update_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "status", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
        ])

        try await adminClient.insertRows(into: tableName, columns: ["id", "status"], values: [
            [.int(1), .nString("active")],
            [.int(2), .nString("active")],
            [.int(3), .nString("inactive")]
        ])

        let count = try await client.admin.updateRows(
            in: tableName,
            set: ["status": .nString("archived")],
            where: "[status] = N'active'"
        )
        XCTAssertEqual(count, 2)

        try await adminClient.dropTable(name: tableName)
    }

    func testConnectionLevelDeleteRows() async throws {
        let tableName = "test_conn_delete_\(UUID().uuidString.prefix(8))"
        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))
        ])

        try await adminClient.insertRows(into: tableName, columns: ["id"], values: [
            [.int(1)], [.int(2)], [.int(3)]
        ])

        let count = try await client.admin.deleteRows(from: tableName, where: "[id] = 2")
        XCTAssertEqual(count, 1)

        try await adminClient.dropTable(name: tableName)
    }

    // MARK: - Generic Drop Constraint Tests

    func testDropConstraintGeneric() async throws {
        let tableName = "test_drop_constraint_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_val"

        try await adminClient.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "val", definition: .standard(.init(dataType: .int)))
        ])

        try await client.constraints.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "[val] > 0"
        )

        let existsBefore = try await client.constraints.constraintExists(
            name: constraintName,
            table: tableName
        )
        XCTAssertTrue(existsBefore, "Constraint should exist before drop")

        try await client.constraints.dropConstraint(
            name: constraintName,
            table: tableName
        )

        let existsAfter = try await client.constraints.constraintExists(
            name: constraintName,
            table: tableName
        )
        XCTAssertFalse(existsAfter, "Constraint should not exist after drop")

        try await adminClient.dropTable(name: tableName)
    }
}
