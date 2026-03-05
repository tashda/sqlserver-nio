import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerConstraintTests: XCTestCase {
    private var group: EventLoopGroup!
    private var baseClient: SQLServerClient!
    private var client: SQLServerClient!
    private var constraintClient: SQLServerConstraintClient!
    private var adminClient: SQLServerAdministrationClient!
    private var testDatabase: String!
    private var skipDueToEnv = false

    private var eventLoop: EventLoop { self.group.next() }

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do {
            _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1").get() }
        } catch {
            skipDueToEnv = true
            return
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "cst")
        self.client = try await makeClient(forDatabase: testDatabase, using: group)
        self.adminClient = SQLServerAdministrationClient(client: self.client)
        self.constraintClient = SQLServerConstraintClient(client: self.client)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully().get()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try await baseClient?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        testDatabase = nil; group = nil
    }

    // MARK: - Helper Methods

    private func createTestTable(name: String, withPrimaryKey: Bool = false) async throws {
        var columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: withPrimaryKey))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200))))),
            SQLServerColumnDefinition(name: "age", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "status", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
        ]

        if !withPrimaryKey {
            columns[0] = SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))
        }

        try await withTimeout(15) { try await self.adminClient.createTable(name: name, columns: columns) }
    }

    private func createReferenceTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "category_name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]

        try await withTimeout(15) { try await self.adminClient.createTable(name: name, columns: columns) }

        let insertSql = """
        INSERT INTO [\(name)] (id, category_name) VALUES
        (1, N'Category A'),
        (2, N'Category B'),
        (3, N'Category C')
        """
        _ = try await self.client.execute(insertSql)
    }

    // MARK: - Foreign Key Constraint Tests

    func testAddForeignKeyConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let parentTableName = "test_fk_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_fk_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(childTableName)_\(parentTableName)"

        try await self.createReferenceTable(name: parentTableName)

        let childColumns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        try await withTimeout(15) { try await self.adminClient.createTable(name: childTableName, columns: childColumns) }

        try await withTimeout(15) { try await self.constraintClient.addForeignKey(
            name: constraintName,
            table: childTableName,
            columns: ["parent_id"],
            referencedTable: parentTableName,
            referencedColumns: ["id"]
        ) }

        let exists = try await self.constraintClient.constraintExists(name: constraintName, table: childTableName)
        XCTAssertTrue(exists, "Foreign key constraint should exist after creation")

        let validInsertSql = "INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (1, 1, N'Valid reference')"
        _ = try await self.client.execute(validInsertSql)

        do {
            let invalidInsertSql = "INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (2, 999, N'Invalid reference')"
            _ = try await self.client.execute(invalidInsertSql)
            XCTFail("Insert with invalid foreign key should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddForeignKeyConstraintWithCascadeOptions() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let parentTableName = "test_fk_cascade_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_fk_cascade_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(childTableName)_cascade"

        try await self.createReferenceTable(name: parentTableName)

        let childColumns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        try await withTimeout(15) { try await self.adminClient.createTable(name: childTableName, columns: childColumns) }

        let options = ForeignKeyOptions(onDelete: .cascade, onUpdate: .cascade)
        try await withTimeout(15) { try await self.constraintClient.addForeignKey(
            name: constraintName,
            table: childTableName,
            columns: ["parent_id"],
            referencedTable: parentTableName,
            referencedColumns: ["id"],
            options: options
        ) }

        let insertChildSql = "INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (1, 1, N'Test record')"
        _ = try await self.client.execute(insertChildSql)

        var childCount = try await self.client.withConnection { conn in
            try await conn.queryScalar("SELECT COUNT(*) FROM [\(childTableName)] WHERE parent_id = 1", as: Int.self)
        }
        XCTAssertEqual(childCount, 1)

        let deleteParentSql = "DELETE FROM [\(parentTableName)] WHERE id = 1"
        _ = try await self.client.execute(deleteParentSql)

        childCount = try await self.client.withConnection { conn in
            try await conn.queryScalar("SELECT COUNT(*) FROM [\(childTableName)] WHERE parent_id = 1", as: Int.self)
        }
        XCTAssertEqual(childCount, 0)
    }

    func testDropForeignKeyConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let parentTableName = "test_drop_fk_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_drop_fk_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(childTableName)_drop"

        try await self.createReferenceTable(name: parentTableName)
        let childColumns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int)))
        ]
        try await withTimeout(15) { try await self.adminClient.createTable(name: childTableName, columns: childColumns) }

        try await withTimeout(15) { try await self.constraintClient.addForeignKey(
            name: constraintName,
            table: childTableName,
            columns: ["parent_id"],
            referencedTable: parentTableName,
            referencedColumns: ["id"]
        ) }

        var exists = try await self.constraintClient.constraintExists(name: constraintName, table: childTableName)
        XCTAssertTrue(exists, "Foreign key constraint should exist after creation")

        try await self.constraintClient.dropForeignKey(name: constraintName, table: childTableName)

        exists = try await self.constraintClient.constraintExists(name: constraintName, table: childTableName)
        XCTAssertFalse(exists, "Foreign key constraint should not exist after being dropped")
    }

    // MARK: - Check Constraint Tests

    func testAddCheckConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_check_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_age"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "age >= 0 AND age <= 150"
        ) }

        let exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Check constraint should exist after creation")

        let validInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await self.client.execute(validInsertSql)

        do {
            let invalidInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'Jane', N'jane@test.com', 200, N'active')"
            _ = try await self.client.execute(invalidInsertSql)
            XCTFail("Insert with invalid age should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropCheckConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_drop_check_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_status"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "status IN ('active', 'inactive', 'pending')"
        ) }

        var exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Check constraint should exist after creation")

        try await self.constraintClient.dropCheckConstraint(name: constraintName, table: tableName)

        exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(exists, "Check constraint should not exist after being dropped")
    }

    // MARK: - Unique Constraint Tests

    func testAddUniqueConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_unique_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_email"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.constraintClient.addUniqueConstraint(
            name: constraintName,
            table: tableName,
            columns: ["email"]
        ) }

        let exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Unique constraint should exist after creation")

        let firstInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await self.client.execute(firstInsertSql)

        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'Jane', N'john@test.com', 30, N'active')"
            _ = try await self.client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate email should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddMultiColumnUniqueConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_multi_unique_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_name_email"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.constraintClient.addUniqueConstraint(
            name: constraintName,
            table: tableName,
            columns: ["name", "email"]
        ) }

        let insert1Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john1@test.com', 25, N'active')"
        let insert2Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'John', N'john2@test.com', 30, N'active')"
        _ = try await self.client.execute(insert1Sql)
        _ = try await self.client.execute(insert2Sql)

        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (3, N'John', N'john1@test.com', 35, N'active')"
            _ = try await self.client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate name+email combination should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropUniqueConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_drop_unique_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_email"

        try await self.createTestTable(name: tableName)

        try await self.constraintClient.addUniqueConstraint(
            name: constraintName,
            table: tableName,
            columns: ["email"]
        )

        var exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Unique constraint should exist after creation")

        try await self.constraintClient.dropUniqueConstraint(name: constraintName, table: tableName)

        exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(exists, "Unique constraint should not exist after being dropped")
    }

    // MARK: - Primary Key Constraint Tests

    func testAddPrimaryKeyConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_pk_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "PK_\(tableName)"

        try await self.createTestTable(name: tableName, withPrimaryKey: false)

        try await withTimeout(15) { try await self.constraintClient.addPrimaryKey(
            name: constraintName,
            table: tableName,
            columns: ["id"]
        ) }

        let exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Primary key constraint should exist after creation")

        let firstInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await self.client.execute(firstInsertSql)

        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'Jane', N'jane@test.com', 30, N'active')"
            _ = try await self.client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate primary key should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddCompositePrimaryKeyConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_composite_pk_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "PK_\(tableName)_composite"

        try await self.createTestTable(name: tableName, withPrimaryKey: false)

        try await withTimeout(15) { try await self.constraintClient.addPrimaryKey(
            name: constraintName,
            table: tableName,
            columns: ["id", "name"]
        ) }

        let insert1Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        let insert2Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'Jane', N'jane@test.com', 30, N'active')"
        _ = try await self.client.execute(insert1Sql)
        _ = try await self.client.execute(insert2Sql)

        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john2@test.com', 35, N'active')"
            _ = try await self.client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate composite primary key should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    // MARK: - Default Constraint Tests

    func testAddDefaultConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_default_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "DF_\(tableName)_status"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.constraintClient.addDefaultConstraint(
            name: constraintName,
            table: tableName,
            column: "status",
            defaultValue: "'pending'"
        ) }

        let exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Default constraint should exist after creation")

        let insertSql = "INSERT INTO [\(tableName)] (id, name, email, age) VALUES (1, N'John', N'john@test.com', 25)"
        _ = try await self.client.execute(insertSql)

        let result = try await self.client.query("SELECT status FROM [\(tableName)] WHERE id = 1")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("status")?.string, "pending")
    }

    func testDropDefaultConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_drop_default_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "DF_\(tableName)_status"

        try await self.createTestTable(name: tableName)

        try await self.constraintClient.addDefaultConstraint(
            name: constraintName,
            table: tableName,
            column: "status",
            defaultValue: "'active'"
        )

        var exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Default constraint should exist after creation")

        try await self.constraintClient.dropDefaultConstraint(name: constraintName, table: tableName)

        exists = try await self.constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(exists, "Default constraint should not exist after being dropped")
    }

    // MARK: - Constraint Information Tests

    func testListTableConstraints() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_list_constraints_table_\(UUID().uuidString.prefix(8))"
        let checkConstraintName = "CK_\(tableName)_age"
        let uniqueConstraintName = "UQ_\(tableName)_email"

        try await self.createTestTable(name: tableName, withPrimaryKey: true)

        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: checkConstraintName,
            table: tableName,
            expression: "age >= 0"
        ) }

        try await withTimeout(15) { try await self.constraintClient.addUniqueConstraint(
            name: uniqueConstraintName,
            table: tableName,
            columns: ["email"]
        ) }

        let constraints = try await self.constraintClient.listTableConstraints(table: tableName)

        XCTAssertGreaterThanOrEqual(constraints.count, 2)

        let constraintNames = constraints.map { $0.name }
        XCTAssertTrue(constraintNames.contains(checkConstraintName))
        XCTAssertTrue(constraintNames.contains(uniqueConstraintName))

        let checkConstraint = constraints.first { $0.name == checkConstraintName }
        XCTAssertEqual(checkConstraint?.type, .check)

        let uniqueConstraint = constraints.first { $0.name == uniqueConstraintName }
        XCTAssertEqual(uniqueConstraint?.type, .unique)
    }

    // MARK: - Constraint Enable/Disable Tests

    func testEnableDisableConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_enable_disable_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_age"

        try await self.createTestTable(name: tableName)

        try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "age >= 0 AND age <= 150"
        )

        let validInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await self.client.execute(validInsertSql)

        try await self.constraintClient.disableConstraint(name: constraintName, table: tableName)

        let invalidInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'Jane', N'jane@test.com', 200, N'active')"
        _ = try await self.client.execute(invalidInsertSql)

        try await self.constraintClient.enableConstraint(name: constraintName, table: tableName)

        do {
            let anotherInvalidInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (3, N'Bob', N'bob@test.com', 300, N'active')"
            _ = try await self.client.execute(anotherInvalidInsertSql)
            XCTFail("Insert with invalid age should have failed after re-enabling constraint")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    // MARK: - Error Handling Tests

    func testAddDuplicateConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_duplicate_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_duplicate"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "age >= 0"
        ) }

        do {
            try await self.constraintClient.addCheckConstraint(
                name: constraintName,
                table: tableName,
                expression: "age <= 100"
            )
            XCTFail("Adding duplicate constraint should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentConstraint() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_nonexistent_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_nonexistent"

        try await self.createTestTable(name: tableName)

        do {
            try await self.constraintClient.dropCheckConstraint(name: constraintName, table: tableName)
            XCTFail("Dropping non-existent constraint should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddForeignKeyWithMismatchedColumns() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let parentTableName = "test_mismatch_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_mismatch_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_mismatch"

        try await self.createReferenceTable(name: parentTableName)
        try await self.createTestTable(name: childTableName)

        do {
            try await self.constraintClient.addForeignKey(
                name: constraintName,
                table: childTableName,
                columns: ["id", "name"], // 2 columns
                referencedTable: parentTableName,
                referencedColumns: ["id"] // 1 column
            )
            XCTFail("Adding foreign key with mismatched column counts should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
