import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerConstraintTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var constraintClient: SQLServerConstraintClient!
    private var adminClient: SQLServerAdministrationClient!
    private var testDatabase: String!

    private var eventLoop: EventLoop { self.group.next() }

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let base = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
        self.testDatabase = "cst_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(10))"
        try await DDLGuard.shared.withLock {
            _ = try await withTimeout(15) { try await base.execute("CREATE DATABASE [\(self.testDatabase!)]").get() }
        }
        self.client = try await makeClient(forDatabase: self.testDatabase, using: group)
        _ = try? await base.shutdownGracefully().get()
        self.constraintClient = SQLServerConstraintClient(client: client)
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDown() async throws {
        let master = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
        try await DDLGuard.shared.withLock {
            _ = try? await withTimeout(15) {
                try await master.execute("ALTER DATABASE [\(self.testDatabase!)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [\(self.testDatabase!)]").get()
            }
        }
        _ = try? await master.shutdownGracefully().get()
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        group = nil
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
        
        // Insert some reference data
        let insertSql = """
        INSERT INTO [\(name)] (id, category_name) VALUES
        (1, N'Category A'),
        (2, N'Category B'),
        (3, N'Category C')
        """
        _ = try await client.execute(insertSql)
    }

    // MARK: - Foreign Key Constraint Tests

    func testAddForeignKeyConstraint() async throws {
        let parentTableName = "test_fk_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_fk_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(childTableName)_\(parentTableName)"

        // Create parent table
        try await createReferenceTable(name: parentTableName)

        // Create child table
        let childColumns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        try await withTimeout(15) { try await self.adminClient.createTable(name: childTableName, columns: childColumns) }

        // Add foreign key constraint
        try await withTimeout(15) { try await self.constraintClient.addForeignKey(
            name: constraintName,
            table: childTableName,
            columns: ["parent_id"],
            referencedTable: parentTableName,
            referencedColumns: ["id"]
        ) }

        // Verify the constraint exists
        let exists = try await constraintClient.constraintExists(name: constraintName, table: childTableName)
        XCTAssertTrue(exists, "Foreign key constraint should exist after creation")

        // Test that the constraint is enforced - valid insert should work
        let validInsertSql = "INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (1, 1, N'Valid reference')"
        _ = try await client.execute(validInsertSql)

        // Test that the constraint is enforced - invalid insert should fail
        do {
            let invalidInsertSql = "INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (2, 999, N'Invalid reference')"
            _ = try await client.execute(invalidInsertSql)
            XCTFail("Insert with invalid foreign key should have failed")
        } catch {
            // Expected to fail due to foreign key constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddForeignKeyConstraintWithCascadeOptions() async throws {
        let parentTableName = "test_fk_cascade_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_fk_cascade_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(childTableName)_cascade"

        // Create parent table
        try await createReferenceTable(name: parentTableName)

        // Create child table
        let childColumns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        try await withTimeout(15) { try await self.adminClient.createTable(name: childTableName, columns: childColumns) }

        // Add foreign key constraint with cascade delete
        let options = ForeignKeyOptions(onDelete: .cascade, onUpdate: .cascade)
        try await withTimeout(15) { try await self.constraintClient.addForeignKey(
            name: constraintName,
            table: childTableName,
            columns: ["parent_id"],
            referencedTable: parentTableName,
            referencedColumns: ["id"],
            options: options
        ) }

        // Insert test data
        let insertChildSql = "INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (1, 1, N'Test record')"
        _ = try await client.execute(insertChildSql)

        // Verify child record exists
        var childCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(childTableName)] WHERE parent_id = 1", as: Int.self)
        XCTAssertEqual(childCount, 1)

        // Delete parent record - should cascade to child
        let deleteParentSql = "DELETE FROM [\(parentTableName)] WHERE id = 1"
        _ = try await client.execute(deleteParentSql)

        // Verify child record was deleted due to cascade
        childCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(childTableName)] WHERE parent_id = 1", as: Int.self)
        XCTAssertEqual(childCount, 0)
    }

    func testDropForeignKeyConstraint() async throws {
        let parentTableName = "test_drop_fk_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_drop_fk_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_\(childTableName)_drop"

        // Create parent and child tables
        try await createReferenceTable(name: parentTableName)
        let childColumns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "parent_id", definition: .standard(.init(dataType: .int)))
        ]
        try await withTimeout(15) { try await self.adminClient.createTable(name: childTableName, columns: childColumns) }

        // Add foreign key constraint
        try await withTimeout(15) { try await self.constraintClient.addForeignKey(
            name: constraintName,
            table: childTableName,
            columns: ["parent_id"],
            referencedTable: parentTableName,
            referencedColumns: ["id"]
        ) }

        // Verify it exists
        var exists = try await constraintClient.constraintExists(name: constraintName, table: childTableName)
        XCTAssertTrue(exists, "Foreign key constraint should exist after creation")

        // Drop the constraint
        try await constraintClient.dropForeignKey(name: constraintName, table: childTableName)

        // Verify it's gone
        exists = try await constraintClient.constraintExists(name: constraintName, table: childTableName)
        XCTAssertFalse(exists, "Foreign key constraint should not exist after being dropped")
    }

    // MARK: - Check Constraint Tests

    func testAddCheckConstraint() async throws {
        let tableName = "test_check_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_age"

        // Create test table
        try await createTestTable(name: tableName)

        // Add check constraint
        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "age >= 0 AND age <= 150"
        ) }

        // Verify the constraint exists
        let exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Check constraint should exist after creation")

        // Test that the constraint is enforced - valid insert should work
        let validInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await client.execute(validInsertSql)

        // Test that the constraint is enforced - invalid insert should fail
        do {
            let invalidInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'Jane', N'jane@test.com', 200, N'active')"
            _ = try await client.execute(invalidInsertSql)
            XCTFail("Insert with invalid age should have failed")
        } catch {
            // Expected to fail due to check constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropCheckConstraint() async throws {
        let tableName = "test_drop_check_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_status"

        // Create test table
        try await createTestTable(name: tableName)

        // Add check constraint
        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "status IN ('active', 'inactive', 'pending')"
        ) }

        // Verify it exists
        var exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Check constraint should exist after creation")

        // Drop the constraint
        try await constraintClient.dropCheckConstraint(name: constraintName, table: tableName)

        // Verify it's gone
        exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(exists, "Check constraint should not exist after being dropped")
    }

    // MARK: - Unique Constraint Tests

    func testAddUniqueConstraint() async throws {
        let tableName = "test_unique_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_email"

        // Create test table
        try await createTestTable(name: tableName)

        // Add unique constraint
        try await withTimeout(15) { try await self.constraintClient.addUniqueConstraint(
            name: constraintName,
            table: tableName,
            columns: ["email"]
        ) }

        // Verify the constraint exists
        let exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Unique constraint should exist after creation")

        // Test that the constraint is enforced - first insert should work
        let firstInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await client.execute(firstInsertSql)

        // Test that the constraint is enforced - duplicate email should fail
        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'Jane', N'john@test.com', 30, N'active')"
            _ = try await client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate email should have failed")
        } catch {
            // Expected to fail due to unique constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddMultiColumnUniqueConstraint() async throws {
        let tableName = "test_multi_unique_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_name_email"

        // Create test table
        try await createTestTable(name: tableName)

        // Add multi-column unique constraint
        try await withTimeout(15) { try await self.constraintClient.addUniqueConstraint(
            name: constraintName,
            table: tableName,
            columns: ["name", "email"]
        ) }

        // Test that the constraint allows same name with different email
        let insert1Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john1@test.com', 25, N'active')"
        let insert2Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'John', N'john2@test.com', 30, N'active')"
        _ = try await client.execute(insert1Sql)
        _ = try await client.execute(insert2Sql)

        // Test that duplicate combination fails
        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (3, N'John', N'john1@test.com', 35, N'active')"
            _ = try await client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate name+email combination should have failed")
        } catch {
            // Expected to fail due to unique constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropUniqueConstraint() async throws {
        let tableName = "test_drop_unique_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "UQ_\(tableName)_email"

        // Create test table
        try await createTestTable(name: tableName)

        // Add unique constraint
        try await constraintClient.addUniqueConstraint(
            name: constraintName,
            table: tableName,
            columns: ["email"]
        )

        // Verify it exists
        var exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Unique constraint should exist after creation")

        // Drop the constraint
        try await constraintClient.dropUniqueConstraint(name: constraintName, table: tableName)

        // Verify it's gone
        exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(exists, "Unique constraint should not exist after being dropped")
    }

    // MARK: - Primary Key Constraint Tests

    func testAddPrimaryKeyConstraint() async throws {
        let tableName = "test_pk_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "PK_\(tableName)"

        // Create test table without primary key
        try await createTestTable(name: tableName, withPrimaryKey: false)

        // Add primary key constraint
        try await withTimeout(15) { try await self.constraintClient.addPrimaryKey(
            name: constraintName,
            table: tableName,
            columns: ["id"]
        ) }

        // Verify the constraint exists
        let exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Primary key constraint should exist after creation")

        // Test that the constraint is enforced - first insert should work
        let firstInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await client.execute(firstInsertSql)

        // Test that the constraint is enforced - duplicate primary key should fail
        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'Jane', N'jane@test.com', 30, N'active')"
            _ = try await client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate primary key should have failed")
        } catch {
            // Expected to fail due to primary key constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddCompositePrimaryKeyConstraint() async throws {
        let tableName = "test_composite_pk_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "PK_\(tableName)_composite"

        // Create test table without primary key
        try await createTestTable(name: tableName, withPrimaryKey: false)

        // Add composite primary key constraint
        try await withTimeout(15) { try await self.constraintClient.addPrimaryKey(
            name: constraintName,
            table: tableName,
            columns: ["id", "name"]
        ) }

        // Test that the constraint allows same id with different name
        let insert1Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        let insert2Sql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'Jane', N'jane@test.com', 30, N'active')"
        _ = try await client.execute(insert1Sql)
        _ = try await client.execute(insert2Sql)

        // Test that duplicate combination fails
        do {
            let duplicateInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john2@test.com', 35, N'active')"
            _ = try await client.execute(duplicateInsertSql)
            XCTFail("Insert with duplicate composite primary key should have failed")
        } catch {
            // Expected to fail due to primary key constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    // MARK: - Default Constraint Tests

    func testAddDefaultConstraint() async throws {
        let tableName = "test_default_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "DF_\(tableName)_status"

        // Create test table
        try await createTestTable(name: tableName)

        // Add default constraint
        try await withTimeout(15) { try await self.constraintClient.addDefaultConstraint(
            name: constraintName,
            table: tableName,
            column: "status",
            defaultValue: "'pending'"
        ) }

        // Verify the constraint exists
        let exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Default constraint should exist after creation")

        // Test that the default value is applied
        let insertSql = "INSERT INTO [\(tableName)] (id, name, email, age) VALUES (1, N'John', N'john@test.com', 25)"
        _ = try await client.execute(insertSql)

        let result = try await client.query("SELECT status FROM [\(tableName)] WHERE id = 1")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("status")?.string, "pending")
    }

    func testDropDefaultConstraint() async throws {
        let tableName = "test_drop_default_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "DF_\(tableName)_status"

        // Create test table
        try await createTestTable(name: tableName)

        // Add default constraint
        try await constraintClient.addDefaultConstraint(
            name: constraintName,
            table: tableName,
            column: "status",
            defaultValue: "'active'"
        )

        // Verify it exists
        var exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertTrue(exists, "Default constraint should exist after creation")

        // Drop the constraint
        try await constraintClient.dropDefaultConstraint(name: constraintName, table: tableName)

        // Verify it's gone
        exists = try await constraintClient.constraintExists(name: constraintName, table: tableName)
        XCTAssertFalse(exists, "Default constraint should not exist after being dropped")
    }

    // MARK: - Constraint Information Tests

    func testListTableConstraints() async throws {
        let tableName = "test_list_constraints_table_\(UUID().uuidString.prefix(8))"
        let checkConstraintName = "CK_\(tableName)_age"
        let uniqueConstraintName = "UQ_\(tableName)_email"
        

        // Create test table with primary key
        try await createTestTable(name: tableName, withPrimaryKey: true)

        // Add various constraints
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

        // List all constraints
        let constraints = try await constraintClient.listTableConstraints(table: tableName)

        // Should have at least our constraints plus the primary key
        XCTAssertGreaterThanOrEqual(constraints.count, 2)

        let constraintNames = constraints.map { $0.name }
        XCTAssertTrue(constraintNames.contains(checkConstraintName))
        XCTAssertTrue(constraintNames.contains(uniqueConstraintName))

        // Verify constraint types
        let checkConstraint = constraints.first { $0.name == checkConstraintName }
        XCTAssertEqual(checkConstraint?.type, .check)

        let uniqueConstraint = constraints.first { $0.name == uniqueConstraintName }
        XCTAssertEqual(uniqueConstraint?.type, .unique)
    }

    // MARK: - Constraint Enable/Disable Tests

    func testEnableDisableConstraint() async throws {
        let tableName = "test_enable_disable_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_age"

        // Create test table
        try await createTestTable(name: tableName)

        // Add check constraint
        try await constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "age >= 0 AND age <= 150"
        )

        // Insert valid data
        let validInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (1, N'John', N'john@test.com', 25, N'active')"
        _ = try await client.execute(validInsertSql)

        // Disable the constraint
        try await constraintClient.disableConstraint(name: constraintName, table: tableName)

        // Now invalid data should be allowed
        let invalidInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (2, N'Jane', N'jane@test.com', 200, N'active')"
        _ = try await client.execute(invalidInsertSql)

        // Re-enable the constraint
        try await constraintClient.enableConstraint(name: constraintName, table: tableName)

        // Invalid data should now be rejected again
        do {
            let anotherInvalidInsertSql = "INSERT INTO [\(tableName)] (id, name, email, age, status) VALUES (3, N'Bob', N'bob@test.com', 300, N'active')"
            _ = try await client.execute(anotherInvalidInsertSql)
            XCTFail("Insert with invalid age should have failed after re-enabling constraint")
        } catch {
            // Expected to fail due to re-enabled check constraint
            XCTAssertTrue(error is SQLServerError)
        }
    }

    // MARK: - Error Handling Tests

    func testAddDuplicateConstraint() async throws {
        let tableName = "test_duplicate_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_\(tableName)_duplicate"

        // Create test table
        try await createTestTable(name: tableName)

        // Add the first constraint
        try await withTimeout(15) { try await self.constraintClient.addCheckConstraint(
            name: constraintName,
            table: tableName,
            expression: "age >= 0"
        ) }

        // Attempt to add duplicate should fail
        do {
            try await constraintClient.addCheckConstraint(
                name: constraintName,
                table: tableName,
                expression: "age <= 100"
            )
            XCTFail("Adding duplicate constraint should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentConstraint() async throws {
        let tableName = "test_nonexistent_constraint_table_\(UUID().uuidString.prefix(8))"
        let constraintName = "CK_nonexistent"

        // Create test table
        try await createTestTable(name: tableName)

        // Attempt to drop non-existent constraint should fail
        do {
            try await constraintClient.dropCheckConstraint(name: constraintName, table: tableName)
            XCTFail("Dropping non-existent constraint should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddForeignKeyWithMismatchedColumns() async throws {
        let parentTableName = "test_mismatch_parent_\(UUID().uuidString.prefix(8))"
        let childTableName = "test_mismatch_child_\(UUID().uuidString.prefix(8))"
        let constraintName = "FK_mismatch"

        // Create tables
        try await createReferenceTable(name: parentTableName)
        try await createTestTable(name: childTableName)

        // Attempt to add foreign key with mismatched column counts should fail
        do {
            try await constraintClient.addForeignKey(
                name: constraintName,
                table: childTableName,
                columns: ["id", "name"], // 2 columns
                referencedTable: parentTableName,
                referencedColumns: ["id"] // 1 column
            )
            XCTFail("Adding foreign key with mismatched column counts should have failed")
        } catch {
            // Expected to fail due to validation
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
