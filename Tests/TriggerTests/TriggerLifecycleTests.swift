import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerTriggerTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var triggerClient: SQLServerTriggerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var triggersToDrop: [(name: String, schema: String)] = []
    private var tablesToDrop: [String] = []

    private var eventLoop: EventLoop { self.group.next() }

    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        self.triggerClient = SQLServerTriggerClient(client: client)
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDownWithError() throws {
        // Drop any triggers that were created during the test using SQLServerTriggerClient
        for trigger in triggersToDrop {
            do {
                try triggerClient.dropTrigger(name: trigger.name, schema: trigger.schema).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop trigger \(trigger.name): \(error)")
            }
        }
        triggersToDrop.removeAll()

        // Drop any tables that were created during the test
        for table in tablesToDrop {
            do {
                try adminClient.dropTable(name: table).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop table \(table): \(error)")
            }
        }
        tablesToDrop.removeAll()

        try self.client.shutdownGracefully().wait()
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }

    // MARK: - Helper Methods

    private func createTestTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200))))),
            SQLServerColumnDefinition(name: "created_date", definition: .standard(.init(dataType: .datetime2(precision: 3)))),
            SQLServerColumnDefinition(name: "modified_date", definition: .standard(.init(dataType: .datetime2(precision: 3))))
        ]
        
        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }

    private func createAuditTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "audit_id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (1, 1)))),
            SQLServerColumnDefinition(name: "table_name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "operation", definition: .standard(.init(dataType: .nvarchar(length: .length(10))))),
            SQLServerColumnDefinition(name: "record_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "old_values", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: true))),
            SQLServerColumnDefinition(name: "new_values", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: true))),
            SQLServerColumnDefinition(name: "audit_date", definition: .standard(.init(dataType: .datetime2(precision: 3), defaultValue: "GETDATE()")))
        ]
        
        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }

    // MARK: - Basic Trigger Tests

    func testCreateSimpleInsertTrigger() async throws {
        let tableName = "test_insert_trigger_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_insert_audit_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_insert"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Create insert trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id, new_values)
            SELECT '\(tableName)', 'INSERT', inserted.id, 
                   CONCAT('name=', inserted.name, ';email=', inserted.email)
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body
        )

        // Verify the trigger exists
        let exists = try await triggerClient.triggerExists(name: triggerName, table: tableName)
        XCTAssertTrue(exists, "Insert trigger should exist after creation")

        // Test the trigger by inserting data
        let insertSql = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql)

        // Verify the trigger fired and created audit record
        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Audit table should have one record after insert")

        let auditRecord = try await client.query("SELECT * FROM [\(auditTableName)]")
        XCTAssertEqual(auditRecord.count, 1)
        XCTAssertEqual(auditRecord.first?.column("operation")?.string, "INSERT")
        XCTAssertEqual(auditRecord.first?.column("record_id")?.int, 1)
    }

    func testCreateUpdateTrigger() async throws {
        let tableName = "test_update_trigger_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_update_audit_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_update"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Insert initial data
        let insertSql = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql)

        // Create update trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id, old_values, new_values)
            SELECT '\(tableName)', 'UPDATE', inserted.id,
                   CONCAT('name=', deleted.name, ';email=', deleted.email),
                   CONCAT('name=', inserted.name, ';email=', inserted.email)
            FROM inserted
            INNER JOIN deleted ON inserted.id = deleted.id;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.update],
            body: body
        )

        // Test the trigger by updating data
        let updateSql = "UPDATE [\(tableName)] SET name = N'Jane Doe', email = N'jane@example.com' WHERE id = 1"
        _ = try await client.execute(updateSql)

        // Verify the trigger fired
        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Audit table should have one record after update")

        let auditRecord = try await client.query("SELECT * FROM [\(auditTableName)]")
        XCTAssertEqual(auditRecord.first?.column("operation")?.string, "UPDATE")
        XCTAssertTrue(auditRecord.first?.column("old_values")?.string?.contains("John Doe") == true)
        XCTAssertTrue(auditRecord.first?.column("new_values")?.string?.contains("Jane Doe") == true)
    }

    func testCreateDeleteTrigger() async throws {
        let tableName = "test_delete_trigger_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_delete_audit_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_delete"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Insert initial data
        let insertSql = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql)

        // Create delete trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id, old_values)
            SELECT '\(tableName)', 'DELETE', deleted.id,
                   CONCAT('name=', deleted.name, ';email=', deleted.email)
            FROM deleted;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.delete],
            body: body
        )

        // Test the trigger by deleting data
        let deleteSql = "DELETE FROM [\(tableName)] WHERE id = 1"
        _ = try await client.execute(deleteSql)

        // Verify the trigger fired
        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Audit table should have one record after delete")

        let auditRecord = try await client.query("SELECT * FROM [\(auditTableName)]")
        XCTAssertEqual(auditRecord.first?.column("operation")?.string, "DELETE")
        XCTAssertTrue(auditRecord.first?.column("old_values")?.string?.contains("John Doe") == true)
    }

    func testCreateMultiEventTrigger() async throws {
        let tableName = "test_multi_event_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_multi_event_audit_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_multi"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Create trigger for multiple events
        let body = """
        BEGIN
            SET NOCOUNT ON;
            
            -- Handle INSERT
            IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
            BEGIN
                INSERT INTO [\(auditTableName)] (table_name, operation, record_id, new_values)
                SELECT '\(tableName)', 'INSERT', inserted.id, 
                       CONCAT('name=', inserted.name, ';email=', inserted.email)
                FROM inserted;
            END
            
            -- Handle UPDATE
            IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
            BEGIN
                INSERT INTO [\(auditTableName)] (table_name, operation, record_id, old_values, new_values)
                SELECT '\(tableName)', 'UPDATE', inserted.id,
                       CONCAT('name=', deleted.name, ';email=', deleted.email),
                       CONCAT('name=', inserted.name, ';email=', inserted.email)
                FROM inserted
                INNER JOIN deleted ON inserted.id = deleted.id;
            END
            
            -- Handle DELETE
            IF NOT EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
            BEGIN
                INSERT INTO [\(auditTableName)] (table_name, operation, record_id, old_values)
                SELECT '\(tableName)', 'DELETE', deleted.id,
                       CONCAT('name=', deleted.name, ';email=', deleted.email)
                FROM deleted;
            END
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert, .update, .delete],
            body: body
        )

        // Test INSERT
        let insertSql = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql)

        // Test UPDATE
        let updateSql = "UPDATE [\(tableName)] SET name = N'Jane Doe' WHERE id = 1"
        _ = try await client.execute(updateSql)

        // Test DELETE
        let deleteSql = "DELETE FROM [\(tableName)] WHERE id = 1"
        _ = try await client.execute(deleteSql)

        // Verify all operations were audited
        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 3, "Audit table should have three records")

        let operations = try await client.query("SELECT operation FROM [\(auditTableName)] ORDER BY audit_id")
        XCTAssertEqual(operations[0].column("operation")?.string, "INSERT")
        XCTAssertEqual(operations[1].column("operation")?.string, "UPDATE")
        XCTAssertEqual(operations[2].column("operation")?.string, "DELETE")
    }

    func testCreateInsteadOfTrigger() async throws {
        let viewName = "test_instead_of_view_\(UUID().uuidString.prefix(8))"
        let tableName = "test_instead_of_table_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(viewName)_instead_of"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create a view
        let viewSql = "CREATE VIEW [\(viewName)] AS SELECT id, name, email FROM [\(tableName)]"
        _ = try await client.execute(viewSql)
        tablesToDrop.append(viewName) // Views can be dropped like tables

        // Create INSTEAD OF trigger on the view
        let body = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date)
            SELECT id, name, email, GETDATE(), GETDATE()
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: viewName,
            timing: .insteadOf,
            events: [.insert],
            body: body
        )

        // Test the INSTEAD OF trigger
        let insertSql = "INSERT INTO [\(viewName)] (id, name, email) VALUES (1, N'John Doe', N'john@example.com')"
        _ = try await client.execute(insertSql)

        // Verify data was inserted into the underlying table
        let count = try await client.queryScalar("SELECT COUNT(*) FROM [\(tableName)]", as: Int.self)
        XCTAssertEqual(count, 1, "Underlying table should have one record")

        let record = try await client.query("SELECT * FROM [\(tableName)]")
        XCTAssertEqual(record.first?.column("name")?.string, "John Doe")
        XCTAssertNotNil(record.first?.column("created_date"))
    }

    func testAlterTrigger() async throws {
        let tableName = "test_alter_trigger_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_alter_audit_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_alter"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Create initial trigger
        let initialBody = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id)
            SELECT '\(tableName)', 'INSERT', inserted.id
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: initialBody
        )

        // Test initial trigger
        let insertSql1 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql1)

        var auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1)

        // Alter the trigger to include more details
        let alteredBody = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id, new_values)
            SELECT '\(tableName)', 'INSERT', inserted.id,
                   CONCAT('name=', inserted.name, ';email=', inserted.email)
            FROM inserted;
        END
        """

        try await triggerClient.alterTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: alteredBody
        )

        // Test altered trigger
        let insertSql2 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (2, N'Jane Doe', N'jane@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql2)

        auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 2)

        // Verify the altered trigger includes new_values
        let latestAudit = try await client.query("SELECT * FROM [\(auditTableName)] WHERE record_id = 2")
        XCTAssertNotNil(latestAudit.first?.column("new_values")?.string)
        XCTAssertTrue(latestAudit.first?.column("new_values")?.string?.contains("Jane Doe") == true)
    }

    func testDropTrigger() async throws {
        let tableName = "test_drop_trigger_table_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_drop"

        // Create test table
        try await createTestTable(name: tableName)

        // Create trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
            -- Simple trigger body
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body
        )

        // Verify it exists
        var exists = try await triggerClient.triggerExists(name: triggerName, table: tableName)
        XCTAssertTrue(exists, "Trigger should exist after creation")

        // Drop the trigger
        try await triggerClient.dropTrigger(name: triggerName)

        // Verify it's gone
        exists = try await triggerClient.triggerExists(name: triggerName, table: tableName)
        XCTAssertFalse(exists, "Trigger should not exist after being dropped")
    }

    // MARK: - Trigger Management Tests

    func testEnableDisableTrigger() async throws {
        let tableName = "test_enable_disable_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_enable_disable_audit_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_enable_disable"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Create trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id)
            SELECT '\(tableName)', 'INSERT', inserted.id
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body
        )

        // Test trigger is initially enabled
        let insertSql1 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql1)

        var auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Trigger should fire when enabled")

        // Disable the trigger
        try await triggerClient.disableTrigger(name: triggerName, table: tableName)

        // Test trigger doesn't fire when disabled
        let insertSql2 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (2, N'Jane Doe', N'jane@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql2)

        auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Trigger should not fire when disabled")

        // Re-enable the trigger
        try await triggerClient.enableTrigger(name: triggerName, table: tableName)

        // Test trigger fires again when re-enabled
        let insertSql3 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (3, N'Bob Smith', N'bob@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql3)

        auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 2, "Trigger should fire when re-enabled")
    }

    func testEnableDisableAllTriggers() async throws {
        let tableName = "test_all_triggers_table_\(UUID().uuidString.prefix(8))"
        let auditTableName = "test_all_triggers_audit_\(UUID().uuidString.prefix(8))"
        let trigger1Name = "tr_\(tableName)_1"
        let trigger2Name = "tr_\(tableName)_2"
        triggersToDrop.append((name: trigger1Name, schema: "dbo"))
        triggersToDrop.append((name: trigger2Name, schema: "dbo"))

        // Create test and audit tables
        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        // Create two triggers
        let body1 = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id)
            SELECT '\(tableName)', 'INSERT1', inserted.id
            FROM inserted;
        END
        """

        let body2 = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id)
            SELECT '\(tableName)', 'INSERT2', inserted.id
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(
            name: trigger1Name,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body1
        )

        try await triggerClient.createTrigger(
            name: trigger2Name,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body2
        )

        // Test both triggers fire
        let insertSql1 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (1, N'John Doe', N'john@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql1)

        var auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 2, "Both triggers should fire")

        // Disable all triggers
        try await triggerClient.disableAllTriggers(table: tableName)

        // Test no triggers fire
        let insertSql2 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (2, N'Jane Doe', N'jane@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql2)

        auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 2, "No triggers should fire when all disabled")

        // Re-enable all triggers
        try await triggerClient.enableAllTriggers(table: tableName)

        // Test both triggers fire again
        let insertSql3 = """
        INSERT INTO [\(tableName)] (id, name, email, created_date, modified_date) 
        VALUES (3, N'Bob Smith', N'bob@example.com', GETDATE(), GETDATE())
        """
        _ = try await client.execute(insertSql3)

        auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 4, "Both triggers should fire when re-enabled")
    }

    // MARK: - Trigger Information Tests

    func testGetTriggerInfo() async throws {
        let tableName = "test_info_trigger_table_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_info"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
            -- Test trigger for info
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert, .update],
            body: body
        )

        // Get trigger info
        let triggerInfo = try await triggerClient.getTriggerInfo(name: triggerName, table: tableName)
        XCTAssertNotNil(triggerInfo)
        XCTAssertEqual(triggerInfo?.name, triggerName)
        XCTAssertEqual(triggerInfo?.tableName, tableName)
        XCTAssertEqual(triggerInfo?.schemaName, "dbo")
        XCTAssertEqual(triggerInfo?.timing, "AFTER")
        XCTAssertFalse(triggerInfo?.isDisabled == true)
        XCTAssertNotNil(triggerInfo?.definition)
        
        // Check events (may vary in order)
        XCTAssertGreaterThanOrEqual(triggerInfo?.events.count ?? 0, 2)
        XCTAssertTrue(triggerInfo?.events.contains("INSERT") == true)
        XCTAssertTrue(triggerInfo?.events.contains("UPDATE") == true)
    }

    func testListTableTriggers() async throws {
        let tableName = "test_list_triggers_table_\(UUID().uuidString.prefix(8))"
        let trigger1Name = "tr_\(tableName)_1"
        let trigger2Name = "tr_\(tableName)_2"
        triggersToDrop.append((name: trigger1Name, schema: "dbo"))
        triggersToDrop.append((name: trigger2Name, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create multiple triggers
        let body1 = """
        BEGIN
            SET NOCOUNT ON;
            -- Trigger 1
        END
        """

        let body2 = """
        BEGIN
            SET NOCOUNT ON;
            -- Trigger 2
        END
        """

        try await triggerClient.createTrigger(
            name: trigger1Name,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body1
        )

        try await triggerClient.createTrigger(
            name: trigger2Name,
            table: tableName,
            timing: .after,
            events: [.update],
            body: body2
        )

        // List all triggers for the table
        let triggers = try await triggerClient.listTableTriggers(table: tableName)
        XCTAssertEqual(triggers.count, 2)

        let triggerNames = triggers.map { $0.name }
        XCTAssertTrue(triggerNames.contains(trigger1Name))
        XCTAssertTrue(triggerNames.contains(trigger2Name))
    }

    func testGetTriggerDefinition() async throws {
        let tableName = "test_definition_table_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_definition"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create trigger with specific body
        let body = """
        BEGIN
            SET NOCOUNT ON;
            -- This is a test trigger for definition retrieval
            PRINT 'Trigger fired';
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body
        )

        // Get trigger definition
        let definition = try await triggerClient.getTriggerDefinition(name: triggerName, table: tableName)
        XCTAssertNotNil(definition)
        XCTAssertTrue(definition?.contains("SET NOCOUNT ON") == true)
        XCTAssertTrue(definition?.contains("test trigger for definition") == true)
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateTrigger() async throws {
        let tableName = "test_duplicate_trigger_table_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_duplicate"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create the first trigger
        let body = """
        BEGIN
            SET NOCOUNT ON;
        END
        """

        try await triggerClient.createTrigger(
            name: triggerName,
            table: tableName,
            timing: .after,
            events: [.insert],
            body: body
        )

        // Attempt to create duplicate should fail
        do {
            try await triggerClient.createTrigger(
                name: triggerName,
                table: tableName,
                timing: .after,
                events: [.update],
                body: body
            )
            XCTFail("Creating duplicate trigger should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentTrigger() async throws {
        let triggerName = "tr_nonexistent_trigger"

        // Attempt to drop non-existent trigger should fail
        do {
            try await triggerClient.dropTrigger(name: triggerName)
            XCTFail("Dropping non-existent trigger should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateTriggerWithNoEvents() async throws {
        let tableName = "test_no_events_table_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_no_events"

        // Create test table
        try await createTestTable(name: tableName)

        let body = """
        BEGIN
            SET NOCOUNT ON;
        END
        """

        // Attempt to create trigger with no events should fail
        do {
            try await triggerClient.createTrigger(
                name: triggerName,
                table: tableName,
                timing: .after,
                events: [],
                body: body
            )
            XCTFail("Creating trigger with no events should have failed")
        } catch {
            // Expected to fail due to validation
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateTriggerOnNonExistentTable() async throws {
        let tableName = "non_existent_table"
        let triggerName = "tr_test"

        let body = """
        BEGIN
            SET NOCOUNT ON;
        END
        """

        // Attempt to create trigger on non-existent table should fail
        do {
            try await triggerClient.createTrigger(
                name: triggerName,
                table: tableName,
                timing: .after,
                events: [.insert],
                body: body
            )
            XCTFail("Creating trigger on non-existent table should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }
}