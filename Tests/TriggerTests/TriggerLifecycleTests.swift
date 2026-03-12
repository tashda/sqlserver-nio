import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class TriggerLifecycleTests: TriggerTestBase, @unchecked Sendable {
    private func insertPerson(into objectName: String, id: Int, name: String, email: String) async throws {
        try await client.withConnection { connection in
            try await connection.insertRow(
                into: objectName,
                values: [
                    "id": .int(id),
                    "name": .nString(name),
                    "email": .nString(email),
                    "created_date": .raw("GETDATE()"),
                    "modified_date": .raw("GETDATE()")
                ]
            )
        }
    }

    // MARK: - Basic Trigger Tests

    func testCreateSimpleInsertTrigger() async throws {
        let tableName = "tr_in_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_in_a_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_insert"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

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

        let exists = try await triggerClient.triggerExists(name: triggerName, table: tableName)
        XCTAssertTrue(exists, "Insert trigger should exist after creation")

        try await insertPerson(into: tableName, id: 1, name: "John Doe", email: "john@example.com")

        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Audit table should have one record after insert")

        let auditRecord = try await client.query("""
            SELECT 
                audit_id,
                table_name,
                operation,
                record_id,
                CAST(old_values AS NVARCHAR(4000)) AS old_values,
                CAST(new_values AS NVARCHAR(4000)) AS new_values,
                audit_date
            FROM [\(auditTableName)]
            """)
        XCTAssertEqual(auditRecord.count, 1)
        XCTAssertEqual(auditRecord.first?.column("operation")?.string, "INSERT")
        XCTAssertEqual(auditRecord.first?.column("record_id")?.int, 1)
    }

    func testCreateUpdateTrigger() async throws {
        let tableName = "tr_up_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_up_a_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_update"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        try await insertPerson(into: tableName, id: 1, name: "John Doe", email: "john@example.com")

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

        try await client.withConnection { connection in
            try await connection.updateRows(in: tableName, set: ["name": .nString("Jane Doe"), "email": .nString("jane@example.com")], where: "id = 1")
        }

        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Audit table should have one record after update")

        let auditRecord = try await client.query("""
            SELECT 
                audit_id,
                table_name,
                operation,
                record_id,
                CAST(old_values AS NVARCHAR(4000)) AS old_values,
                CAST(new_values AS NVARCHAR(4000)) AS new_values,
                audit_date
            FROM [\(auditTableName)]
            """)
        XCTAssertEqual(auditRecord.first?.column("operation")?.string, "UPDATE")
        XCTAssertTrue(auditRecord.first?.column("old_values")?.string?.contains("John Doe") == true)
        XCTAssertTrue(auditRecord.first?.column("new_values")?.string?.contains("Jane Doe") == true)
    }

    func testCreateDeleteTrigger() async throws {
        let tableName = "tr_dl_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_dl_a_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_delete"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        try await insertPerson(into: tableName, id: 1, name: "John Doe", email: "john@example.com")

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

        try await client.withConnection { connection in
            try await connection.deleteRows(from: tableName, where: "id = 1")
        }

        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 1, "Audit table should have one record after delete")

        let auditRecord = try await client.query("""
            SELECT 
                audit_id,
                table_name,
                operation,
                record_id,
                CAST(old_values AS NVARCHAR(4000)) AS old_values,
                CAST(new_values AS NVARCHAR(4000)) AS new_values,
                audit_date
            FROM [\(auditTableName)]
            """)
        XCTAssertEqual(auditRecord.first?.column("operation")?.string, "DELETE")
        XCTAssertTrue(auditRecord.first?.column("old_values")?.string?.contains("John Doe") == true)
    }

    func testCreateMultiEventTrigger() async throws {
        let tableName = "tr_ml_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_ml_a_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_multi"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        let body = """
        BEGIN
            SET NOCOUNT ON;
            IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
            BEGIN
                INSERT INTO [\(auditTableName)] (table_name, operation, record_id, new_values)
                SELECT '\(tableName)', 'INSERT', inserted.id, 
                       CONCAT('name=', inserted.name, ';email=', inserted.email)
                FROM inserted;
            END
            IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
            BEGIN
                INSERT INTO [\(auditTableName)] (table_name, operation, record_id, old_values, new_values)
                SELECT '\(tableName)', 'UPDATE', inserted.id,
                       CONCAT('name=', deleted.name, ';email=', deleted.email),
                       CONCAT('name=', inserted.name, ';email=', inserted.email)
                FROM inserted
                INNER JOIN deleted ON inserted.id = deleted.id;
            END
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

        try await insertPerson(into: tableName, id: 1, name: "John Doe", email: "john@example.com")
        try await client.withConnection { connection in
            try await connection.updateRows(in: tableName, set: ["name": .nString("Jane Doe")], where: "id = 1")
            try await connection.deleteRows(from: tableName, where: "id = 1")
        }

        let auditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(auditCount, 3, "Audit table should have three records")

        let operations = try await client.query("SELECT operation FROM [\(auditTableName)] ORDER BY audit_id")
        XCTAssertEqual(operations[0].column("operation")?.string, "INSERT")
        XCTAssertEqual(operations[1].column("operation")?.string, "UPDATE")
        XCTAssertEqual(operations[2].column("operation")?.string, "DELETE")
    }

    func testCreateInsteadOfTrigger() async throws {
        let viewName = "tr_io_v_\(UUID().uuidString.prefix(8))"
        let tableName = "tr_io_t_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(viewName)_instead_of"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        let viewClient = SQLServerViewClient(client: client)
        try await viewClient.createView(name: viewName, query: "SELECT id, name, email FROM [\(tableName)]")
        tablesToDrop.append(viewName)

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

        try await client.withConnection { connection in
            try await connection.insertRow(into: viewName, values: ["id": .int(1), "name": .nString("John Doe"), "email": .nString("john@example.com")])
        }

        let count = try await client.queryScalar("SELECT COUNT(*) FROM [\(tableName)]", as: Int.self)
        XCTAssertEqual(count, 1, "Underlying table should have one record")
    }

    func testAlterTrigger() async throws {
        let tableName = "tr_al_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_al_a_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_alter"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        let initialBody = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id)
            SELECT '\(tableName)', 'INSERT', inserted.id
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert], body: initialBody)
        try await insertPerson(into: tableName, id: 1, name: "John Doe", email: "john@example.com")

        let alteredBody = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id, new_values)
            SELECT '\(tableName)', 'INSERT', inserted.id,
                   CONCAT('name=', inserted.name, ';email=', inserted.email)
            FROM inserted;
        END
        """

        try await triggerClient.alterTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert], body: alteredBody)
        try await insertPerson(into: tableName, id: 2, name: "Jane Doe", email: "jane@example.com")

        let latestAudit = try await client.query("SELECT CAST(new_values AS NVARCHAR(4000)) AS new_values FROM [\(auditTableName)] WHERE record_id = 2")
        XCTAssertTrue(latestAudit.first?.column("new_values")?.string?.contains("Jane Doe") == true)
    }

    func testDropTrigger() async throws {
        let tableName = "tr_dr_t_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_drop"

        try await createTestTable(name: tableName)
        try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert], body: "BEGIN SET NOCOUNT ON; END")

        let existsBeforeDrop = try await triggerClient.triggerExists(name: triggerName, table: tableName)
        XCTAssertTrue(existsBeforeDrop)
        try await triggerClient.dropTrigger(name: triggerName)
        let existsAfterDrop = try await triggerClient.triggerExists(name: triggerName, table: tableName)
        XCTAssertFalse(existsAfterDrop)
    }
}
