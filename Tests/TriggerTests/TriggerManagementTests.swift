import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class TriggerManagementTests: TriggerTestBase, @unchecked Sendable {
    private func insertPerson(into tableName: String, id: Int, name: String, email: String) async throws {
        try await client.withConnection { connection in
            try await connection.insertRow(
                into: tableName,
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

    func testEnableDisableTrigger() async throws {
        let tableName = "tr_ed_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_ed_a_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_enable_disable"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        let body = """
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO [\(auditTableName)] (table_name, operation, record_id)
            SELECT '\(tableName)', 'INSERT', inserted.id
            FROM inserted;
        END
        """

        try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert], body: body)

        try await insertPerson(into: tableName, id: 1, name: "John Doe", email: "john@example.com")
        let firstAuditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(firstAuditCount, 1)

        try await triggerClient.disableTrigger(name: triggerName, table: tableName)
        try await insertPerson(into: tableName, id: 2, name: "Jane Doe", email: "jane@example.com")
        let secondAuditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(secondAuditCount, 1)

        try await triggerClient.enableTrigger(name: triggerName, table: tableName)
        try await insertPerson(into: tableName, id: 3, name: "Bob Smith", email: "bob@example.com")
        let thirdAuditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(thirdAuditCount, 2)
    }

    func testEnableDisableAllTriggers() async throws {
        let tableName = "tr_ea_t_\(UUID().uuidString.prefix(8))"
        let auditTableName = "tr_ea_a_\(UUID().uuidString.prefix(8))"
        let tr1 = "tr_\(tableName)_1"
        let tr2 = "tr_\(tableName)_2"
        triggersToDrop.append((name: tr1, schema: "dbo"))
        triggersToDrop.append((name: tr2, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await createAuditTable(name: auditTableName)

        let b1 = "BEGIN SET NOCOUNT ON; INSERT INTO [\(auditTableName)] (table_name, operation, record_id) VALUES ('\(tableName)', 'I1', 1); END"
        let b2 = "BEGIN SET NOCOUNT ON; INSERT INTO [\(auditTableName)] (table_name, operation, record_id) VALUES ('\(tableName)', 'I2', 1); END"

        try await triggerClient.createTrigger(name: tr1, table: tableName, timing: .after, events: [.insert], body: b1)
        try await triggerClient.createTrigger(name: tr2, table: tableName, timing: .after, events: [.insert], body: b2)

        try await insertPerson(into: tableName, id: 1, name: "A", email: "a@example.com")
        let initialAuditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(initialAuditCount, 2)

        try await triggerClient.disableAllTriggers(table: tableName)
        try await insertPerson(into: tableName, id: 2, name: "B", email: "b@example.com")
        let disabledAuditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(disabledAuditCount, 2)

        try await triggerClient.enableAllTriggers(table: tableName)
        try await insertPerson(into: tableName, id: 3, name: "C", email: "c@example.com")
        let enabledAuditCount = try await client.queryScalar("SELECT COUNT(*) FROM [\(auditTableName)]", as: Int.self)
        XCTAssertEqual(enabledAuditCount, 4)
    }
}
