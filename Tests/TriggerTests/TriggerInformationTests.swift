import XCTest
import SQLServerKit
import SQLServerKitTesting

final class TriggerInformationTests: TriggerTestBase, @unchecked Sendable {
    func testGetTriggerInfo() async throws {
        let tableName = "tr_in_t_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_info"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert, .update], body: "BEGIN SET NOCOUNT ON; END")

        let info = try await triggerClient.getTriggerInfo(name: triggerName, table: tableName)
        XCTAssertNotNil(info)
        XCTAssertEqual(info?.name, triggerName)
        XCTAssertEqual(info?.timing, "AFTER")
        XCTAssertTrue(info?.events.contains("INSERT") == true)
        XCTAssertTrue(info?.events.contains("UPDATE") == true)
    }

    func testListTableTriggers() async throws {
        let tableName = "tr_ls_t_\(UUID().uuidString.prefix(8))"
        let tr1 = "tr_\(tableName)_1"
        let tr2 = "tr_\(tableName)_2"
        triggersToDrop.append((name: tr1, schema: "dbo"))
        triggersToDrop.append((name: tr2, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await triggerClient.createTrigger(name: tr1, table: tableName, timing: .after, events: [.insert], body: "BEGIN SET NOCOUNT ON; END")
        try await triggerClient.createTrigger(name: tr2, table: tableName, timing: .after, events: [.update], body: "BEGIN SET NOCOUNT ON; END")

        let triggers = try await triggerClient.listTableTriggers(table: tableName)
        XCTAssertEqual(triggers.count, 2)
        let names = triggers.map { $0.name }
        XCTAssertTrue(names.contains(tr1))
        XCTAssertTrue(names.contains(tr2))
    }

    func testGetTriggerDefinition() async throws {
        let tableName = "tr_df_t_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_def"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        let body = "BEGIN SET NOCOUNT ON; -- Test comment\n PRINT 'Hi'; END"
        try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert], body: body)

        let def = try await triggerClient.getTriggerDefinition(name: triggerName, table: tableName)
        XCTAssertNotNil(def)
        XCTAssertTrue(def?.contains("Test comment") == true)
    }
}
