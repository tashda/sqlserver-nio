import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class TriggerErrorTests: TriggerTestBase, @unchecked Sendable {
    func testCreateDuplicateTrigger() async throws {
        let tableName = "tr_dq_t_\(UUID().uuidString.prefix(8))"
        let triggerName = "tr_\(tableName)_dup"
        triggersToDrop.append((name: triggerName, schema: "dbo"))

        try await createTestTable(name: tableName)
        try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.insert], body: "BEGIN SET NOCOUNT ON; END")

        do {
            try await triggerClient.createTrigger(name: triggerName, table: tableName, timing: .after, events: [.update], body: "BEGIN SET NOCOUNT ON; END")
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentTrigger() async throws {
        do {
            try await triggerClient.dropTrigger(name: "tr_not_here")
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateTriggerWithNoEvents() async throws {
        let tableName = "tr_ne_t_\(UUID().uuidString.prefix(8))"
        try await createTestTable(name: tableName)

        do {
            try await triggerClient.createTrigger(name: "tr_no_ev", table: tableName, timing: .after, events: [], body: "BEGIN SET NOCOUNT ON; END")
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateTriggerOnNonExistentTable() async throws {
        do {
            try await triggerClient.createTrigger(name: "tr_bad_tbl", table: "no_table", timing: .after, events: [.insert], body: "BEGIN SET NOCOUNT ON; END")
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
