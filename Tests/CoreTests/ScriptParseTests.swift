import XCTest
import SQLServerKit
import SQLServerKitTesting

/// `SQLServerScriptClient.parse`: SSMS's Parse, checking syntax without running anything.
final class ScriptParseTests: StandardTestBase, @unchecked Sendable {

    func testAValidScriptParses() async throws {
        // Names are not resolved: a table that doesn't exist is not a parse issue.
        let issue = try await client.scripts.parse("""
            SELECT name FROM sys.databases
            GO
            UPDATE dbo.table_that_does_not_exist SET a = 1 WHERE b = 2
            """)
        XCTAssertNil(issue)
    }

    func testAnIssueReportsItsLineInTheWholeScript() async throws {
        let issue = try await client.scripts.parse("""
            SELECT 1
            GO
            SELECT 2
            SELEC 3 FROM
            """)
        let found = try XCTUnwrap(issue)
        XCTAssertEqual(found.line, 4)
        XCTAssertFalse(found.message.isEmpty)
    }

    func testNothingRunsAndTheConnectionIsNotLeftParseOnly() async throws {
        let table = "parse_only_probe_\(UInt32.random(in: 1...999_999))"
        _ = try await client.scripts.parse("CREATE TABLE dbo.\(table) (a int)")
        _ = try await client.scripts.parse("SELEC broken")
        // The table was only parsed, never created; and the pool's connection runs queries again.
        let rows = try await client.query("SELECT OBJECT_ID(N'dbo.\(table)') AS id, 1 AS one")
        XCTAssertEqual(rows.count, 1)
        XCTAssertNil(rows.first?.column("id")?.int)
        XCTAssertEqual(rows.first?.column("one")?.int, 1)
    }
}
