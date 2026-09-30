import Foundation
@testable import SQLServerKit
import Testing

@Suite struct ScriptBatchTests {
    @Test func splitsOnGoLines() {
        let batches = SQLServerScript.batches(in: "CREATE TABLE t (a int)\nGO\nINSERT t VALUES (1)\ngo 3\nSELECT 1\n")
        #expect(batches.map(\.sql) == ["CREATE TABLE t (a int)", "INSERT t VALUES (1)", "SELECT 1\n"])
        #expect(batches.map(\.repeatCount) == [1, 3, 1])
        #expect(batches.map(\.line) == [1, 3, 5])
    }

    @Test func goInsideStringsCommentsAndIdentifiersIsNotASeparator() {
        let script = """
        SELECT 'line one
        GO
        still the string'
        /* a comment
        GO
        */
        SELECT [a
        GO] FROM t
        GO
        SELECT 2 -- trailing GO is fine
        GO -- with a comment
        """
        let batches = SQLServerScript.batches(in: script)
        #expect(batches.count == 2)
        #expect(batches[0].sql.hasSuffix("GO] FROM t"))
        #expect(batches[1].sql == "SELECT 2 -- trailing GO is fine")
    }

    @Test func escapedQuotesAndNestedCommentsKeepTheirState() {
        let script = "SELECT 'it''s'\n/* outer /* inner */ still\nGO\n*/\nGO\nSELECT 3"
        #expect(SQLServerScript.batches(in: script).count == 2)
    }

    @Test func windowsLineEndingsAndEmptyBatches() {
        let batches = SQLServerScript.batches(in: "SELECT 1\r\nGO\r\n\r\nGO\r\nSELECT 2\r\n")
        #expect(batches.map(\.sql) == ["SELECT 1", "SELECT 2\n"])
    }

    @Test func separatorVariants() {
        #expect(SQLServerScript.separatorCount("GO") == 1)
        #expect(SQLServerScript.separatorCount("  go  ") == 1)
        #expect(SQLServerScript.separatorCount("GO 5") == 5)
        #expect(SQLServerScript.separatorCount("GO -- done") == 1)
        #expect(SQLServerScript.separatorCount("GOTO label") == nil)
        #expect(SQLServerScript.separatorCount("GO x") == nil)
        #expect(SQLServerScript.separatorCount("SELECT 1 GO") == nil)
    }
}
