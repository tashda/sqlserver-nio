import XCTest
import SQLServerKit
import SQLServerKitTesting

/// Tests for multi-result-set streaming via SQL batch queries.
/// Validates that `SELECT ...; SELECT ...` batches deliver all result sets
/// through the streaming API with correct metadata and row data.
final class MultiResultSetTests: StandardTestBase, @unchecked Sendable {

    // MARK: - Streaming Multi-Result-Set Tests

    func testStreamQueryTwoSelectStatements() async throws {
        let connection = try await client.connection()
        let stream = connection.streamQuery("SELECT 1 AS a; SELECT 2 AS b")

        var metadataEvents: [[SQLServerColumnDescription]] = []
        var rowEvents: [SQLServerRow] = []
        var doneEvents: [SQLServerStreamDone] = []

        for try await event in stream {
            switch event {
            case .metadata(let columns):
                metadataEvents.append(columns)
            case .row(let row):
                rowEvents.append(row)
            case .done(let done):
                doneEvents.append(done)
            case .message:
                break
            }
        }

        XCTAssertEqual(metadataEvents.count, 2, "Expected 2 metadata events for 2 SELECT statements")
        XCTAssertEqual(rowEvents.count, 2, "Expected 2 row events for 2 SELECT statements")
        XCTAssertGreaterThanOrEqual(doneEvents.count, 2, "Expected at least 2 done events")

        // Verify column names differ between result sets
        XCTAssertEqual(metadataEvents[0].first?.name, "a")
        XCTAssertEqual(metadataEvents[1].first?.name, "b")

        // Verify row values
        XCTAssertEqual(rowEvents[0].column("a")?.string, "1")
        XCTAssertEqual(rowEvents[1].column("b")?.string, "2")
    }

    func testStreamQueryThreeSelectStatements() async throws {
        let connection = try await client.connection()
        let stream = connection.streamQuery("SELECT 1 AS x; SELECT 2 AS y; SELECT 3 AS z")

        var metadataCount = 0
        var rowCount = 0

        for try await event in stream {
            switch event {
            case .metadata: metadataCount += 1
            case .row: rowCount += 1
            case .done, .message: break
            }
        }

        XCTAssertEqual(metadataCount, 3, "Expected 3 metadata events for 3 SELECT statements")
        XCTAssertEqual(rowCount, 3, "Expected 3 row events for 3 SELECT statements")
    }

    func testStreamQueryMixedRowCounts() async throws {
        try await withTempTable(columns: [("id", "INT"), ("val", "VARCHAR(50)")]) { t1 in
            try await withTempTable(columns: [("num", "INT")]) { t2 in
                // Insert 5 rows into t1
                for i in 1...5 {
                    _ = try await executeQuery("INSERT INTO \(t1) VALUES (\(i), 'row\(i)')")
                }
                // Insert 3 rows into t2
                for i in 1...3 {
                    _ = try await executeQuery("INSERT INTO \(t2) VALUES (\(i))")
                }

                let connection = try await client.connection()
                let stream = connection.streamQuery("SELECT * FROM \(t1); SELECT * FROM \(t2)")

                var currentResultSetRows = 0
                var resultSetRowCounts: [Int] = []

                for try await event in stream {
                    switch event {
                    case .metadata:
                        if currentResultSetRows > 0 || !resultSetRowCounts.isEmpty {
                            resultSetRowCounts.append(currentResultSetRows)
                        }
                        currentResultSetRows = 0
                    case .row:
                        currentResultSetRows += 1
                    case .done:
                        // Only record when done follows rows (skip empty done)
                        break
                    case .message:
                        break
                    }
                }
                // Append the last result set
                resultSetRowCounts.append(currentResultSetRows)

                XCTAssertEqual(resultSetRowCounts.count, 2, "Expected 2 result sets")
                XCTAssertEqual(resultSetRowCounts[0], 5, "First result set should have 5 rows")
                XCTAssertEqual(resultSetRowCounts[1], 3, "Second result set should have 3 rows")
            }
        }
    }

    func testStreamQueryWithDMLAndSelect() async throws {
        try await withTempTable(columns: [("id", "INT"), ("name", "VARCHAR(50)")]) { tableName in
            let connection = try await client.connection()
            let stream = connection.streamQuery("""
                INSERT INTO \(tableName) VALUES (1, 'test');
                SELECT * FROM \(tableName)
            """)

            var metadataCount = 0
            var rowCount = 0
            var doneEvents: [SQLServerStreamDone] = []

            for try await event in stream {
                switch event {
                case .metadata: metadataCount += 1
                case .row: rowCount += 1
                case .done(let done): doneEvents.append(done)
                case .message: break
                }
            }

            // INSERT produces a done with row count but no metadata
            // SELECT produces metadata + row + done
            XCTAssertGreaterThanOrEqual(metadataCount, 1, "SELECT should produce at least 1 metadata event")
            XCTAssertEqual(rowCount, 1, "SELECT should return the 1 inserted row")
        }
    }

    func testNonStreamingMultiStatement() async throws {
        // Non-streaming query() returns only the first result set
        let rows = try await client.query("SELECT 1 AS a; SELECT 2 AS b")

        // Should get at least the first result set
        XCTAssertFalse(rows.isEmpty, "Should return at least one row")
        XCTAssertEqual(rows.first?.column("a")?.string, "1")
    }

    func testStreamQuerySingleStatement() async throws {
        let connection = try await client.connection()
        let stream = connection.streamQuery("SELECT 42 AS val")

        var metadataCount = 0
        var rowValues: [String] = []

        for try await event in stream {
            switch event {
            case .metadata: metadataCount += 1
            case .row(let row):
                if let v = row.column("val")?.string { rowValues.append(v) }
            case .done, .message: break
            }
        }

        XCTAssertEqual(metadataCount, 1)
        XCTAssertEqual(rowValues, ["42"])
    }

    func testDoneTokenStatusFlags() async throws {
        let connection = try await client.connection()
        let stream = connection.streamQuery("SELECT 1 AS a; SELECT 2 AS b")

        var doneEvents: [SQLServerStreamDone] = []

        for try await event in stream {
            if case .done(let done) = event {
                doneEvents.append(done)
            }
        }

        // Should have at least 2 done events
        XCTAssertGreaterThanOrEqual(doneEvents.count, 2)

        // All done events except the last should have DONE_MORE (0x0001) set
        for i in 0..<(doneEvents.count - 1) {
            let hasMore = (doneEvents[i].status & 0x0001) != 0
            XCTAssertTrue(hasMore, "Done event at index \(i) should have DONE_MORE flag, status=0x\(String(format: "%04X", doneEvents[i].status))")
        }

        // Last done should NOT have DONE_MORE
        let lastDone = doneEvents.last!
        let lastHasMore = (lastDone.status & 0x0001) != 0
        XCTAssertFalse(lastHasMore, "Last done event should not have DONE_MORE flag, status=0x\(String(format: "%04X", lastDone.status))")
    }
}
