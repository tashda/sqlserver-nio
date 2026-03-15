import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

/// Tests for large result sets to verify the token parser handles
/// thousands of rows without truncation or iteration limits.
final class LargeResultSetTests: StandardTestBase, @unchecked Sendable {

    // MARK: - Row Count Integrity

    func testQuery10000Rows() async throws {
        let rows = try await client.query("""
            SELECT TOP 10000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        XCTAssertEqual(rows.count, 10000, "Expected exactly 10000 rows, got \(rows.count)")
    }

    func testQuery25000Rows() async throws {
        let rows = try await client.query("""
            SELECT TOP 25000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        XCTAssertEqual(rows.count, 25000, "Expected exactly 25000 rows, got \(rows.count)")
    }

    // MARK: - Data Integrity Across Large Sets

    func testLargeResultSetDataIntegrity() async throws {
        let rowCount = 10000
        let rows = try await client.query("""
            SELECT TOP \(rowCount)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num,
                CONCAT('row-', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS label
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        XCTAssertEqual(rows.count, rowCount)

        // Verify first and last rows have correct data
        let firstNum = rows.first?.column("row_num")?.int
        XCTAssertEqual(firstNum, 1, "First row should be row_num=1")

        let lastName = rows.last?.column("label")?.string
        XCTAssertEqual(lastName, "row-\(rowCount)", "Last row should be row-\(rowCount)")

        // Verify no gaps by checking a sample of rows
        for i in stride(from: 0, to: rowCount, by: 1000) {
            let num = rows[i].column("row_num")?.int
            XCTAssertEqual(num, i + 1, "Row at index \(i) should have row_num=\(i + 1), got \(String(describing: num))")
        }
    }

    // MARK: - Wide Rows (Many Columns)

    func testWideRowsWithManyColumns() async throws {
        // 50 columns × 5000 rows — tests both column count and row count
        let rows = try await client.query("""
            SELECT TOP 5000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
                CAST('A' AS varchar(50)) AS c1, CAST('B' AS varchar(50)) AS c2,
                CAST('C' AS varchar(50)) AS c3, CAST('D' AS varchar(50)) AS c4,
                CAST('E' AS varchar(50)) AS c5, CAST('F' AS varchar(50)) AS c6,
                CAST('G' AS varchar(50)) AS c7, CAST('H' AS varchar(50)) AS c8,
                CAST(1 AS int) AS n1, CAST(2 AS int) AS n2,
                CAST(3 AS int) AS n3, CAST(4 AS int) AS n4,
                CAST(5.5 AS decimal(10,2)) AS d1, CAST(6.6 AS decimal(10,2)) AS d2,
                CAST(GETDATE() AS datetime) AS dt1, CAST(GETDATE() AS datetime) AS dt2,
                CAST(NULL AS varchar(50)) AS null1, CAST(NULL AS int) AS null2,
                CAST(NULL AS datetime) AS null3, CAST(NULL AS decimal(10,2)) AS null4
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        XCTAssertEqual(rows.count, 5000, "Expected 5000 wide rows")
        XCTAssertEqual(rows.first?.columnMetadata.count, 21, "Expected 21 columns")

        // Verify null columns are preserved across large result set
        let lastRow = rows.last!
        XCTAssertNil(lastRow.column("null1")?.string)
        XCTAssertNil(lastRow.column("null2")?.string)
        XCTAssertEqual(lastRow.column("c1")?.string, "A")
    }

    // MARK: - Large Payload Rows

    func testLargePayloadRows() async throws {
        // Each row has a ~1KB payload
        let rows = try await client.query("""
            SELECT TOP 10000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num,
                REPLICATE('X', 1000) AS payload
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        XCTAssertEqual(rows.count, 10000, "Expected 10000 rows with 1KB payloads")

        // Verify payload integrity on a sample
        for i in stride(from: 0, to: 10000, by: 2500) {
            let payload = rows[i].column("payload")?.string
            XCTAssertEqual(payload?.count, 1000, "Row \(i) payload should be 1000 chars")
        }
    }

    // MARK: - Performance

    func testLargeQueryPerformance() async throws {
        let start = Date()

        let rows = try await client.query("""
            SELECT TOP 10000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num,
                CONCAT('data-', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS label
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        let duration = Date().timeIntervalSince(start)

        XCTAssertEqual(rows.count, 10000)
        XCTAssertLessThan(duration, 30.0, "10K rows should complete within 30 seconds, took \(String(format: "%.1f", duration))s")

        logger.info("📊 10000 rows fetched in \(String(format: "%.3f", duration))s (\(Int(Double(rows.count) / duration)) rows/sec)")
    }

    // MARK: - Table-Based Large Result Set

    func testLargeInsertAndSelect() async throws {
        let tableName = generateUniqueTableName(prefix: "large")

        let admin = SQLServerAdministrationClient(client: client)
        try await admin.createTable(name: tableName, columns: [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
        ])

        defer { Task { try? await admin.dropTable(name: tableName) } }

        // Insert 10000 rows using a single batch via CROSS JOIN
        _ = try await client.execute("""
            INSERT INTO \(tableName) (id, value)
            SELECT TOP 10000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
                CONCAT(N'value-', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
            FROM sys.all_columns a
            CROSS JOIN sys.all_columns b
        """)

        // Verify full round-trip
        let rows = try await client.query("SELECT id, value FROM \(tableName) ORDER BY id")

        XCTAssertEqual(rows.count, 10000, "Should retrieve all 10000 inserted rows")
        XCTAssertEqual(rows.first?.column("id")?.int, 1)
        XCTAssertEqual(rows.first?.column("value")?.string, "value-1")
        XCTAssertEqual(rows.last?.column("id")?.int, 10000)
        XCTAssertEqual(rows.last?.column("value")?.string, "value-10000")
    }
}
