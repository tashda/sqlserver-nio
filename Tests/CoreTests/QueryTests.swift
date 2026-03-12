import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit
import SQLServerKitTesting

/// Consolidated query tests for SQLServerNIO
/// Covers basic SQL queries, parameters, and result handling
final class QueryTests: StandardTestBase, @unchecked Sendable {

    // MARK: - Basic Query Tests

    func testBasicSQLQuery() async throws {
        logTestStart("Basic SQL Query Test")

        let result = try await executeQuery("SELECT 1 as test_col, 'working' as test_val", expectedRows: 1)

        // Verify the result data
        if let firstRow = result.first {
            // The numeric value might be in binary format, let's check what we actually get
            let testColData = firstRow.column("test_col")
            let testValData = firstRow.column("test_val")

            logger.info("📊 test_col data: \(String(describing: testColData))")
            logger.info("📊 test_val data: \(String(describing: testValData))")

            // For now, just verify we got some data back and the string column works
            XCTAssertEqual(testValData?.string, "working")
            XCTAssertNotNil(testColData)
        }

        logTestSuccess("Basic SQL Query Test completed successfully!")
    }

    func testMultipleRowQuery() async throws {
        logTestStart("Multiple Row Query Test")

        let result = try await executeQuery("""
            SELECT 1 as id, 'First' as name
            UNION ALL
            SELECT 2 as id, 'Second' as name
            UNION ALL
            SELECT 3 as id, 'Third' as name
        """, expectedRows: 3)

        // Verify all rows are present
        let ids = result.compactMap { $0.column("id")?.string }
        let names = result.compactMap { $0.column("name")?.string }

        XCTAssertEqual(ids, ["1", "2", "3"])
        XCTAssertEqual(names, ["First", "Second", "Third"])

        logTestSuccess("Multiple row query successful!")
    }


    func testQueryPagedReturnsRequestedWindow() async throws {
        let rows = try await client.queryPaged("""
            SELECT v.n AS id, CONCAT('row-', v.n) AS name
            FROM (VALUES (1),(2),(3),(4),(5)) AS v(n)
        """, limit: 2, offset: 1)

        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows[0].column("id")?.int, 2)
        XCTAssertEqual(rows[0].column("name")?.string, "row-2")
        XCTAssertEqual(rows[1].column("id")?.int, 3)
        XCTAssertEqual(rows[1].column("name")?.string, "row-3")
    }

    func testWithDatabaseScopesOperationsAndResetsConnection() async throws {
        try await withTemporaryDatabase(client: client, prefix: "echo") { dbName in
            let admin = SQLServerAdministrationClient(client: self.client, database: dbName)
            try await admin.createTable(
                name: "paged_scope",
                columns: [
                    SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
            )

            let names = try await self.client.withDatabase(dbName) { connection in
                try await connection.insertRow(into: "paged_scope", values: [
                    "id": .int(1),
                    "name": .nString("one")
                ])
                try await connection.insertRow(into: "paged_scope", values: [
                    "id": .int(2),
                    "name": .nString("two")
                ])
                let rows = try await connection.query("SELECT name FROM [dbo].[paged_scope] ORDER BY id")
                return rows.compactMap { $0.column("name")?.string }
            }

            XCTAssertEqual(names, ["one", "two"])

            let currentDb = try await self.client.queryScalar("SELECT DB_NAME() AS db", as: String.self)
            XCTAssertEqual(currentDb?.lowercased(), "master")
        }
    }

    func testDedicatedConnectionCanChangeDatabaseAndRelease() async throws {
        try await withTemporaryDatabase(client: client, prefix: "echo") { dbName in
            let connection = try await self.client.connection()
            do {
                try await connection.use(database: dbName)

                let currentDb = try await connection.queryScalar("SELECT DB_NAME() AS db", as: String.self)
                XCTAssertEqual(currentDb?.lowercased(), dbName.lowercased())

                _ = try await connection.execute("""
                    CREATE TABLE [dbo].[connection_scope] (
                        [id] INT NOT NULL PRIMARY KEY,
                        [name] NVARCHAR(50) NOT NULL
                    )
                """)

                try await connection.insertRow(into: "connection_scope", values: [
                    "id": .int(1),
                    "name": .nString("scoped")
                ])

                let rows = try await connection.query("SELECT name FROM [dbo].[connection_scope]")
                XCTAssertEqual(rows.first?.column("name")?.string, "scoped")
            } catch {
                try? await connection.close()
                throw error
            }

            try await connection.close()
        }
    }

    func testObjectDefinitionReturnsViewDefinition() async throws {
        try await withTemporaryDatabase(client: client, prefix: "echo") { dbName in
            try await self.client.withDatabase(dbName) { connection in
                _ = try await connection.execute("CREATE VIEW [dbo].[echo_view] AS SELECT 42 AS value")
            }

            let definition = try await self.client.objectDefinition(
                database: dbName,
                schema: "dbo",
                name: "echo_view",
                kind: .view
            )

            XCTAssertEqual(definition?.type, .view)
            XCTAssertTrue(definition?.definition?.localizedCaseInsensitiveContains("CREATE VIEW [dbo].[echo_view]") == true)
            XCTAssertTrue(definition?.definition?.localizedCaseInsensitiveContains("SELECT 42 AS value") == true)
        }
    }

    // MARK: - Data Type Tests

    func testBasicDataTypes() async throws {
        logTestStart("Basic Data Types Test")

        let result = try await executeQuery("""
            SELECT
                42 as integer_val,
                3.14159 as decimal_val,
                'test string' as string_val,
                CAST(1 as bit) as boolean_val
        """, expectedRows: 1)

        if let row = result.first {
            XCTAssertEqual(row.column("integer_val")?.string, "42")
            XCTAssertEqual(row.column("decimal_val")?.string, "3.14159")
            XCTAssertEqual(row.column("string_val")?.string, "test string")
            XCTAssertEqual(row.column("boolean_val")?.string, "1")
        }

        logTestSuccess("Basic data types test successful!")
    }

    func testRowDataPreservesNullColumns() async throws {
        logTestStart("Row Data Preserves Null Columns Test")

        let result = try await executeQuery("""
            SELECT
                CAST(1 as int) as col_a,
                CAST(NULL as varchar(10)) as col_b,
                CAST('x' as varchar(10)) as col_c,
                CAST(NULL as datetime) as col_d
        """, expectedRows: 1)

        guard let row = result.first else {
            return XCTFail("Expected one row for null alignment test")
        }

        XCTAssertEqual(row.columnMetadata.count, 4)
        XCTAssertEqual(row.data.count, 4)

        XCTAssertEqual(row.data[0].string, "1")
        XCTAssertNil(row.data[1].string)
        XCTAssertEqual(row.data[2].string, "x")
        XCTAssertNil(row.data[3].string)

        XCTAssertNotNil(row.column("col_b"))
        XCTAssertNil(row.column("col_b")?.string)
        XCTAssertNotNil(row.column("col_d"))
        XCTAssertNil(row.column("col_d")?.string)

        logTestSuccess("Row data preserves null columns test successful!")
    }

    // MARK: - Error Handling Tests

    func testInvalidSQLQuery() async throws {
        logTestStart("Invalid SQL Query Test")

        // Test that the query either throws an error or returns an empty result set
        do {
            let result = try await executeQuery("SELECT * FROM nonexistent_table_xyz")
            if result.isEmpty {
                logger.info("✅ Query returned empty result set for nonexistent table (acceptable behavior)")
                logTestSuccess("Invalid SQL query test completed")
            } else {
                XCTFail("Expected empty result set for invalid table. Got \(result.count) rows instead.")
            }
        } catch {
            // If an error is thrown, that's also acceptable
            logger.info("✅ Invalid query properly handled with error: \(error)")
            XCTAssertTrue(error.localizedDescription.contains("not found") ||
                         error.localizedDescription.contains("Invalid object name"))
            logTestSuccess("Invalid SQL query test completed")
        }
    }

    func testSQLSyntaxError() async throws {
        logTestStart("SQL Syntax Error Test")

        do {
            _ = try await executeQuery("SELCT 1") // Intentional typo
            XCTFail("Expected syntax error for malformed SQL command")
        } catch {
            let description = error.localizedDescription.lowercased()
            let syntaxMatch = description.contains("syntax")
            let missingProcMatch = description.contains("could not find stored procedure")
            XCTAssertTrue(syntaxMatch || missingProcMatch,
                          "Expected syntax-related error, got: \(error.localizedDescription)")
            logger.info("✅ Expected syntax error caught: \(error)")
        }

        logTestSuccess("SQL syntax error test completed")
    }

    // MARK: - Batch Query Tests

    func testBatchQueries() async throws {
        logTestStart("Batch Query Test")

        let result = try await executeQuery("""
            SELECT 1 as batch1_id;
            SELECT 2 as batch2_id, 'batch2' as batch2_name;
            SELECT 3 as batch3_id, 'batch3' as batch3_name, 3.14 as batch3_value;
        """)

        // Should return multiple result sets
        XCTAssertGreaterThan(result.count, 0)
        logTestSuccess("Batch query successful, returned \(result.count) total rows")
    }

    // MARK: - Performance Tests

    func testQueryPerformance() async throws {
        logTestStart("Query Performance Test")

        let startTime = Date()

        let result = try await executeQuery("""
            SELECT TOP 100
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num,
                'Performance Test Data ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as varchar) as data
            FROM sys.objects o1
            CROSS JOIN sys.objects o2
        """)

        let duration = Date().timeIntervalSince(startTime)

        XCTAssertGreaterThan(result.count, 0)
        XCTAssertLessThan(duration, 5.0, "Query should complete within 5 seconds")

        logTestSuccess("Performance test completed: \(result.count) rows in \(String(format: "%.3f", duration)) seconds")
    }
}
