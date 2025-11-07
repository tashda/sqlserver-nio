import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Simple TDS Layer Tests - Basic Working Tests
/// Tests fundamental TDS functionality using SQLServerClient against live database
final class SimpleTDSTests: StandardTestBase {

    // MARK: - Basic Connection Test

    func testBasicConnection() async throws {
        logTestStart("Basic TDS Connection Test")

        let result = try await executeQuery("SELECT 1 as test_val, GETDATE() as connection_time", expectedRows: 1)

        let row = result.first!
        XCTAssertNotNil(row.column("test_val"))
        XCTAssertNotNil(row.column("connection_time"))

        logTestSuccess("Basic TDS connection test completed")
    }

    // MARK: - Basic Data Types Test

    func testBasicDataTypes() async throws {
        logTestStart("Basic Data Types Test")

        let result = try await executeQuery("""
            SELECT
                42 as integer_val,
                3.14159 as float_val,
                'Hello World' as string_val,
                CAST('2023-12-25' as date) as date_val,
                CAST(1 as bit) as boolean_val,
                NEWID() as guid_val
        """, expectedRows: 1)

        let row = result.first!

        XCTAssertNotNil(row.column("integer_val"))
        XCTAssertNotNil(row.column("float_val"))
        XCTAssertNotNil(row.column("string_val"))
        XCTAssertNotNil(row.column("date_val"))
        XCTAssertNotNil(row.column("boolean_val"))
        XCTAssertNotNil(row.column("guid_val"))

        logTestSuccess("Basic data types test completed")
    }

    // MARK: - Basic SQL Operations Test

    func testBasicSQLOperations() async throws {
        logTestStart("Basic SQL Operations Test")

        // Use the standardized temp table pattern
        _ = try await withTempTable(columns: [
            ("id", "INT IDENTITY(1,1) PRIMARY KEY"),
            ("name", "VARCHAR(50)"),
            ("value", "INT"),
            ("created_at", "DATETIME DEFAULT GETDATE()")
        ]) { tableName in

            // Insert data
            try await insertIntoTable(tableName, columns: ["name", "value"], values: ["Test 1", "100"])
            try await insertIntoTable(tableName, columns: ["name", "value"], values: ["Test 2", "200"])
            try await insertIntoTable(tableName, columns: ["name", "value"], values: ["Test 3", "300"])

            // Query data
            let selectResult = try await selectAllFromTable(tableName)
            XCTAssertEqual(selectResult.count, 3)

            // Update data
            _ = try await executeQuery("UPDATE \(tableName) SET value = value * 2 WHERE name = 'Test 1'")

            // Verify update
            let updateResult = try await executeQuery("SELECT value FROM \(tableName) WHERE name = 'Test 1'")
            XCTAssertEqual(updateResult.count, 1)

            return tableName
        }

        logTestSuccess("Basic SQL operations test completed")
    }

    // MARK: - Error Handling Test

    func testErrorHandling() async throws {
        logTestStart("Error Handling Test")

        // Test SQL syntax error
        try await expectErrorContaining("syntax") {
            _ = try await executeQuery("SELCT 1") // Intentional syntax error
        }

        // Test invalid object reference
        try await expectErrorContaining("not found") {
            _ = try await executeQuery("SELECT * FROM nonexistent_table_xyz")
        }

        logTestSuccess("Error handling test completed")
    }
}