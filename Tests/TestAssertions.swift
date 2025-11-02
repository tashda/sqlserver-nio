import XCTest
import Foundation
import SQLServerTDS

// MARK: - Test Assertions

public func assertResultCount(_ result: [TDSRow], expectedCount: Int, file: StaticString = #file, line: UInt = #line) {
    XCTAssertEqual(result.count, expectedCount, file: file, line: line)
}

public func assertColumnExists(_ result: [TDSRow], columnName: String, file: StaticString = #file, line: UInt = #line) {
    guard let firstRow = result.first else {
        XCTFail("No rows found in result to check columns", file: file, line: line)
        return
    }

    // Get column names from the column metadata
    let columnNames = firstRow.columnMetadata.colData.map { $0.colName }
    XCTAssertTrue(columnNames.contains(columnName), "Expected column '\(columnName)' not found in result. Available columns: \(columnNames)", file: file, line: line)
}

public func assertValue(_ result: [TDSRow], column: String, expectedValue: String, file: StaticString = #file, line: UInt = #line) {
    guard let row = result.first else {
        XCTFail("No rows found in result", file: file, line: line)
        return
    }

    guard let value = row.column(column)?.string else {
        XCTFail("Column '\(column)' not found or cannot be converted to String", file: file, line: line)
        return
    }
    XCTAssertEqual(value, expectedValue, file: file, line: line)
}

public func assertValue(_ result: [TDSRow], column: String, expectedValue: Int, file: StaticString = #file, line: UInt = #line) {
    guard let row = result.first else {
        XCTFail("No rows found in result", file: file, line: line)
        return
    }

    guard let value = row.column(column)?.int else {
        XCTFail("Column '\(column)' not found or cannot be converted to Int", file: file, line: line)
        return
    }
    XCTAssertEqual(value, expectedValue, file: file, line: line)
}

public func assertValue(_ result: [TDSRow], column: String, expectedValue: Bool, file: StaticString = #file, line: UInt = #line) {
    guard let row = result.first else {
        XCTFail("No rows found in result", file: file, line: line)
        return
    }

    guard let value = row.column(column)?.bool else {
        XCTFail("Column '\(column)' not found or cannot be converted to Bool", file: file, line: line)
        return
    }
    XCTAssertEqual(value, expectedValue, file: file, line: line)
}