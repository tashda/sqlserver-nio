import XCTest
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit
import SQLServerKitTesting

/// Standard base class for all SQLServer tests
/// Provides consistent setup, teardown, and query execution patterns
/// All tests should inherit from this class unless they specifically test connection lifecycle
open class StandardTestBase: XCTestCase, @unchecked Sendable {

    // MARK: - Properties

    /// Shared SQLServer client for the test
    public var client: SQLServerClient!

    /// Logger for the test
    public let logger = Logger(label: "StandardTestBase")

    // MARK: - Setup and Teardown

    override open func setUp() async throws {
        _ = isLoggingConfigured
        TestEnvironmentManager.loadEnvironmentVariables()

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0

        self.client = try await SQLServerClient.connect(
            configuration: config,
            numberOfThreads: 1
        )

        logger.info("✅ Test setup completed successfully")
    }

    override open func tearDown() async throws {
        if let client = client {
            try await client.shutdownGracefully()
            self.client = nil
        }

        logger.info("✅ Test teardown completed successfully")
    }

    // MARK: - Standard Query Methods

    public func executeQuery(_ sql: String) async throws -> [SQLServerRow] {
        return try await client.query(sql)
    }

    public func executeQuery(_ sql: String, expectedRows: Int) async throws -> [SQLServerRow] {
        let result = try await executeQuery(sql)
        XCTAssertEqual(result.count, expectedRows, "Expected \(expectedRows) rows, but got \(result.count)")
        return result
    }

    public func executeSingleRowQuery(_ sql: String) async throws -> SQLServerRow {
        let result = try await executeQuery(sql, expectedRows: 1)
        return result.first!
    }

    public func executeScalarQuery(_ sql: String) async throws -> String? {
        let row = try await executeSingleRowQuery(sql)
        return row.data.first?.string
    }

    public func executeScalarQuery(_ sql: String, columnName: String) async throws -> String? {
        let row = try await executeSingleRowQuery(sql)
        return row.column(columnName)?.string
    }

    // MARK: - Standard Table Operations

    public func createTempTable(
        _ tableName: String,
        columns: [(String, String)]
    ) async throws {
        let columnDefinitions = columns.map { "\($0.0) \($0.1)" }.joined(separator: ", ")
        let sql = "CREATE TABLE \(tableName) (\(columnDefinitions))"
        _ = try await executeQuery(sql)
        logger.info("✅ Created temporary table: \(tableName)")
    }

    public func dropTableIfExists(_ tableName: String) async throws {
        let sql = "IF OBJECT_ID('\(tableName)') IS NOT NULL DROP TABLE \(tableName)"
        _ = try await executeQuery(sql)
        logger.info("✅ Dropped table: \(tableName)")
    }

    public func insertIntoTable(
        _ tableName: String,
        columns: [String],
        values: [String]
    ) async throws {
        let columnList = columns.joined(separator: ", ")
        let valueList = values.map { "'\($0)'" }.joined(separator: ", ")
        let sql = "INSERT INTO \(tableName) (\(columnList)) VALUES (\(valueList))"
        _ = try await executeQuery(sql)
    }

    public func selectAllFromTable(_ tableName: String) async throws -> [SQLServerRow] {
        return try await executeQuery("SELECT * FROM \(tableName)")
    }

    public func countRowsInTable(_ tableName: String) async throws -> Int {
        let result = try await executeSingleRowQuery("SELECT COUNT(*) as count FROM \(tableName)")
        return Int(result.column("count")?.string ?? "0") ?? 0
    }

    // MARK: - Standard Test Patterns

    public func withTempTable<T>(
        columns: [(String, String)],
        operation: (String) async throws -> T
    ) async throws -> T {
        let tableName = generateUniqueTableName()
        try await createTempTable(tableName, columns: columns)

        do {
            let result = try await operation(tableName)
            try await dropTableIfExists(tableName)
            return result
        } catch {
            try? await dropTableIfExists(tableName)
            throw error
        }
    }

    public func expectError<T: Error>(
        _ expectedErrorType: T.Type,
        in operation: () async throws -> Any
    ) async throws {
        do {
            _ = try await operation()
            XCTFail("Expected error of type \(expectedErrorType), but operation succeeded")
        } catch {
            XCTAssertTrue(error is T, "Expected error of type \(expectedErrorType), but got: \(type(of: error))")
            logger.info("✅ Expected error caught: \(error)")
        }
    }

    public func expectErrorContaining(
        _ expectedText: String,
        in operation: () async throws -> Any
    ) async throws {
        do {
            _ = try await operation()
            XCTFail("Expected error containing '\(expectedText)', but operation succeeded")
        } catch {
            let errorDescription = error.localizedDescription.lowercased()
            let searchText = expectedText.lowercased()
            XCTAssertTrue(errorDescription.contains(searchText),
                          "Expected error containing '\(expectedText)', but got: \(error.localizedDescription)")
            logger.info("✅ Expected error caught: \(error)")
        }
    }

    // MARK: - Utility Methods

    public func generateUniqueTableName(prefix: String = "test") -> String {
        let token = UUID().uuidString.prefix(8)
        return "\(prefix)_\(token)"
    }

    public func logTestStart(_ message: String) {
        logger.info("🚀 ===== \(message) =====")
    }

    public func logTestSuccess(_ message: String) {
        logger.info("✅ \(message)")
    }

    public func logTestError(_ message: String, error: Error) {
        logger.error("❌ \(message): \(error)")
    }
}
