import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Standard base class for all SQLServer tests
/// Provides consistent setup, teardown, and query execution patterns
/// All tests should inherit from this class unless they specifically test connection lifecycle
open class StandardTestBase: XCTestCase {

    // MARK: - Properties

    /// Shared event loop group for the test
    public var group: EventLoopGroup!

    /// Shared SQLServer client for the test
    public var client: SQLServerClient!

    /// Logger for the test
    public let logger = Logger(label: "StandardTestBase")

    // MARK: - Setup and Teardown

    override open func setUp() async throws {
        // Load environment variables
        TestEnvironmentManager.loadEnvironmentVariables()

        // Create configuration with no connection pooling to avoid cleanup issues
        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0

        // Create event loop group
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        // Create client using shared event loop group (this is the key!)
        self.client = try await SQLServerClient.connect(
            configuration: config,
            eventLoopGroupProvider: .shared(group)
        ).get()

        logger.info("✅ Test setup completed successfully")
    }

    override open func tearDown() async throws {
        // Shutdown client first
        if let client = client {
            _ = client.shutdownGracefully()
            self.client = nil
        }

        // Then shutdown the event loop group
        if let group = group {
            // Use async shutdown to avoid Swift 6 warnings
            try await group.shutdownGracefully()
            self.group = nil
        }

        logger.info("✅ Test teardown completed successfully")
    }

    // MARK: - Standard Query Methods

    /// Execute a simple SQL query with the shared client
    /// This is the standard way to execute queries in tests
    public func executeQuery(_ sql: String) async throws -> [TDSRow] {
        return try await client.query(sql)
    }

    /// Execute a query and verify it returns the expected number of rows
    public func executeQuery(_ sql: String, expectedRows: Int) async throws -> [TDSRow] {
        let result = try await executeQuery(sql)
        XCTAssertEqual(result.count, expectedRows, "Expected \(expectedRows) rows, but got \(result.count)")
        return result
    }

    /// Execute a query and verify it returns exactly one row
    public func executeSingleRowQuery(_ sql: String) async throws -> TDSRow {
        let result = try await executeQuery(sql, expectedRows: 1)
        return result.first!
    }

    /// Execute a query and return the first row's column value as string
    public func executeScalarQuery(_ sql: String) async throws -> String? {
        let row = try await executeSingleRowQuery(sql)
        return row.data.first?.string
    }

    /// Execute a query and return the first row's column value by name
    public func executeScalarQuery(_ sql: String, columnName: String) async throws -> String? {
        let row = try await executeSingleRowQuery(sql)
        return row.column(columnName)?.string
    }

    // MARK: - Standard Table Operations

    /// Create a temporary table with the given columns
    public func createTempTable(
        _ tableName: String,
        columns: [(String, String)] // (name, type)
    ) async throws {
        let columnDefinitions = columns.map { "\($0.0) \($0.1)" }.joined(separator: ", ")
        let sql = "CREATE TABLE \(tableName) (\(columnDefinitions))"
        _ = try await executeQuery(sql)
        logger.info("✅ Created temporary table: \(tableName)")
    }

    /// Drop a table if it exists
    public func dropTableIfExists(_ tableName: String) async throws {
        let sql = "IF OBJECT_ID('\(tableName)') IS NOT NULL DROP TABLE \(tableName)"
        _ = try await executeQuery(sql)
        logger.info("✅ Dropped table: \(tableName)")
    }

    /// Insert data into a table
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

    /// Select all data from a table
    public func selectAllFromTable(_ tableName: String) async throws -> [TDSRow] {
        return try await executeQuery("SELECT * FROM \(tableName)")
    }

    /// Get count of rows in a table
    public func countRowsInTable(_ tableName: String) async throws -> Int {
        let result = try await executeSingleRowQuery("SELECT COUNT(*) as count FROM \(tableName)")
        return Int(result.column("count")?.string ?? "0") ?? 0
    }

    // MARK: - Standard Test Patterns

    /// Standard test pattern: create table, perform operations, verify results, cleanup
    public func withTempTable<T>(
        columns: [(String, String)],
        operation: (String) async throws -> T
    ) async throws -> T {
        let tableName = generateUniqueTableName()

        // Create table
        try await createTempTable(tableName, columns: columns)

        do {
            // Perform operation
            let result = try await operation(tableName)

            // Cleanup
            try await dropTableIfExists(tableName)

            return result
        } catch {
            // Ensure cleanup even if operation fails
            try? await dropTableIfExists(tableName)
            throw error
        }
    }

    /// Test that a query throws an expected error
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

    /// Test that a query throws an error with specific text in the description
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

    /// Generate a unique table name for testing
    public func generateUniqueTableName(prefix: String = "test") -> String {
        let token = UUID().uuidString.prefix(8)
        return "\(prefix)_\(token)"
    }

    /// Log test start with custom message
    public func logTestStart(_ message: String) {
        logger.info("🚀 ===== \(message) =====")
    }

    /// Log test completion
    public func logTestSuccess(_ message: String) {
        logger.info("✅ \(message)")
    }

    /// Log test failure with error
    public func logTestError(_ message: String, error: Error) {
        logger.error("❌ \(message): \(error)")
    }
}
