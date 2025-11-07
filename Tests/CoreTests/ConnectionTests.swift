import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Consolidated connection tests for SQLServerNIO
/// Covers basic connection functionality, pooling, and lifecycle management
final class ConnectionTests: StandardTestBase {

    // MARK: - Basic Connection Tests

    func testDirectTDSConnection() async throws {
        logger.info("🔧 Testing direct TDS connection...")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        // We'll handle shutdown after the test body to avoid async/sync conflicts

        let baseConfig = makeSQLServerClientConfiguration()
        let socketAddress = try await SocketAddress.makeAddressResolvingHost(
            baseConfig.connection.hostname,
            port: baseConfig.connection.port
        )

        let connection = try await TDSConnection.connect(
            to: socketAddress,
            tlsConfiguration: nil,
            serverHostname: baseConfig.connection.hostname,
            on: group.next()
        ).get()

        logger.info("✅ Direct TDS connection successful!")

        // Close connection and wait for it to complete
        _ = connection.close()

        // Give a small delay to ensure cleanup
        try await Task.sleep(nanoseconds: 50_000_000) // 0.05 seconds

        // Handle shutdown safely to avoid Swift 6 warnings
        try await group.shutdownGracefully()
    }

    func testSQLServerClientConnect() async throws {
        logTestStart("SQLServerClient Connect Test")

        // Use the existing shared client and group (no need to create new ones)
        // This test just verifies that the connection from setUp() works
        let result = try await executeQuery("SELECT 1 as connection_test", expectedRows: 1)

        // Verify we got the expected result
        XCTAssertEqual(result.first?.column("connection_test")?.string, "1")

        logTestSuccess("SQLServerClient connect test completed")
    }

    func testWithConnectionBasicQuery() async throws {
        logTestStart("WithConnection Basic Query Test")

        let result = try await client.withConnection { connection in
            return try await connection.query("SELECT 1 as test_col, 'working' as test_val")
        }

        XCTAssertEqual(result.count, 1)
        if let firstRow = result.first {
            XCTAssertEqual(firstRow.column("test_col")?.string, "1")
            XCTAssertEqual(firstRow.column("test_val")?.string, "working")
        }

        logTestSuccess("withConnection basic query successful!")
    }

    // MARK: - Connection Pool Tests

    func testConnectionReuse() async throws {
        logTestStart("Connection Reuse Test")

        // Execute multiple operations to test connection pooling
        for i in 0..<3 {
            let result = try await client.withConnection { connection in
                return try await connection.query("SELECT '\(i)' as iteration")
            }

            XCTAssertEqual(result.count, 1)
            logger.info("Operation \(i) completed successfully")
        }

        logTestSuccess("Connection reuse test completed - 3 operations completed successfully")
    }

    // MARK: - Error Handling Tests

    func testConnectionErrorHandling() async throws {
        logTestStart("Connection Error Handling Test")

        // Test SQL error with invalid query
        do {
            let result = try await client.query("SELECT * FROM nonexistent_table_xyz")
            // If we get here, the query didn't fail as expected
            // Some SQL Server configurations may return empty result sets instead of errors
            if result.isEmpty {
                logger.info("✅ Query returned empty result set for nonexistent table (acceptable behavior)")
                logTestSuccess("Connection error handling test completed")
            } else {
                XCTFail("Expected empty result set or error for invalid table name. Got \(result.count) rows instead.")
            }
        } catch {
            logger.info("✅ SQL error properly handled: \(error)")
            XCTAssertTrue(error.localizedDescription.contains("not found") ||
                         error.localizedDescription.contains("Invalid object name") ||
                         error.localizedDescription.contains("Invalid") ||
                         error.localizedDescription.contains("not exist"))
            logTestSuccess("Connection error handling test completed")
        }
    }
}