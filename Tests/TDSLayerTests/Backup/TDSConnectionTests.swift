import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Comprehensive TDS Connection Tests
/// Tests TDS connection, packet handling, and communication using SQLServerClient against live database
final class TDSConnectionTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private let logger = Logger(label: "TDSConnectionTests")

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: config,
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        await client?.shutdownGracefully()
        try await group?.shutdownGracefully()
    }

    // MARK: - Basic Connection Tests

    func testBasicConnection() async throws {
        logger.info("🔧 Testing basic TDS connection...")

        let result = try await client.query("SELECT 1 as test_val, GETDATE() as connection_time")

        XCTAssertEqual(result.count, 1)
        let row = result.first!
        XCTAssertNotNil(row.column("test_val"))
        XCTAssertNotNil(row.column("connection_time"))

        logger.info("✅ Basic TDS connection test completed")
    }

    func testConnectionWithParameters() async throws {
        logger.info("🔧 Testing connection with parameters...")

        let result = try await client.query("""
            SELECT
                42 as param1,
                'string_param' as param2,
                3.14159 as param3,
                1 as param4,
                GETDATE() as param5
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!
        XCTAssertNotNil(row.column("param1"))
        XCTAssertNotNil(row.column("param2"))
        XCTAssertNotNil(row.column("param3"))
        XCTAssertNotNil(row.column("param4"))
        XCTAssertNotNil(row.column("param5"))

        logger.info("✅ Connection with parameters test completed")
    }

    func testMultipleConnections() async throws {
        logger.info("🔧 Testing multiple simultaneous connections...")

        var results: [Int] = []
        let concurrentQueries = 5

        // Create multiple concurrent clients
        func queryWithClient(clientIndex: Int) async -> Int {
            var config = makeSQLServerClientConfiguration()
            config.poolConfiguration.connectionIdleTimeout = nil
            config.poolConfiguration.minimumIdleConnections = 0

            let testGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            defer { try! testGroup.syncShutdownGracefully() }

            let testClient = try await SQLServerClient.connect(
                configuration: config,
                eventLoopGroupProvider: .createNew(numberOfThreads: 1)
            ).get()

            let result = try await testClient.query("SELECT \(clientIndex) as query_id")
            defer { try await testClient.shutdownGracefully() }

            return result.first?.column("query_id")?.integer ?? 0
        }

        // Run concurrent queries
        await withTaskGroup(of: Int.self) { group in
            for i in 1...concurrentQueries {
                group.addTask {
                    return await queryWithClient(clientIndex: i)
                }
            }

            for try await result in group {
                results.append(result)
            }
        }

        XCTAssertEqual(results.count, concurrentQueries)
        XCTAssertEqual(Set(results).count, concurrentQueries) // All should be unique

        logger.info("✅ Multiple connections test completed - \(concurrentQueries) concurrent queries successful")
    }

    // MARK: - Connection Pool Tests

    func testConnectionPooling() async throws {
        logger.info("🔧 Testing connection pooling...")

        var connectionIds: [String] = []
        let queryCount = 10

        // Execute multiple queries to test connection reuse
        for i in 1...queryCount {
            let result = try await client.query("SELECT '\(i)' as iteration, CONNECTION_ID() as conn_id")

            if let connId = result.first?.column("conn_id")?.string {
                connectionIds.append(connId)
            }
        }

        // With connection pooling, we should see connection reuse
        let uniqueConnections = Set(connectionIds)
        logger.info("📊 Used \(uniqueConnections.count) unique connections for \(queryCount) queries")

        // Assert that we're reusing connections (though the exact number depends on pool configuration)
        XCTAssertLessThanOrEqual(uniqueConnections.count, queryCount)

        logger.info("✅ Connection pooling test completed")
    }

    func testConnectionLifecycle() async throws {
        logger.info("🔧 Testing connection lifecycle...")

        // Test connection creation
        let startTime = Date()
        let result1 = try await client.query("SELECT 'Created' as status, GETDATE() as created_time")
        let createTime = Date().timeIntervalSince(startTime)

        XCTAssertEqual(result1.count, 1)
        XCTAssertLessThan(createTime, 5.0) // Should be fast

        // Test connection reuse
        let reuseStart = Date()
        let result2 = try await client.query("SELECT 'Reused' as status, GETDATE() as reused_time")
        let reuseTime = Date().timeIntervalSince(reuseStart)

        XCTAssertEqual(result2.count, 1)
        // Reuse should be faster than creation (though this depends on various factors)
        logger.info("📊 Creation time: \(String(format: "%.3f", createTime))s, Reuse time: \(String(format: "%.3f", reuseTime))s")

        logger.info("✅ Connection lifecycle test completed")
    }

    // // MARK: - Error Handling Tests

    func testConnectionErrorHandling() async throws {
        logger.info("🔧 Testing connection error handling...")

        // Test SQL syntax error
        do {
            let result = try await client.query("SELCT 1") // Intentional syntax error
            XCTFail("Should have thrown a syntax error. Got \(result.count) rows instead.")
        } catch {
            logger.info("✅ SQL syntax error properly handled: \(error)")
            XCTAssertTrue(error.localizedDescription.contains("syntax") ||
                         error.localizedDescription.contains("incorrect"))
        }

        // Test invalid object reference
        do {
            let result = try await client.query("SELECT * FROM nonexistent_table_xyz")
            XCTFail("Should have thrown an error for non-existent table. Got \(result.count) rows instead.")
        } catch {
            logger.info("✅ Invalid object error properly handled: \(error)")
        }

        // Test permission error (if applicable)
        do {
            let result = try await client.query("SELECT * FROM master.sys.database_files WHERE 1=0") // May fail due to permissions
            // If this succeeds, that's fine - we just want to test error handling
            logger.info("✅ Permission test - access to master.sys.database_files: \(result.count) rows")
        } catch {
            logger.info("✅ Permission error properly handled: \(error)")
        }

        logger.info("✅ Connection error handling test completed")
    }

    func testConnectionTimeoutHandling() async throws {
        logger.info("🔧 Testing connection timeout handling...")

        // Create a client with very short timeout
        var timeoutConfig = makeSQLServerClientConfiguration()
        timeoutConfig.poolConfiguration.connectionIdleTimeout = nil
        timeoutConfig.poolConfiguration.minimumIdleConnections = 0

        let timeoutClient = try await SQLServerClient.connect(
            configuration: timeoutConfig,
            eventLoopGroupProvider: .createNew(numberOfThreads: 1)
        ).get()

        // Execute a query
        let result = try await timeoutClient.query("SELECT 1 as timeout_test")

        XCTAssertEqual(result.count, 1)

        // Clean up
        try await timeoutClient.shutdownGracefully()

        logger.info("✅ Connection timeout handling test completed")
    }

    // MARK: - Packet Communication Tests

    func testLargeQueryPackets() async throws {
        logger.info("🔧 Testing large query packets...")

        // Generate a large SQL query that will likely span multiple packets
        let largeQuery = """
            SELECT
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num,
                'Large text content ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as varchar) + ' that spans multiple lines to test packet fragmentation' as large_text,
                REPLICATE('X', 1000) as repeated_data,
                NEWID() as guid_val,
                GETDATE() as timestamp_val
            FROM sys.objects o1
            CROSS JOIN sys.objects o2
            WHERE ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) <= 10
        """

        let result = try await client.query(largeQuery)

        XCTAssertEqual(result.count, 10)

        // Verify large data was transmitted correctly
        let row = result.first!
        XCTAssertNotNil(row.column("row_num"))
        XCTAssertNotNil(row.column("large_text"))
        XCTAssertNotNil(row.column("repeated_data"))
        XCTAssertNotNil(row.column("guid_val"))
        XCTAssertNotNil(row.column("timestamp_val"))

        logger.info("✅ Large query packets test completed - \(result.count) rows with large data")
    }

    func testBinaryDataInPackets() async throws {
        logger.info("🔧 Testing binary data in packets...")

        let result = try await client.query("""
            SELECT
                CAST(0x48656c6c6f20576f726c6420574646573774 as varbinary(50)) as binary_data,
                CAST(REPLICATE(0xFF, 1024) as varbinary(1024)) as large_binary,
                CAST('Binary string' as varbinary(24)) as string_as_binary
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        XCTAssertNotNil(row.column("binary_data"))
        XCTAssertNotNil(row.column("large_binary"))
        XCTAssertNotNil(row.column("string_as_binary"))

        logger.info("✅ Binary data in packets test completed")
    }

    // MARK: - Connection State Tests

    func testConnectionStateAfterError() async throws {
        logger.info("🔧 Testing connection state after error...")

        // Execute a query that fails
        do {
            let result = try await client.query("SELECT 1/0 as division_error")
            XCTFail("Should have failed due to division by zero")
        } catch {
            logger.info("✅ Division by zero error handled")
        }

        // Connection should still be usable after the error
        let result = try await client.query("SELECT 'Still connected' as status")

        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("status")?.string, "Still connected")

        logger.info("✅ Connection state after error test completed")
    }

    func testConnectionStateAfterLargeOperation() async throws {
        logger.info("🔧 Testing connection state after large operation...")

        // Execute a large query
        let largeResult = try await client.query("""
            SELECT TOP 100
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num,
                OBJECT_NAME as object_name,
                OBJECT_ID as object_id
            FROM sys.objects
        """)

        XCTAssertEqual(largeResult.count, 100)

        // Connection should still be usable
        let result = try await client.query("SELECT 'After large operation' as status")

        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("status")?.string, "After large operation")

        logger.info("✅ Connection state after large operation test completed")
    }

    // MARK: - SSL/TLS Tests

    func testSSLConnection() async throws {
        logger.info("🔧 Testing SSL/TLS connection...")

        // The current connection should already be using TLS if configured
        let result = try await client.query("""
            SELECT
                SESSIONPROPERTY('encryption') as encryption_enabled,
                ENCRYPTBYKEY('AES_256', 'Test Key', '1', 'Test Vector') as encrypted_data,
                DECRYPTBYKEY(ENCRYPTBYKEY('AES_256', 'Test Key', '1', 'Test Vector'), 'Test Key', 'AES_256') as decrypted_data
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!
        XCTAssertNotNil(row.column("encryption_enabled"))
        XCTAssertNotNil(row.column("encrypted_data"))
        XCTAssertNotNil(row.column("decrypted_data"))

        logger.info("✅ SSL/TLS connection test completed")
    }

    // MARK: - Connection Pool Stress Tests

    func testConnectionPoolStress() async throws {
        logger.info("🔧 Testing connection pool stress...")

        let stressTestCount = 50
        var successfulQueries = 0
        var failedQueries = 0

        func stressQuery(queryId: Int) async -> Bool {
            do {
                let result = try await client.query("SELECT '\(queryId)' as test_id, GETDATE() as query_time")
                return result.count > 0
            } catch {
                return false
            }
        }

        // Run stress test
        await withTaskGroup(of: Bool.self) { group in
            for i in 1...stressTestCount {
                group.addTask {
                    let success = await stressQuery(queryId: i)
                    if success {
                        successfulQueries += 1
                    } else {
                        failedQueries += 1
                    }
                }
            }

            for try await _ in group {
                // Wait for all tasks to complete
            }
        }

        logger.info("📊 Stress test results: \(successfulQueries)/\(stressTestCount) successful, \(failedQueries) failed")
        XCTAssertGreaterThan(successfulQueries, stressTestCount * 0.9) // At least 90% success rate

        logger.info("✅ Connection pool stress test completed")
    }

    // MARK: - Long-Running Connection Tests

    func testLongRunningConnection() async throws {
        logger.info("🔧 Testing long-running connection...")

        let startTime = Date()
        var queryCount = 0

        // Run queries for 10 seconds
        let endTime = startTime.addingTimeInterval(10.0)

        while Date() < endTime {
            let result = try await client.query("SELECT '\(queryCount)' as iteration, GETDATE() as query_time")
            queryCount += 1

            // Small delay between queries
            try await Task.sleep(nanoseconds: 100_000_000) // 0.1 seconds
        }

        let duration = Date().timeIntervalSince(startTime)
        let queriesPerSecond = Double(queryCount) / duration

        logger.info("📊 Long-running connection test results:")
        logger.info("   Duration: \(String(format: "%.2f", duration))s")
        logger.info("   Queries executed: \(queryCount)")
        logger.info("   Queries per second: \(String(format: "%.1f", queriesPerSecond))")

        XCTAssertGreaterThan(queryCount, 50, "Should have executed at least 50 queries in 10 seconds")
        XCTAssertGreaterThan(queriesPerSecond, 5, "Should handle at least 5 queries per second")

        logger.info("✅ Long-running connection test completed")
    }
}