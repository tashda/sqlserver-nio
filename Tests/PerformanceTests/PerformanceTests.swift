import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Simple performance tests for SQLServerNIO
/// Tests basic performance characteristics
final class PerformanceTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private let logger = Logger(label: "PerformanceTests")

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
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    // MARK: - Connection Performance Tests

    func testConnectionPerformance() async throws {
        logger.info("🔧 Testing connection performance...")

        let connectionCount = 3
        var connectionTimes: [TimeInterval] = []
        var createdEventLoopGroups: [EventLoopGroup] = []

        for i in 1...connectionCount {
            let startTime = Date()

            var config = makeSQLServerClientConfiguration()
            config.poolConfiguration.connectionIdleTimeout = nil
            config.poolConfiguration.minimumIdleConnections = 0

            let testGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            createdEventLoopGroups.append(testGroup)

            let testClient = try await SQLServerClient.connect(
                configuration: config,
                eventLoopGroupProvider: .shared(testGroup)
            ).get()

            let connectionTime = Date().timeIntervalSince(startTime)
            connectionTimes.append(connectionTime)

            // Execute a simple query to ensure connection is working
            let _ = try await testClient.query("SELECT 1 as connection_test")

            // Properly shutdown the client with timeout and error handling
            try await withTimeout(10.0) {
                try await testClient.shutdownGracefully().get()
            }

            logger.info("   Connection \(i): \(String(format: "%.3f", connectionTime))s")
        }

        // Clean up all created EventLoopGroups
        for group in createdEventLoopGroups {
            try await withTimeout(5.0) {
                try await group.shutdownGracefully()
            }
        }

        let averageConnectionTime = connectionTimes.reduce(0, +) / Double(connectionTimes.count)

        logger.info("📊 Connection Performance Results:")
        logger.info("   Average: \(String(format: "%.3f", averageConnectionTime))s")

        // Performance assertions (adjust based on your environment)
        XCTAssertLessThan(averageConnectionTime, 2.0, "Average connection time should be under 2 seconds")

        logger.info("✅ Connection performance test completed!")
    }

    // MARK: - Query Performance Tests

    func testQueryPerformance() async throws {
        logger.info("🔧 Testing query performance...")

        let queries = [
            "Simple Query": "SELECT 1 as test",
            "System Table Query": "SELECT TOP 5 * FROM sys.objects",
            "Count Query": "SELECT COUNT(*) as count FROM sys.tables"
        ]

        var performanceResults: [String: TimeInterval] = [:]

        for (queryName, query) in queries {
            let startTime = Date()
            let result = try await client.query(query)
            let duration = Date().timeIntervalSince(startTime)
            performanceResults[queryName] = duration

            logger.info("   \(queryName): \(result.count) rows in \(String(format: "%.3f", duration))s")
        }

        // Performance assertions
        // Allow for network latency and connection overhead
        // Based on sqlcmd baseline of ~0.075s, allow reasonable margin
        XCTAssertLessThanOrEqual(performanceResults["Simple Query"] ?? 0, 0.3, "Simple query should be under 300ms")
        XCTAssertLessThanOrEqual(performanceResults["System Table Query"] ?? 0, 1.0, "System table query should complete in reasonable time")

        logger.info("✅ Query performance test completed!")
    }
}