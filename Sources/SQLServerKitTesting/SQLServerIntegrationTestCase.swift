#if canImport(XCTest)
import XCTest
import NIO
import NIOPosix
import SQLServerKit

/// Base class for SQL Server integration tests.
/// Handles boilerplate setUp and tearDown for a live server-backed client.
open class SQLServerIntegrationTestCase: XCTestCase {
    public var group: EventLoopGroup!
    public var client: SQLServerClient!
    open override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        
        // Ensure Docker is started if requested
        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }
        
        TestEnvironmentManager.loadEnvironmentVariables()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do {
            let client = self.client!
            _ = try await withTimeout(10) { try await client.query("SELECT 1").get() }
        } catch { throw error }
    }

    open override func tearDown() async throws {
        try? await client?.shutdownGracefully().get()
        try? await group?.shutdownGracefully()
        client = nil
        group = nil
    }
}
#endif
