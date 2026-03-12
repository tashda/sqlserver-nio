import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class TestNameTests: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

        // Create connection
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        group = nil
    }

    // MARK: - Tests

    func testExample() async throws {
        // Test implementation here
    }
}