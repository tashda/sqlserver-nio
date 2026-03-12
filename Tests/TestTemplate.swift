import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class TestNameTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

        // Create connection
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - Tests

    func testExample() async throws {
        // Test implementation here
    }
}