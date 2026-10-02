import XCTest
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

final class TestNameTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        try requireSQLServerTestServer()

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