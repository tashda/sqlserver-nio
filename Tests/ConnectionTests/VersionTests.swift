@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerVersionTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    func testServerVersionViaClientAPI() async throws {
        let version = try await client.serverVersion()
        // Simple validation: version should start with digits and contain dots
        XCTAssertTrue(version.range(of: #"^\d+\.\d+(\.\d+)*"#, options: .regularExpression) != nil,
                     "Server version should be in format X.Y[.Z], got: \(version)")
    }
}
