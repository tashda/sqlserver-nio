@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerVersionTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        group = nil
    }

    func testServerVersionViaClientAPI() async throws {
        let version = try await client.serverVersion()
        // Simple validation: version should start with digits and contain dots
        XCTAssertTrue(version.range(of: #"^\d+\.\d+(\.\d+)*"#, options: .regularExpression) != nil,
                     "Server version should be in format X.Y[.Z], got: \(version)")
    }
}