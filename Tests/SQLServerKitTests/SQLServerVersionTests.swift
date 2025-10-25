@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerVersionTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testServerVersionViaClientAPI() async throws {
        let version = try await client.serverVersion()
        let regex = NSRegularExpression(sqlServerVersionPattern)
        XCTAssertTrue(regex.matches(version), "Server version should match expected pattern, got: \(version)")
    }
}

