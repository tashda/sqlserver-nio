import XCTest
import SQLServerKit
@_exported import SQLServerKitTesting

/// The server in `variable` for an XCTest test. Without the variable the test is skipped with a
/// message that names it; with `SQLSERVER_TEST_REQUIRED=1` it fails instead.
@discardableResult
public func requireSQLServerTestServer(
    _ variable: String = TestServer.defaultVariable,
    file: StaticString = #filePath,
    line: UInt = #line
) throws -> TestServer {
    switch TestServer.availability(of: variable) {
    case .available(let server):
        return server
    case .fail(let message):
        throw TestServer.Unavailable(message)
    case .skip(let message):
        throw XCTSkip(message, file: file, line: line)
    }
}

/// Base class for SQL Server integration tests.
/// Handles boilerplate setUp and tearDown for a live server-backed client.
@available(macOS 12.0, *)
open class SQLServerIntegrationTestCase: XCTestCase {
    public var client: SQLServerClient!

    open override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        try requireSQLServerTestServer()
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        let client = self.client!
        _ = try await withTimeout(10) { try await client.query("SELECT 1") }
    }

    open override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }
}
