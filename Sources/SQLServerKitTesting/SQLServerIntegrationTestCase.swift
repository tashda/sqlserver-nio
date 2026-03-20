#if canImport(XCTest)
import XCTest
import SQLServerKit

/// Base class for SQL Server integration tests.
/// Handles boilerplate setUp and tearDown for a live server-backed client.
@available(macOS 12.0, *)
open class SQLServerIntegrationTestCase: XCTestCase {
    public var client: SQLServerClient!

    open override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)

        if envFlagEnabled("USE_DOCKER") {
            _ = try ensureSQLServerTestFixture(requireAdventureWorks: envFlagEnabled("TDS_LOAD_ADVENTUREWORKS"))
        }

        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        do {
            let client = self.client!
            _ = try await withTimeout(10) { try await client.query("SELECT 1") }
        } catch { throw error }
    }

    open override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }
}
#endif
