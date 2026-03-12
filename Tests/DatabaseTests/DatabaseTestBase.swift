import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

class DatabaseTestBase: XCTestCase, @unchecked Sendable {
    var baseClient: SQLServerClient!
    var testDatabase: String!
    var adminClient: SQLServerAdministrationClient!
    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured

        // Ensure Docker is started if requested
        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        do {
            _ = try await withTimeout(10) { try await self.baseClient.query("SELECT 1") }
        } catch {
            throw error
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "dbprops")
        self.adminClient = SQLServerAdministrationClient(client: baseClient)
    }

    override func tearDown() async throws {
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try? await baseClient?.shutdownGracefully()
        testDatabase = nil
    }
}
