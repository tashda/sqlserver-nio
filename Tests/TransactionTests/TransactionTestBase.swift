import XCTest
import SQLServerKit
import SQLServerKitTesting

class TransactionTestBase: XCTestCase, @unchecked Sendable {
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    var testDatabase: String!
    private var snapshotIsolationChecked = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        _ = try await withTimeout(10) { try await self.baseClient.query("SELECT 1") }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "tx")
        self.client = try await makeClient(forDatabase: testDatabase, using: nil)
    }

    override func tearDown() async throws {
        if client !== baseClient { try? await client?.shutdownGracefully() }
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try? await baseClient?.shutdownGracefully()
        testDatabase = nil
        client = nil
        baseClient = nil
    }

    func ensureSnapshotIsolationEnabled() async throws {
        guard !snapshotIsolationChecked else { return }
        snapshotIsolationChecked = true
        let database = try await client.currentDatabaseName() ?? ""
        let stateRows = try await client.query("""
            SELECT snapshot_isolation_state
            FROM sys.databases
            WHERE name = N'\(database.replacingOccurrences(of: "'", with: "''"))'
            """)
        let state = stateRows.first?.column("snapshot_isolation_state")?.int ?? 0
        if state != 1 {
            _ = try await client.admin.setSnapshotIsolation(database: database, enabled: true)
        }
    }
}
