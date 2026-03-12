import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

class TransactionTestBase: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    var testDatabase: String!
    private var snapshotIsolationChecked = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        
        // Ensure Docker is started if requested
        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }
        
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do {
            _ = try await withTimeout(10) { try await self.baseClient.query("SELECT 1").get() }
        } catch {
            throw error
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "tx")
        self.client = try await makeClient(forDatabase: testDatabase, using: group)
    }

    override func tearDown() async throws {
        if client !== baseClient { try? await client?.shutdownGracefully().get() }
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        _ = try await baseClient?.shutdownGracefully().get()
        try? await group?.shutdownGracefully()
        testDatabase = nil
        client = nil
        baseClient = nil
        group = nil
    }

    func ensureSnapshotIsolationEnabled() async throws {
        guard !snapshotIsolationChecked else { return }
        snapshotIsolationChecked = true
        let databaseRows = try await self.client.query("SELECT DB_NAME() AS db").get()
        let database = databaseRows.first?.column("db")?.string ?? ""
        try await self.client.withConnection { connection in
            let stateRows = try await connection.query("""
            SELECT snapshot_isolation_state
            FROM sys.databases
            WHERE name = N'\(database.replacingOccurrences(of: "'", with: "''"))'
            """)
            let state = stateRows.first?.column("snapshot_isolation_state")?.int ?? 0
            if state != 1 {
                try await connection.setSnapshotIsolation(database: database, enabled: true)
            }
        }
    }
}
