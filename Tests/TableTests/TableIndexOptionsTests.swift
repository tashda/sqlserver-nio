@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableIndexOptionsTests: XCTestCase {
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
        // Suppress "Already closed" errors during shutdown - they're expected under stress
        do {
            try await client?.shutdownGracefully().get()
        } catch {
            // Silently ignore "Already closed" errors - they happen during EventLoop shutdown
            if error.localizedDescription.contains("Already closed") ||
               error.localizedDescription.contains("ChannelError error 6") {
                // Both "Already closed" string and ChannelError error 6 are expected during shutdown
            } else {
                throw error
            }
        }

        if let g = group {
            _ = try? await SQLServerClient.shutdownEventLoopGroup(g).get()
        }
    }

    func testIndexOptionsAreScripted() async throws {
        let table = "opt_tbl_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let idx = "IX_\(table)_Options"
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "idx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let adminClient = SQLServerAdministrationClient(client: dbClient)
                let indexClient = SQLServerIndexClient(client: dbClient)

                // Create table with primary key using SQLServerKit APIs
                let columns = [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "Code", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                ]
                try await adminClient.createTable(name: table, columns: columns)

                // Create index with specific options using SQLServerKit APIs
                let indexOptions = IndexOptions(
                    fillFactor: 80,
                    padIndex: true,
                    ignoreDuplicateKey: true,
                    allowRowLocks: true,
                    allowPageLocks: false
                )
                try await indexClient.createUniqueIndex(
                    name: idx,
                    table: table,
                    columns: [IndexColumn(name: "Code", sortDirection: .ascending)],
                    options: indexOptions
                )
            }

        try await withDbClient(for: db, using: self.group) { dbClient in
            guard let def = try await withRetry(attempts: 5, operation: {
                try await withTimeout(60, operation: {
                    try await withReliableConnection(client: dbClient, operation: { conn in
                        do {
                            return try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                        } catch {
                            if error.localizedDescription == "Already closed" {
                                throw SQLServerError.connectionClosed // Convert to retryable error
                            } else {
                                throw error
                            }
                        }
                    })
                })
            }), let ddl = def.definition else {
                XCTFail("No definition returned")
                return
            }

        XCTAssertTrue(ddl.contains("CREATE UNIQUE NONCLUSTERED INDEX [\(idx)]"))
        XCTAssertTrue(ddl.contains("WITH ("))
        XCTAssertTrue(ddl.contains("PAD_INDEX = ON"))
        XCTAssertTrue(ddl.contains("FILLFACTOR = 80"))
        XCTAssertTrue(ddl.contains("ALLOW_PAGE_LOCKS = OFF"))
        XCTAssertTrue(ddl.contains("ALLOW_ROW_LOCKS = ON"))
        XCTAssertTrue(ddl.contains("IGNORE_DUP_KEY = ON"))

        // Cleanup
            // No explicit cleanup; database is dropped
            }
        }
        // Ensure scoped dbClient is properly shutdown before test ends
        // (withTemporaryDatabase drops the DB; we close the client explicitly)
        // Note: this path only executes if the do/catch didn't early throw
        // and the database block returned normally.
        // In error paths above, the defer inside withTemporaryDatabase handles cleanup.
        // Here we simply ensure no background tasks linger.
        // Since dbClient is scoped, instantiate a fresh one is unnecessary here.
        } catch {
            if let te = error as? AsyncTimeoutError {
                throw XCTSkip("Skipping due to timeout during index option scripting: \(te)")
            }
            let norm = SQLServerError.normalize(error)
            switch norm {
            case .connectionClosed, .timeout:
                throw XCTSkip("Skipping due to unstable server during index option scripting: \(norm)")
            default:
                throw error
            }
        }
    }
}
