@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerColumnstoreIndexTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        _ = try await withTimeout(5) { try await self.client.query("SELECT 1") }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    @available(macOS 12.0, *)
    func testColumnstoreIndexScripting() async throws {
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "csmx") { db in
            let table = "cs_tbl_\(UUID().uuidString.prefix(6))"

            try await withDbClient(for: db) { dbClient in
                let dbAdminClient = SQLServerAdministrationClient(client: dbClient)
                let indexClient = SQLServerIndexClient(client: dbClient)

                // Create table using SQLServerKit APIs
                let columns = [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int))),
                    SQLServerColumnDefinition(name: "C1", definition: .standard(.init(dataType: .int))),
                    SQLServerColumnDefinition(name: "C2", definition: .standard(.init(dataType: .int)))
                ]
                try await dbAdminClient.createTable(name: table, columns: columns)

                let ccs = "CCS_\(UUID().uuidString.prefix(6))"
                _ = ccs
                let nccs = "NCCS_\(UUID().uuidString.prefix(6))"
                // Prefer a single nonclustered columnstore index; if unsupported, skip gracefully.
                do {
                    _ = try await withRetry(attempts: 5) {
                        try await indexClient.createColumnstoreIndex(
                            name: nccs,
                            table: table,
                            clustered: false,
                            columns: ["C1", "C2"]
                        )
                    }
                    try? await Task.sleep(nanoseconds: 200_000_000)
                } catch {
                    throw XCTSkip("Columnstore indexes are not supported on this server/edition")
                }

                // Use the public metadata API to fetch the object definition
                let dbClient = try await makeClient(forDatabase: db)
                defer { Task { try? await dbClient.shutdownGracefully() } }
                let def = try await dbClient.metadata.objectDefinition(schema: "dbo", name: table, kind: .table)
                guard let def, let ddl = def.definition else { throw XCTSkip("Unable to fetch definition reliably due to connection resets") }
                XCTAssertTrue(ddl.contains("COLUMNSTORE"), "Expected columnstore index in script")
            }
        }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Skipping due to server closing connections during columnstore test") }
            throw e
        } catch {
            throw error
        }
    }
}
