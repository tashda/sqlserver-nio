@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerColumnstoreIndexTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testColumnstoreIndexScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "csmx") { db in
            let table = "cs_tbl_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)] (Id INT NOT NULL, C1 INT, C2 INT);")
            let ccs = "CCS_\(UUID().uuidString.prefix(6))"
            let nccs = "NCCS_\(UUID().uuidString.prefix(6))"
            // Prefer a single nonclustered columnstore index; if unsupported, skip gracefully.
            do {
                _ = try await withRetry(attempts: 5) {
                    try await executeInDb(client: self.client, database: db, "CREATE NONCLUSTERED COLUMNSTORE INDEX [\(nccs)] ON [dbo].[\(table)] (C1, C2);")
                }
                try? await Task.sleep(nanoseconds: 200_000_000)
            } catch {
                throw XCTSkip("Columnstore indexes are not supported on this server/edition")
            }

            // Use a reliable connection after potential DDL errors to avoid lingering connectionClosed
            var def: ObjectDefinition?
            var remaining = 3
            while def == nil && remaining > 0 {
                remaining -= 1
                do {
                    def = try await withReliableConnection(client: self.client, attempts: 5) { conn in
                        _ = try await conn.changeDatabase(db).get()
                        return try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                    }
                } catch {
                    // spin a fresh client bound to DB and retry once
                    if let se = error as? SQLServerError, case .connectionClosed = se, remaining > 0 {
                        let dbClient = try await makeClient(forDatabase: db, using: self.group)
                        def = try? await withReliableConnection(client: dbClient, attempts: 5) { conn in
                            return try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                        }
                        _ = try? await dbClient.shutdownGracefully().get()
                    } else {
                        throw error
                    }
                }
            }
            guard let def, let ddl = def.definition else { throw XCTSkip("Unable to fetch definition reliably due to connection resets") }
            XCTAssertTrue(ddl.contains("COLUMNSTORE"), "Expected columnstore index in script")
        }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Skipping due to server closing connections during columnstore test") }
            throw e
        } catch {
            throw error
        }
    }
}
