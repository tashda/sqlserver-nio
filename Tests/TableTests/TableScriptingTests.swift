@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableScriptingMatrixTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        // Probe basic connectivity; skip if unstable
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1 as ready").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    private func deep() -> Bool { env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1" }

    @available(macOS 12.0, *)
    func testTableScriptGoldenRecreate() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "tsmx") { db in
            // A small but representative set; expanded when deep mode is enabled
            struct Col { let name: String; let def: String }
            struct Combo { let name: String; let cols: [Col]; let pk: String; let options: String? }
            var cases: [Combo] = [
                Combo(name: "t_id_def_pk", cols: [
                    Col(name: "Id", def: "INT IDENTITY(10,2) NOT NULL"),
                    Col(name: "Name", def: "NVARCHAR(50) COLLATE Latin1_General_CI_AS NOT NULL DEFAULT N'X'"),
                    Col(name: "Flag", def: "BIT NOT NULL DEFAULT 0")
                ], pk: "CONSTRAINT [PK_t] PRIMARY KEY CLUSTERED ([Id])", options: nil),
                Combo(name: "t_sparse_guid", cols: [
                    Col(name: "K", def: "UNIQUEIDENTIFIER ROWGUIDCOL NOT NULL DEFAULT NEWID()"),
                    Col(name: "SparseCol", def: "NVARCHAR(100) SPARSE NULL"),
                    Col(name: "C", def: "AS (LEN([SparseCol])) PERSISTED")
                ], pk: "CONSTRAINT [PK_t] PRIMARY KEY NONCLUSTERED ([K])", options: nil),
                // LOB column to exercise TEXTIMAGE_ON scripting
                Combo(name: "t_lob_textimage", cols: [
                    Col(name: "Id", def: "INT NOT NULL"),
                    Col(name: "Note", def: "NVARCHAR(MAX) NULL")
                ], pk: "CONSTRAINT [PK_t] PRIMARY KEY CLUSTERED ([Id])", options: nil),
            ]
            if self.deep() {
                cases.append(contentsOf: [
                    Combo(name: "t_temporal", cols: [
                        Col(name: "Id", def: "INT NOT NULL"),
                        Col(name: "ValidFrom", def: "DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL"),
                        Col(name: "ValidTo", def: "DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL"),
                    ], pk: "CONSTRAINT [PK_t] PRIMARY KEY CLUSTERED ([Id])", options: "WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[t_temporal_History]))"),
                ])
            }

            for combo in cases {
                let table = combo.name + "_" + UUID().uuidString.prefix(6)
                var create = "CREATE TABLE [dbo].[\(table)] (\n"
                var columnLines = combo.cols.map { "    [\($0.name)] \($0.def)" }
                // If this is a temporal combo, inject the PERIOD clause to form valid DDL
                let hasTemporal = combo.options?.localizedCaseInsensitiveContains("SYSTEM_VERSIONING = ON") == true
                if hasTemporal,
                   combo.cols.contains(where: { $0.name == "ValidFrom" }),
                   combo.cols.contains(where: { $0.name == "ValidTo" }) {
                    columnLines.append("    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])")
                }
                create += columnLines.joined(separator: ",\n")
                create += ",\n    \(combo.pk)\n)"
                if let options = combo.options { create += " \(options)" }
                create += ";"
                try? await withRetry(attempts: 3, operation: {
                    _ = try await executeInDb(client: self.client, database: db, create)
                })

                // Fetch scripted definition
                let def = try await withReliableConnection(client: self.client, operation: { conn in
                    _ = try await conn.changeDatabase(db).get()
                    return try await conn.fetchObjectDefinition(schema: "dbo", name: String(table), kind: .table).get()
                })
                guard let def, let ddl = def.definition else { XCTFail("No DDL returned for \(table)"); continue }

                // Golden re-exec: disable temporal if needed, drop, then recreate from DDL
                if ddl.localizedCaseInsensitiveContains("SYSTEM_VERSIONING = ON") {
                    // Turn off system versioning to allow DROP
                    _ = try? await executeInDb(client: self.client, database: db, "ALTER TABLE [dbo].[\(table)] SET (SYSTEM_VERSIONING = OFF);")
                    // If an explicit history table is referenced, drop it for a clean recreate
                    if let range = ddl.range(of: #"HISTORY_TABLE\s*=\s*\[[^\]]+\]\.\[[^\]]+\]"#, options: [.regularExpression, .caseInsensitive]) {
                        let clause = String(ddl[range])
                        // Extract schema and name
                        if let m = clause.range(of: #"\[([^\]]+)\]\.\[([^\]]+)\]"#, options: .regularExpression) {
                            let pair = String(clause[m])
                            let parts = pair.replacingOccurrences(of: "[", with: "").replacingOccurrences(of: "]", with: "").split(separator: ".", maxSplits: 1).map(String.init)
                            if parts.count == 2 {
                                let hs = parts[0]
                                let ht = parts[1]
                                _ = try? await executeInDb(client: self.client, database: db, "IF OBJECT_ID(N'\(hs).\(ht)', 'U') IS NOT NULL DROP TABLE [\(hs)].[\(ht)];")
                            }
                        }
                    }
                }
                _ = try await executeInDb(client: self.client, database: db, "DROP TABLE [dbo].[\(table)]")
                do { _ = try await self.client.executeScript(ddl) } catch {
                    XCTFail("Failed to recreate \(table) from scripted DDL: \(error)\nDDL=\n\(ddl)")
                    continue
                }
                // Sanity: verify columns count matches original using enhanced metadata APIs
                let metadataClient = try await self.client.withConnection { connection in
                    SQLServerMetadataClient(connection: connection)
                }
                let columns = try await metadataClient.listColumns(schema: "dbo", table: table).get()
                XCTAssertEqual(columns.count, combo.cols.count, "Column count mismatch after re-exec for \(table)")

                // Additional assertions for special cases
                if combo.name.contains("lob_textimage") {
                    XCTAssertTrue(ddl.contains("TEXTIMAGE_ON"), "LOB table script should include TEXTIMAGE_ON")
                }
            }
        }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Skipping due to server closing connections during table scripting matrix") }
            throw e
        }
    }
}
