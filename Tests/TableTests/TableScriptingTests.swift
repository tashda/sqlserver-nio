@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import NIO
import Logging
import Foundation

final class SQLServerTableScriptingMatrixTests: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        // Probe basic connectivity; skip if unstable
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1 as ready").get() } } catch { throw error }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testTableScriptGoldenRecreate() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "tsmx") { db in
            try await withDbClient(for: db, using: self.group) { dbClient in
                let dbAdminClient = SQLServerAdministrationClient(client: dbClient)
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
                cases.append(contentsOf: [
                    Combo(name: "t_temporal", cols: [
                        Col(name: "Id", def: "INT NOT NULL"),
                        Col(name: "ValidFrom", def: "DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL"),
                        Col(name: "ValidTo", def: "DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL"),
                    ], pk: "CONSTRAINT [PK_t] PRIMARY KEY CLUSTERED ([Id])", options: "WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[t_temporal_History]))"),
                ])

                for combo in cases {
                    let table = combo.name + "_" + UUID().uuidString.prefix(6)
                    if combo.name == "t_temporal" {
                        let create = {
                            var statement = "CREATE TABLE [dbo].[\(table)] (\n"
                            var columnLines = combo.cols.map { "    [\($0.name)] \($0.def)" }
                            columnLines.append("    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])")
                            statement += columnLines.joined(separator: ",\n")
                            statement += ",\n    \(combo.pk)\n)"
                            if let options = combo.options { statement += " \(options)" }
                            statement += ";"
                            return statement
                        }()
                        try await withRetry(attempts: 3) {
                            try await executeInDb(client: self.client, database: db, create)
                        }
                    } else {
                        let columns = combo.name == "t_id_def_pk" ? [
                            SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (seed: 10, increment: 2)))),
                            SQLServerColumnDefinition(name: "Name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), defaultValue: "N'X'", collation: "Latin1_General_CI_AS"))),
                            SQLServerColumnDefinition(name: "Flag", definition: .standard(.init(dataType: .bit, defaultValue: "0")))
                        ] : combo.name == "t_sparse_guid" ? [
                            SQLServerColumnDefinition(name: "K", definition: .standard(.init(dataType: .uniqueidentifier, isPrimaryKey: true, defaultValue: "NEWID()", isRowGuidCol: true))),
                            SQLServerColumnDefinition(name: "SparseCol", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true, isSparse: true))),
                            SQLServerColumnDefinition(name: "C", definition: .computed(expression: "LEN([SparseCol])", persisted: true))
                        ] : [
                            SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                            SQLServerColumnDefinition(name: "Note", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: true)))
                        ]
                        try await dbAdminClient.createTable(name: String(table), columns: columns)
                    }

                    let def = try await dbClient.metadata.objectDefinition(schema: "dbo", name: String(table), kind: .table)
                    guard let def, let ddl = def.definition else { XCTFail("No DDL returned for \(table)"); continue }

                    // Golden re-exec: disable temporal if needed, drop, then recreate from DDL
                    if ddl.localizedCaseInsensitiveContains("SYSTEM_VERSIONING = ON") {
                        _ = try? await executeInDb(client: self.client, database: db, "ALTER TABLE [dbo].[\(table)] SET (SYSTEM_VERSIONING = OFF);")
                        if let range = ddl.range(of: #"HISTORY_TABLE\s*=\s*\[[^\]]+\]\.\[[^\]]+\]"#, options: [.regularExpression, .caseInsensitive]) {
                            let clause = String(ddl[range])
                            if let m = clause.range(of: #"\[([^\]]+)\]\.\[([^\]]+)\]"#, options: .regularExpression) {
                                let pair = String(clause[m])
                                let parts = pair.replacingOccurrences(of: "[", with: "").replacingOccurrences(of: "]", with: "").split(separator: ".", maxSplits: 1).map(String.init)
                                if parts.count == 2 {
                                    let hs = parts[0]
                                    let ht = parts[1]
                                    try? await dbAdminClient.dropTable(name: ht, schema: hs, database: db)
                                }
                            }
                        }
                    }
                    try await dbAdminClient.dropTable(name: String(table), schema: "dbo", database: db)
                    do {
                        _ = try await dbClient.executeScript(ddl)
                    } catch {
                        XCTFail("Failed to recreate \(table) from scripted DDL: \(error)\nDDL=\n\(ddl)")
                        continue
                    }
                    let columns = try await dbClient.metadata.listColumns(schema: "dbo", table: String(table))
                    XCTAssertEqual(columns.count, combo.cols.count, "Column count mismatch after re-exec for \(table)")

                    if combo.name.contains("lob_textimage") {
                        XCTAssertTrue(ddl.contains("TEXTIMAGE_ON"), "LOB table script should include TEXTIMAGE_ON")
                    }
                }
            }
        }
    }
}
