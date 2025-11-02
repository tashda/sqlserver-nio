@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerIndexMatrixTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    private func deep() -> Bool { env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1" }

    @available(macOS 12.0, *)
    func testIndexOptionMatrixScripting() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "imx") { db in
            let table = "ix_tbl_\(UUID().uuidString.prefix(6))"

            try await withDbClient(for: db, using: self.group) { dbClient in
                let dbAdminClient = SQLServerAdministrationClient(client: dbClient)
                let indexClient = SQLServerIndexClient(client: dbClient)

                // Create table using SQLServerKit APIs with regular primary key
                let columns = [
                    SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                    SQLServerColumnDefinition(name: "Name", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
                    SQLServerColumnDefinition(name: "Age", definition: .standard(.init(dataType: .int))),
                    SQLServerColumnDefinition(name: "Email", definition: .standard(.init(dataType: .nvarchar(length: .length(255)))))
                ]
                try await dbAdminClient.createTable(name: table, columns: columns)

                struct IX { let name: String; let cols: [IndexColumn]; let filter: String?; let options: IndexOptions?; let unique: Bool }
                var cases: [IX] = [
                    IX(name: "ix_nonclustered",
                        cols: [
                            IndexColumn(name: "Name", sortDirection: .ascending),
                            IndexColumn(name: "Email", sortDirection: .ascending, isIncluded: true)
                        ],
                        filter: "Name IS NOT NULL",
                        options: IndexOptions(fillFactor: 80, allowRowLocks: false),
                        unique: false),
                    IX(name: "ix_desc",
                        cols: [IndexColumn(name: "Age", sortDirection: .descending)],
                        filter: nil,
                        options: nil,
                        unique: false),
                ]
                if self.deep() {
                    cases.append(IX(name: "ix_with_options",
                        cols: [
                            IndexColumn(name: "Name", sortDirection: .ascending),
                            IndexColumn(name: "Age", sortDirection: .descending),
                            IndexColumn(name: "Email", sortDirection: .ascending, isIncluded: true)
                        ],
                        filter: "Age > 0",
                        options: IndexOptions(padIndex: true, statisticsNoRecompute: true, maxDop: 2),
                        unique: false))
                    // Add a separate unique index test for IGNORE_DUP_KEY
                    cases.append(IX(name: "ix_unique_ignore_dup",
                        cols: [IndexColumn(name: "Name", sortDirection: .ascending)],
                        filter: nil,
                        options: IndexOptions(ignoreDuplicateKey: true),
                        unique: true))
                }

                for spec in cases {
                    let ixName = spec.name + "_" + UUID().uuidString.prefix(6)

                    // Create index using SQLServerKit APIs
                    if spec.unique {
                        try await indexClient.createUniqueIndex(
                            name: ixName,
                            table: table,
                            columns: spec.cols,
                            options: spec.options,
                            filter: spec.filter
                        )
                    } else {
                        try await indexClient.createIndex(
                            name: ixName,
                            table: table,
                            columns: spec.cols,
                            options: spec.options,
                            filter: spec.filter
                        )
                    }

                    guard let def = try await withDbConnection(client: self.client, database: db, operation: { conn in
                        try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                    }), let ddl = def.definition else { XCTFail("No DDL returned"); continue }

                    // Check the scripted DDL contains our index with key features
                    let expectedCreateType = spec.unique ? "CREATE UNIQUE NONCLUSTERED INDEX [\(ixName)]" : "CREATE NONCLUSTERED INDEX [\(ixName)]"
                    XCTAssertTrue(ddl.contains(expectedCreateType))
                    if spec.cols.contains(where: { $0.isIncluded }) { XCTAssertTrue(ddl.contains("INCLUDE")) }
                    if let filter = spec.filter { XCTAssertTrue(ddl.contains("WHERE \(filter)")) }
                    if let options = spec.options {
                        if let fillFactor = options.fillFactor { XCTAssertTrue(ddl.contains("FILLFACTOR = \(fillFactor)")) }
                        if options.padIndex { XCTAssertTrue(ddl.contains("PAD_INDEX = ON")) }
                        if options.ignoreDuplicateKey { XCTAssertTrue(ddl.contains("IGNORE_DUP_KEY = ON")) }
                        if options.statisticsNoRecompute { XCTAssertTrue(ddl.contains("STATISTICS_NORECOMPUTE = ON")) }
                        if let maxDop = options.maxDop { XCTAssertTrue(ddl.contains("MAXDOP = \(maxDop)")) }
                        if !options.allowRowLocks { XCTAssertTrue(ddl.contains("ALLOW_ROW_LOCKS = OFF")) }
                    }
                }

                // Compression option on clustered index (scripting should surface DATA_COMPRESSION)
                let cix = "cix_" + UUID().uuidString.prefix(6)
                try await indexClient.createClusteredIndex(
                    name: cix,
                    table: table,
                    columns: [IndexColumn(name: "Id", sortDirection: .ascending)],
                    options: IndexOptions(dataCompression: .page)
                )

                guard let def2 = try await withDbConnection(client: self.client, database: db, operation: { conn in
                    try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                }), let ddl2 = def2.definition else { XCTFail("No DDL returned"); return }
                XCTAssertTrue(ddl2.contains("DATA_COMPRESSION"), "Scripted index should include DATA_COMPRESSION when present")
            }
        }
    }
}
