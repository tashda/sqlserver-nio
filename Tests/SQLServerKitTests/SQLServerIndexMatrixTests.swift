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
        loadEnvFileIfPresent()
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
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)] (Id INT PRIMARY KEY, Name NVARCHAR(50), Age INT, Email NVARCHAR(255));")

            struct IX { let name: String; let cols: [String]; let include: [String]; let filter: String?; let options: [String] }
            var cases: [IX] = [
                IX(name: "ix_nonclustered", cols: ["Name ASC"], include: ["Email"], filter: "Name IS NOT NULL", options: ["FILLFACTOR = 80", "ALLOW_ROW_LOCKS = OFF"]),
                IX(name: "ix_desc", cols: ["Age DESC"], include: [], filter: nil, options: []),
            ]
            if self.deep() {
                cases.append(IX(name: "ix_with_options", cols: ["Name ASC", "Age DESC"], include: ["Email"], filter: "Age > 0", options: ["PAD_INDEX = ON", "IGNORE_DUP_KEY = ON", "STATISTICS_NORECOMPUTE = ON", "MAXDOP = 2"]))
            }

            for spec in cases {
                let ixName = spec.name + "_" + UUID().uuidString.prefix(6)
                var create = "CREATE NONCLUSTERED INDEX [\(ixName)] ON [dbo].[\(table)] (\(spec.cols.joined(separator: ", ")))"
                if !spec.include.isEmpty { create += " INCLUDE (\(spec.include.map { "[\($0)]" }.joined(separator: ", ")))" }
                if let filter = spec.filter { create += " WHERE \(filter)" }
                if !spec.options.isEmpty { create += " WITH (\(spec.options.joined(separator: ", ")))" }
                _ = try await executeInDb(client: self.client, database: db, create)

                guard let def = try await withDbConnection(client: self.client, database: db, { conn in
                    try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                }), let ddl = def.definition else { XCTFail("No DDL returned"); continue }

                // Check the scripted DDL contains our index with key features
                XCTAssertTrue(ddl.contains("CREATE NONCLUSTERED INDEX [\(ixName)]"))
                if !spec.include.isEmpty { XCTAssertTrue(ddl.contains("INCLUDE")) }
                if let filter = spec.filter { XCTAssertTrue(ddl.contains("WHERE \(filter)")) }
                for opt in spec.options { XCTAssertTrue(ddl.contains(opt.replacingOccurrences(of: "]", with: "]]"))) }
            }

            // Compression option on clustered index (scripting should surface DATA_COMPRESSION)
            let cix = "cix_" + UUID().uuidString.prefix(6)
            _ = try await executeInDb(client: self.client, database: db, "CREATE CLUSTERED INDEX [\(cix)] ON [dbo].[\(table)] (Id) WITH (DATA_COMPRESSION = PAGE);")
            guard let def2 = try await withDbConnection(client: self.client, database: db, { conn in
                try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
            }), let ddl2 = def2.definition else { XCTFail("No DDL returned"); return }
            XCTAssertTrue(ddl2.contains("DATA_COMPRESSION"), "Scripted index should include DATA_COMPRESSION when present")
        }
    }
}
