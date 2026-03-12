@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerTemporalPartitionedTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        // Probe connectivity once; mark for skip if unstable
        do {
            _ = try await withTimeout(5) { try await self.client.query("SELECT 1 as ready") }
        } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // Temporal table scripting (no partitioning)
    func testTemporalTableScripting() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "ttmp") { db in
            let table = "tmp_temporal_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
            let hist = "\(table)_History"
            try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.createSystemVersionedTable(name: String(table), historyTableName: String(hist), schema: "dbo", database: db)
            }

            // Fetch definition using a DB-scoped connection
            let def = try await withDbConnection(client: self.client, database: db) { conn in
                try await conn.objectDefinition(schema: "dbo", name: table, kind: .table)
            }
            guard let def, let ddl = def.definition else { XCTFail("No definition returned"); return }
            XCTAssertTrue(ddl.contains("PERIOD FOR SYSTEM_TIME"))
            XCTAssertTrue(ddl.contains("SYSTEM_VERSIONING = ON"))
            XCTAssertTrue(ddl.contains("HISTORY_TABLE = [dbo].[\(hist)]"))

            try? await withDbConnection(client: self.client, database: db) { conn in
                try await conn.setSystemVersioning(table: String(table), enabled: false, schema: "dbo", database: db)
                try await conn.dropTable(name: String(table), schema: "dbo", database: db)
                try await conn.dropTable(name: String(hist), schema: "dbo", database: db)
            }
        }
    }

    // Partitioned table scripting (no temporal)
    func testPartitionedTableScripting() async throws {
        do {
            try await self.client.withConnection { conn in
                    let pf = "pfInt_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                    let ps = "psInt_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                    _ = try await conn.query("SELECT 1")
                    try await conn.createPartitionFunction(name: String(pf), dataType: .int, values: ["100", "1000"])
                    try await conn.createPartitionScheme(name: String(ps), functionName: String(pf))

                    let table = "tmp_part_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
                    try await conn.createPartitionedTable(
                        name: String(table),
                        columns: [
                            SQLServerColumnDefinition(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                            SQLServerColumnDefinition(name: "Code", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
                        ],
                        partitionScheme: String(ps),
                        partitionColumn: "Id"
                    )

                    guard let def = try await withTimeout(10, operation: { try await conn.objectDefinition(schema: "dbo", name: table, kind: .table) }), let ddl = def.definition else {
                        XCTFail("No definition returned")
                        return
                    }
                    XCTAssertTrue(ddl.contains("ON [\(ps)]([Id])"))
                    try? await conn.dropTable(name: String(table))
                    try? await conn.dropPartitionScheme(name: String(ps))
                    try? await conn.dropPartitionFunction(name: String(pf))
            }
        } catch {
            let norm = SQLServerError.normalize(error)
            switch norm {
            case .connectionClosed, .timeout:
                XCTFail("Partitioned table scripting failed due to connectivity: \(norm)")
                return
            default:
                throw error
            }
        }
    }
}
