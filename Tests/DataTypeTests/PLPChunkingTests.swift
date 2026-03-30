import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerPlpChunkingTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    @available(macOS 12.0, *)
    func testPlpMultiChunkBoundaries() async throws {
        try await withTimeout(60) {
            try await withTemporaryDatabase(client: self.client, prefix: "plp2") { db in
                let table = "plp2_tbl_\(UUID().uuidString.prefix(6))"

                try await withDbClient(for: db) { dbClient in
                    let dbAdminClient = SQLServerAdministrationClient(client: dbClient)

                    // Create table using SQLServerKit APIs with MAX data types
                    let columns = [
                        SQLServerColumnDefinition(name: "nv", definition: .standard(.init(dataType: .nvarchar(length: .max)))),
                        SQLServerColumnDefinition(name: "vb", definition: .standard(.init(dataType: .varbinary(length: .max))))
                    ]
                    try await dbAdminClient.createTable(name: table, columns: columns)

                    // Payload sizes around likely chunk boundaries (64k, 128k, 1MB)
                    let sizes: [Int] = [65_535, 65_536, 131_072, 1_000_000]
                    for s in sizes {
                        // Use a BMP character so NVARCHAR stores 2 bytes per char deterministically
                        let nv = String(repeating: "ユ", count: s/2)
                        let vb = Array((0..<s).map { UInt8($0 & 0xFF) })
                        _ = try await dbAdminClient.insertRow(into: table, values: ["nv": .nString(nv), "vb": .bytes(vb)])
                    }

                    // Query data back using SQLServerKit APIs
                    let rows = try await dbClient.query("SELECT DATALENGTH(nv) AS nvl, DATALENGTH(vb) AS vbl FROM [dbo].[\(table)] ORDER BY vbl").get()
                    XCTAssertEqual(rows.count, sizes.count)
                    for (idx, s) in sizes.enumerated() {
                        XCTAssertEqual(rows[idx].column("nvl")?.int, (s/2) * 2) // bytes for NVARCHAR
                        XCTAssertEqual(rows[idx].column("vbl")?.int, s)
                    }
                }
            }
        }
    }
}
