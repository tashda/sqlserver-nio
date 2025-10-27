@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerPlpChunkingTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testPlpMultiChunkBoundaries() async throws {
        try await withTimeout(60) {
            try await withTemporaryDatabase(client: self.client, prefix: "plp2") { db in
            let table = "plp2_tbl_\(UUID().uuidString.prefix(6))"
            _ = try await executeInDb(client: self.client, database: db, "CREATE TABLE [dbo].[\(table)] (nv NVARCHAR(MAX), vb VARBINARY(MAX));")
            // Payload sizes around likely chunk boundaries (64k, 128k, 1MB)
            let sizes: [Int] = [65_535, 65_536, 131_072, 1_000_000]
            for s in sizes {
                // Use a BMP character so NVARCHAR stores 2 bytes per char deterministically
                let nv = String(repeating: "ユ", count: s/2)
                let vb = Array((0..<s).map { UInt8($0 & 0xFF) })
                let nvLit = SQLServerLiteralValue.nString(nv).sqlLiteral()
                let vbLit = SQLServerLiteralValue.bytes(vb).sqlLiteral()
                _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [dbo].[\(table)] (nv, vb) VALUES (\(nvLit), \(vbLit));")
            }
            let rows = try await queryInDb(client: self.client, database: db, "SELECT DATALENGTH(nv) AS nvl, DATALENGTH(vb) AS vbl FROM [dbo].[\(table)] ORDER BY vbl")
            XCTAssertEqual(rows.count, sizes.count)
            for (idx, s) in sizes.enumerated() {
                XCTAssertEqual(rows[idx].column("nvl")?.int, (s/2) * 2) // bytes for NVARCHAR
                XCTAssertEqual(rows[idx].column("vbl")?.int, s)
            }
            }
        }
    }
}
