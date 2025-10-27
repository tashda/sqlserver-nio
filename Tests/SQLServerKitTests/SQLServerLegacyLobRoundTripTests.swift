@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerLegacyLobRoundTripTests: XCTestCase {
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
        client = nil
        group = nil
    }

    func testNTextAndImageRoundTrip() async throws {
        try await withTimeout(30) {
            try await withTemporaryDatabase(client: self.client, prefix: "lob") { db in
            let table = "legacy_lob_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
            // TEXT/NTEXT/IMAGE are deprecated; if this fails on your server config, we will skip.
            do {
                _ = try await executeInDb(client: self.client, database: db, """
                    CREATE TABLE [dbo].[\(table)] (
                        t TEXT NULL,
                        n NTEXT NULL,
                        i IMAGE NULL
                    );
                """)
            } catch {
                throw XCTSkip("Server does not allow TEXT/NTEXT/IMAGE: \(error)")
            }

            let textPayload = String(repeating: "A", count: 8_192)
            let ntextPayload = String(repeating: "Ω", count: 4_096)
            let imagePayload = Array((0..<8192).map { UInt8($0 & 0xFF) })
            let textLiteral = SQLServerLiteralValue.string(textPayload).sqlLiteral()
            let ntextLiteral = SQLServerLiteralValue.nString(ntextPayload).sqlLiteral()
            let imageLiteral = SQLServerLiteralValue.bytes(imagePayload).sqlLiteral()

            _ = try await executeInDb(client: self.client, database: db, """
                INSERT INTO [\(table)] (t, n, i) VALUES (\(textLiteral), \(ntextLiteral), \(imageLiteral));
            """)

            let rows = try await queryInDb(client: self.client, database: db, "SELECT DATALENGTH(t) AS tl, DATALENGTH(n) AS nl, DATALENGTH(i) AS il FROM [\(table)]")
            guard let row = rows.first else { XCTFail("Missing LOB row"); return }
            XCTAssertEqual(row.column("tl")?.int, textPayload.count)
            // NTEXT stores UCS-2, so its byte length is 2x chars
            XCTAssertEqual(row.column("nl")?.int, ntextPayload.utf16.count * 2)
            XCTAssertEqual(row.column("il")?.int, imagePayload.count)

            // Also verify NBCROW behavior with nulls
            _ = try await executeInDb(client: self.client, database: db, "INSERT INTO [\(table)] (t, n, i) VALUES (NULL, NULL, NULL);")
            let nullRow = try await queryInDb(client: self.client, database: db, "SELECT t, n, i FROM [\(table)] WHERE t IS NULL").first
            XCTAssertNil(nullRow?.column("t")?.string)
            XCTAssertNil(nullRow?.column("n")?.string)
            XCTAssertNil(nullRow?.column("i")?.bytes)
            }
        }
    }

    func testXmlAndMaxChunkingRoundTrip() async throws {
        try await withTimeout(60) {
            try await withTemporaryDatabase(client: self.client, prefix: "plp") { db in
            let table = "plp_roundtrip_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(table)] (
                    x XML NULL,
                    nv NVARCHAR(MAX) NULL,
                    vb VARBINARY(MAX) NULL
                );
            """)

            let xmlPayload = "<root>" + String(repeating: "<v>z</v>", count: 50_000) + "</root>" // sizable XML
            let nvPayload = String(repeating: "ユ", count: 100_000) // multi-chunk NVARCHAR(MAX)
            let vbPayload = Array((0..<200_000).map { UInt8($0 & 0xFF) }) // multi-chunk VARBINARY(MAX)
            let xmlLiteral = SQLServerLiteralValue.string(xmlPayload).sqlLiteral()
            let nvLiteral = SQLServerLiteralValue.nString(nvPayload).sqlLiteral()
            let vbLiteral = SQLServerLiteralValue.bytes(vbPayload).sqlLiteral()

            _ = try await executeInDb(client: self.client, database: db, """
                INSERT INTO [\(table)] (x, nv, vb) VALUES (\(xmlLiteral), \(nvLiteral), \(vbLiteral));
            """)

            let rows = try await queryInDb(client: self.client, database: db, "SELECT DATALENGTH(x) AS xl, DATALENGTH(nv) AS nvl, DATALENGTH(vb) AS vbl FROM [\(table)]")
            guard let row = rows.first else { XCTFail("Missing PLP row"); return }
            XCTAssertGreaterThan(row.column("xl")?.int ?? 0, 0)
            XCTAssertEqual(row.column("nvl")?.int, nvPayload.utf16.count * 2)
            XCTAssertEqual(row.column("vbl")?.int, vbPayload.count)
            }
        }
    }
}
