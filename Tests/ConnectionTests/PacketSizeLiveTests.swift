import XCTest
import Foundation
import SQLServerKit
import SQLServerKitTesting

/// The network packet size against a live server: the size LOGIN7 asks for is the one SQL Server
/// reports for the session, and requests and results that span many packets arrive intact at the
/// smallest, the default and the largest size.
final class PacketSizeLiveTests: XCTestCase, @unchecked Sendable {
    override func setUp() async throws {
        _ = isLoggingConfigured
        TestEnvironmentManager.loadEnvironmentVariables()
    }

    private func connect(packetSize: Int?) async throws -> SQLServerConnection {
        var configuration = makeSQLServerConnectionConfiguration()
        if let packetSize { configuration.packetSize = packetSize }
        return try await SQLServerConnection.connect(configuration: configuration)
    }

    /// Checks the size the server reports for the session. On an encrypted connection
    /// sys.dm_exec_connections.net_packet_size includes the TLS record overhead (170 bytes on
    /// SQL Server 2022); the size agreed at login is the ENVCHANGE value.
    private func assertServerPacketSize(_ connection: SQLServerConnection, _ size: Int, file: StaticString = #filePath, line: UInt = #line) async throws {
        let rows = try await connection.query(
            "SELECT net_packet_size AS size, encrypt_option AS encrypted FROM sys.dm_exec_connections WHERE session_id = @@SPID"
        )
        let reported = rows.first?.column("size")?.int ?? -1
        if rows.first?.column("encrypted")?.string?.uppercased() == "TRUE" {
            XCTAssertTrue((size...size + 256).contains(reported), "net_packet_size \(reported) for \(size) (encrypted)", file: file, line: line)
        } else {
            XCTAssertEqual(reported, size, "net_packet_size for \(size)", file: file, line: line)
        }
    }

    func testDefaultAsksFor8000() async throws {
        let connection = try await connect(packetSize: nil)
        defer { Task { try? await connection.close() } }
        XCTAssertEqual(connection.negotiatedPacketSize, 8000)
        try await assertServerPacketSize(connection, 8000)
    }

    func testLargeBatchParameterAndResultAtEverySize() async throws {
        for size in [512, 8000, 32767] {
            let connection = try await connect(packetSize: size)
            defer { Task { try? await connection.close() } }
            // SQL Server may grant less than asked (16384 for the largest size on some encrypted
            // sessions); the driver then uses the size it was granted.
            let granted = connection.negotiatedPacketSize
            XCTAssertTrue(granted == size || (size > 16384 && granted >= 16383 && granted < size), "negotiated \(granted) for \(size)")
            try await assertServerPacketSize(connection, granted)

            // A 200 KB batch (UTF-16) spans hundreds of 512-byte packets.
            let text = String(repeating: "packet-", count: 14_286)
            let batch = try await connection.queryScalar("SELECT LEN(N'\(text)') AS n", as: Int.self)
            XCTAssertEqual(batch, text.count, "batch at \(size)")

            // The same text as an RPC parameter.
            let rpc = try await connection.call(procedure: "sp_executesql", parameters: [
                .init(name: "@stmt", value: SQLServerValue(string: "SELECT LEN(N'\(text)') AS n"), direction: .in),
            ])
            XCTAssertEqual(rpc.rows.first?.column("n")?.int, text.count, "RPC at \(size)")

            // A result of about 4 MB.
            var rows = 0
            var lastID = 0
            for try await event in connection.streamQuery(
                "SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id, REPLICATE(N'r', 100) AS payload FROM sys.all_objects a CROSS JOIN sys.all_objects b ORDER BY id"
            ) {
                if case .row(let row) = event {
                    rows += 1
                    lastID = row.column("id")?.int ?? -1
                }
            }
            XCTAssertEqual(rows, 20_000, "rows at \(size)")
            XCTAssertEqual(lastID, 20_000, "last row at \(size)")
        }
    }
}
