import XCTest
import SQLServerKit
import SQLServerKitTesting

/// Read-only routing against a three-replica availability group
/// (`testlab/ag.sh up`). A read-intent login to the primary receives an
/// ENVCHANGE routing token and must continue on the secondary it names.
final class LabAvailabilityGroupTests: XCTestCase, @unchecked Sendable {
    private struct Lab {
        let primaryHost: String
        let primaryPort: Int
        let database: String
        let username: String
        let password: String
    }

    private func lab() throws -> Lab {
        guard let primary = env("NIO_LAB_AG_PRIMARY"),
              let database = env("NIO_LAB_AG_DATABASE"),
              let username = env("NIO_LAB_AG_USERNAME"),
              let password = env("NIO_LAB_AG_PASSWORD") else {
            if envFlagEnabled("NIO_LAB_REQUIRE") {
                XCTFail("Availability group lab is required but NIO_LAB_AG_* is not set; run testlab/ag.sh up")
            }
            throw XCTSkip("Availability group lab not configured (testlab/ag.sh up)")
        }
        let parts = primary.split(separator: ":")
        return Lab(primaryHost: String(parts[0]), primaryPort: Int(parts[1]) ?? 1433, database: database,
                   username: username, password: password)
    }

    private func configuration(_ lab: Lab, readOnly: Bool) -> SQLServerConnection.Configuration {
        var configuration = SQLServerConnection.Configuration(
            hostname: lab.primaryHost,
            port: lab.primaryPort,
            login: .init(database: lab.database, authentication: .sqlPassword(username: lab.username, password: lab.password)),
            tlsConfiguration: .trustingServerCertificate,
            readOnlyIntent: readOnly
        )
        configuration.connectTimeoutSeconds = 20
        return configuration
    }

    private func serverAndUpdateability(_ connection: SQLServerConnection) async throws -> (String?, String?) {
        let rows = try await connection.query("SELECT @@SERVERNAME AS server, CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS NVARCHAR(20)) AS mode;")
        return (rows.first?.column("server")?.string, rows.first?.column("mode")?.string)
    }

    func testReadIntentLoginFollowsRoutingToSecondary() async throws {
        let lab = try lab()
        let connection = try await SQLServerConnection.connect(configuration: configuration(lab, readOnly: true))
        defer { Task { try? await connection.close() } }
        let (server, mode) = try await serverAndUpdateability(connection)
        XCTAssertNotEqual(server?.lowercased(), "ag1", "Read-intent login should be routed away from the primary")
        XCTAssertEqual(mode, "READ_ONLY")
        let probe = try await connection.query("SELECT v FROM dbo.probe WHERE id = 1;")
        XCTAssertEqual(probe.first?.column("v")?.string, "primary", "Secondary serves replicated data")
    }

    func testReadWriteLoginStaysOnPrimary() async throws {
        let lab = try lab()
        let connection = try await SQLServerConnection.connect(configuration: configuration(lab, readOnly: false))
        defer { Task { try? await connection.close() } }
        let (server, mode) = try await serverAndUpdateability(connection)
        XCTAssertEqual(server?.lowercased(), "ag1")
        XCTAssertEqual(mode, "READ_WRITE")
    }

    func testPooledReadIntentSessionsAreRoutedAndReset() async throws {
        let lab = try lab()
        var configuration = SQLServerClient.Configuration(connection: configuration(lab, readOnly: true))
        configuration.poolConfiguration.maximumConcurrentConnections = 2
        let client = try await SQLServerClient.connect(configuration: configuration, numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        for _ in 0..<5 {
            let rows = try await client.query("SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS NVARCHAR(20)) AS mode, DB_NAME() AS db;")
            XCTAssertEqual(rows.first?.column("mode")?.string, "READ_ONLY")
            XCTAssertEqual(rows.first?.column("db")?.string, lab.database, "Session reset restores the routed database")
        }
    }
}
