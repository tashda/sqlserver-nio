import XCTest
import SQLServerKit
import SQLServerKitTesting

/// Windows (Kerberos) authentication against SQL Server on Linux in a Samba
/// Active Directory domain (`Tests/Fixtures/kerberos/start-server.sh`). The test process gets
/// its tickets from the lab's KDC through `KRB5_CONFIG`.
///
/// Without the `NIO_LAB_KRB_*` variables these tests skip, unless
/// `NIO_LAB_REQUIRE=1`, in which case a missing lab is a failure.
final class LabKerberosTests: XCTestCase, @unchecked Sendable {
    private struct Lab {
        let host: String
        let port: Int
        let username: String
        let password: String
        let domain: String
        let login: String
    }

    private func lab() throws -> Lab {
        guard let host = env("NIO_LAB_KRB_HOST"),
              let port = env("NIO_LAB_KRB_PORT").flatMap(Int.init),
              let username = env("NIO_LAB_KRB_USERNAME"),
              let password = env("NIO_LAB_KRB_PASSWORD"),
              let domain = env("NIO_LAB_KRB_DOMAIN"),
              let login = env("NIO_LAB_KRB_LOGIN") else {
            if envFlagEnabled("NIO_LAB_REQUIRE") {
                XCTFail("Kerberos lab is required but NIO_LAB_KRB_* is not set; run Tests/Fixtures/kerberos/start-server.sh")
            }
            throw XCTSkip("Kerberos lab not configured (Tests/Fixtures/kerberos/start-server.sh)")
        }
        return Lab(host: host, port: port, username: username, password: password, domain: domain, login: login)
    }

    private func configuration(_ lab: Lab, password: String? = nil) -> SQLServerConnection.Configuration {
        var configuration = SQLServerConnection.Configuration(
            hostname: lab.host,
            port: lab.port,
            login: .init(
                database: "master",
                authentication: .windowsIntegrated(username: lab.username, password: password ?? lab.password, domain: lab.domain)
            ),
            tlsConfiguration: .trustingServerCertificate
        )
        configuration.connectTimeoutSeconds = 20
        configuration.retryConfiguration = .init(maximumAttempts: 1)
        return configuration
    }

    func testLogsInWithKerberos() async throws {
        let lab = try lab()
        let connection = try await SQLServerConnection.connect(configuration: configuration(lab))
        defer { Task { try? await connection.close() } }
        let rows = try await connection.query(
            "SELECT SUSER_SNAME() AS login, auth_scheme FROM sys.dm_exec_connections WHERE session_id = @@SPID;"
        )
        XCTAssertEqual(rows.first?.column("login")?.string?.uppercased(), lab.login.uppercased())
        XCTAssertEqual(rows.first?.column("auth_scheme")?.string, "KERBEROS")
    }

    func testPooledSessionsStayKerberosAfterReset() async throws {
        let lab = try lab()
        var clientConfiguration = SQLServerClient.Configuration(connection: configuration(lab))
        clientConfiguration.poolConfiguration.maximumConcurrentConnections = 2
        let client = try await SQLServerClient.connect(configuration: clientConfiguration, numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        for _ in 0..<3 {
            let rows = try await client.query("SELECT SUSER_SNAME() AS login;")
            XCTAssertEqual(rows.first?.column("login")?.string?.uppercased(), lab.login.uppercased())
        }
    }

    func testWrongPasswordFailsAsAuthenticationWithoutRetrying() async throws {
        let lab = try lab()
        let started = ContinuousClock.now
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration(lab, password: "wrong-\(UUID().uuidString)"))
            try? await connection.close()
            XCTFail("A wrong Kerberos password must not log in")
        } catch {
            XCTAssertLessThan(ContinuousClock.now - started, .seconds(15))
            XCTAssertFalse("\(error)".isEmpty)
        }
    }
}
