import XCTest
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

/// Windows (Kerberos) authentication (`SQLSERVER_TEST_KERBEROS_URL`): the URL's user is the
/// principal (`user@REALM`); with a password the driver gets the ticket itself, without one it uses
/// the ticket already in the cache (`kinit` first). `krb5Config` names the Kerberos settings and
/// `serviceHost` the name the service ticket is for.
final class KerberosLoginTests: XCTestCase, @unchecked Sendable {
    private func server() throws -> TestServer {
        let server = try requireSQLServerTestServer(TestServer.kerberosVariable)
        guard server.kerberos != nil else {
            throw XCTSkip("\(TestServer.kerberosVariable) does not say authentication=kerberos")
        }
        server.useKerberosSettings()
        return server
    }

    private func configuration(_ server: TestServer, password: String? = nil, realm: String? = nil) -> SQLServerConnection.Configuration {
        var configuration = server.configuration
        if password != nil || realm != nil {
            let parts = server.username.split(separator: "@", maxSplits: 1).map(String.init)
            configuration.login.authentication = .windowsIntegrated(
                username: parts.first ?? server.username,
                password: password ?? server.password,
                domain: realm ?? (parts.count > 1 ? parts[1] : nil)
            )
        }
        configuration.connectTimeoutSeconds = 20
        configuration.retryConfiguration = .init(maximumAttempts: 1)
        return configuration
    }

    private func expectedUser(_ server: TestServer) -> String {
        String(server.username.split(separator: "@").first ?? Substring(server.username)).lowercased()
    }

    /// The session's login, and its authentication scheme when the login may read
    /// `sys.dm_exec_connections` (VIEW SERVER STATE); a Windows login to SQL Server on Linux is
    /// Kerberos either way.
    private func loginAndScheme(_ run: (String) async throws -> [SQLServerRow]) async throws -> (String, String?) {
        let login = try await run("SELECT SUSER_SNAME() AS login;").first?.column("login")?.string ?? ""
        let scheme = try? await run("SELECT auth_scheme FROM sys.dm_exec_connections WHERE session_id = @@SPID;")
            .first?.column("auth_scheme")?.string
        return (login, scheme)
    }

    func testLogsInWithKerberos() async throws {
        let server = try server()
        let connection = try await SQLServerConnection.connect(configuration: configuration(server))
        defer { Task { try? await connection.close() } }
        let (login, scheme) = try await loginAndScheme { try await connection.query($0) }
        XCTAssertTrue(login.contains("\\"), "A Windows login: \(login)")
        XCTAssertEqual(login.split(separator: "\\").last.map { $0.lowercased() }, expectedUser(server), login)
        if let scheme { XCTAssertEqual(scheme, "KERBEROS") }
    }

    func testPooledSessionsStayKerberosAfterReset() async throws {
        let server = try server()
        var clientConfiguration = SQLServerClient.Configuration(connection: configuration(server))
        clientConfiguration.poolConfiguration.maximumConcurrentConnections = 2
        let client = try await SQLServerClient.connect(configuration: clientConfiguration, numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        for _ in 0..<3 {
            let (login, scheme) = try await loginAndScheme { try await client.query($0) }
            XCTAssertEqual(login.split(separator: "\\").last.map { $0.lowercased() }, expectedUser(server), login)
            if let scheme { XCTAssertEqual(scheme, "KERBEROS") }
        }
    }

    func testWrongPasswordFailsAsAuthenticationWithoutRetrying() async throws {
        let server = try server()
        let started = ContinuousClock.now
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration(server, password: "wrong-\(UUID().uuidString)"))
            try? await connection.close()
            XCTFail("A wrong Kerberos password must not log in")
        } catch {
            XCTAssertLessThan(ContinuousClock.now - started, .seconds(15))
            XCTAssertFalse("\(error)".isEmpty)
        }
    }

    /// Negotiate: without a KDC for the realm the driver falls back to NTLMv2 at once (SQL Server on
    /// Linux then refuses it). The Kerberos attempt must not hold up the login.
    func testUnknownRealmFallsBackToNTLMQuickly() async throws {
        let server = try server()
        let started = ContinuousClock.now
        do {
            let connection = try await SQLServerConnection.connect(
                configuration: configuration(server, password: "not-the-password", realm: "NO-SUCH-REALM.TEST")
            )
            try? await connection.close()
            XCTFail("SQL Server on Linux accepts only Kerberos")
        } catch {
            XCTAssertLessThan(ContinuousClock.now - started, .seconds(5), "The Kerberos attempt delayed the login")
        }
    }
}
