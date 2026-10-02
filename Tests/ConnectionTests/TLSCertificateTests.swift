import XCTest
import NIOSSL
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

/// Certificate validation and TDS 8.0 Strict against a server that requires TLS
/// (`SQLSERVER_TEST_TLS_URL`, with the CA that signed the server's certificate in `caFile`). With
/// `encrypt=strict` the Strict tests run too; see TESTING.md for a server made with plain Docker.
final class TLSCertificateTests: XCTestCase, @unchecked Sendable {
    /// A CA that signed nothing the server uses.
    private static let unrelatedCA = """
-----BEGIN CERTIFICATE-----
MIIDFjCCAf6gAwIBAgIUCkic/R+h8idVhB1FaWndbwxGwTQwDQYJKoZIhvcNAQEL
BQAwKjEoMCYGA1UEAwwfc3Fsc2VydmVyLW5pbyB1bnRydXN0ZWQgdGVzdCBDQTAg
Fw0yNjEwMDEwODA3NTBaGA8yMTI2MDkwNzA4MDc1MFowKjEoMCYGA1UEAwwfc3Fs
c2VydmVyLW5pbyB1bnRydXN0ZWQgdGVzdCBDQTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAM4Pa0YFWfx4JGFOeyA2qW67BJK/iJeh70BPQmBxKRTz0V9w
41rW7LhG2jMWoZUURJskERMLERnq8pHYPvtFQR/cHwvtwIFdmD3gm2FaCvHofwr0
IodkFOXlkY2w6PXS9KS5CtbjTsujfMNP+rDk+1RP5NeAzIJFuGj+ta7SvC3CEVqu
Zd7s4dEbXMsQXwEp2DQibk6bTqU7ovetAzDBVd6AIniibYIYwpV280sw0IGUSs64
xVzZDtaAtjU6hSIrIEIHgbjM30b5GNk8SJk7uPtpg6U6HUla0o97XaHuwczWyYXu
JO5OazZi2NTgn/JipGJVMC9kicqq/589BpFALYUCAwEAAaMyMDAwHQYDVR0OBBYE
FOF7aty49QS9RsjciT5bWlSR9eRqMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcN
AQELBQADggEBADLLpk9VGR5rpZ6nnxWXb0itlMEqi0cCM+f4KfvWqY6TGc0mxatX
+ggixffRJTcP8nLDWu5arUSXVhUoEvtIHLY07vsCErUstXCNVf6VaQ2HbteY+Gxq
V8fSAtk6R5xRfc+35Uog8gw3F2iKSkF1rVyA/FWcELWZTnnmnDZiahH7dz2997d7
NmqzDqDlNHCE+vpZhmN5jWEG3ekxy+ipE17etzTHXU4IBtO9ix1W+qWXc9E9wu/0
L9vQv2xS85u5m93Q1OHUNWdGn1svNemAR4Q1KvCwmQIlS38LgFfvDzR7YVYmawxM
fgL5G6TWr/kiYUGgg3otfbbQAg/JAfjfHGI=
-----END CERTIFICATE-----
"""

    private func server() throws -> TestServer {
        let server = try requireSQLServerTestServer(TestServer.tlsVariable)
        guard server.caFile != nil else {
            throw XCTSkip("\(TestServer.tlsVariable) has no caFile, so certificate checks cannot be tested")
        }
        return server
    }

    private func strictServer() throws -> TestServer {
        let server = try server()
        guard server.encrypt == .strict else {
            throw XCTSkip("\(TestServer.tlsVariable) is not encrypt=strict")
        }
        return server
    }

    private func configuration(
        _ server: TestServer,
        ca: String? = nil,
        mode: SQLServerEncryptionMode? = nil,
        certificateName: String? = nil
    ) -> SQLServerConnection.Configuration {
        var configuration = server.configuration
        if let ca { configuration.tlsConfiguration = .withCACertificate(atPath: ca) }
        if let mode { configuration.encryptionMode = mode }
        if let certificateName { configuration.hostNameInCertificate = certificateName }
        configuration.connectTimeoutSeconds = 20
        configuration.retryConfiguration = .init(maximumAttempts: 1)
        return configuration
    }

    private func unrelatedCAFile() throws -> String {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent("echo-sqlserver-unrelated-ca.pem").path
        try Self.unrelatedCA.write(toFile: path, atomically: true, encoding: .utf8)
        return path
    }

    private func assertEncryptedSession(_ connection: SQLServerConnection) async throws {
        let rows = try await connection.query("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID;")
        XCTAssertEqual(rows.first?.column("encrypt_option")?.string?.uppercased(), "TRUE")
    }

    private func tlsFailure(_ configuration: SQLServerConnection.Configuration) async throws -> SQLServerTLSFailure {
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration)
            try? await connection.close()
            XCTFail("Connection should fail")
            throw SQLServerError.connectionClosed
        } catch SQLServerError.tlsFailed(let failure) {
            return failure
        }
    }

    func testConnectsWithTheCAAndTheSessionIsEncrypted() async throws {
        let server = try server()
        let connection = try await SQLServerConnection.connect(configuration: configuration(server))
        defer { Task { try? await connection.close() } }
        try await assertEncryptedSession(connection)
    }

    func testCertificateForAnotherHostIsRejectedAndNamed() async throws {
        let server = try server()
        let failure = try await tlsFailure(configuration(server, certificateName: "other.echo-sqlserver.invalid"))
        XCTAssertEqual(failure.kind, .certificateNameMismatch, failure.message)
        XCTAssertEqual(failure.expectedHost, "other.echo-sqlserver.invalid")
        XCTAssertFalse(failure.certificate?.names.isEmpty ?? true, "The failure lists the names the certificate has")
    }

    func testCertificateFromAnUntrustedCAIsRejectedAndNamed() async throws {
        let server = try server()
        let failure = try await tlsFailure(configuration(server, ca: try unrelatedCAFile()))
        XCTAssertEqual(failure.kind, .certificateUntrusted, failure.message)
        XCTAssertNotNil(failure.certificate?.issuer)
        XCTAssertEqual(failure.certificate?.sha256Fingerprint.count, 32 * 3 - 1)
    }

    func testStrictNegotiatesTLSFirstAndRunsQueries() async throws {
        let server = try strictServer()
        let connection = try await SQLServerConnection.connect(configuration: configuration(server))
        defer { Task { try? await connection.close() } }
        try await assertEncryptedSession(connection)
        // Cancellation and a follow-up request work inside the outer TLS.
        let task = Task { try await connection.execute("WAITFOR DELAY '00:00:20';") }
        try await Task.sleep(for: .milliseconds(300))
        task.cancel()
        _ = try? await task.value
        let rows = try await connection.query("SELECT 1 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 1)
    }

    func testMandatoryClientAgainstStrictServerFailsClearly() async throws {
        let server = try strictServer()
        // A server that forces strict encryption does not accept TDS 7.x PRELOGIN; the attempt must
        // fail rather than hang.
        let started = ContinuousClock.now
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration(server, mode: .mandatory))
            try? await connection.close()
            XCTFail("A TDS 7.x client should be refused by a strict-only server")
        } catch {
            // Expected.
        }
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(25))
    }
}
