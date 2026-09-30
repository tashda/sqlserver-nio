import XCTest
import NIOSSL
import SQLServerKit
import SQLServerKitTesting

/// Certificate validation and TDS 8.0 Strict against lab servers that use a
/// certificate from the lab CA (`testlab/testlab.sh tls`).
///
/// Without the `NIO_LAB_TLS_*` variables these tests skip, unless
/// `NIO_LAB_REQUIRE=1`, in which case a missing lab is a failure.
final class LabTLSTests: XCTestCase, @unchecked Sendable {
    private struct Lab {
        let host: String
        let certificateName: String
        let caPath: String
        let otherCAPath: String
        let tlsPort: Int
        let strictPort: Int
        let expiredPort: Int
        let username: String
        let password: String
    }

    private func lab() throws -> Lab {
        guard let host = env("NIO_LAB_TLS_HOST"),
              let name = env("NIO_LAB_TLS_CERT_NAME"),
              let ca = env("NIO_LAB_TLS_CA"),
              let otherCA = env("NIO_LAB_TLS_OTHER_CA"),
              let tlsPort = env("NIO_LAB_TLS_PORT").flatMap(Int.init),
              let strictPort = env("NIO_LAB_STRICT_PORT").flatMap(Int.init),
              let expiredPort = env("NIO_LAB_EXPIRED_PORT").flatMap(Int.init),
              let username = env("NIO_LAB_TLS_USERNAME"),
              let password = env("NIO_LAB_TLS_PASSWORD") else {
            if envFlagEnabled("NIO_LAB_REQUIRE") {
                XCTFail("TLS lab is required but NIO_LAB_TLS_* is not set; run testlab/testlab.sh tls")
            }
            throw XCTSkip("TLS lab not configured (testlab/testlab.sh tls)")
        }
        return Lab(host: host, certificateName: name, caPath: ca, otherCAPath: otherCA, tlsPort: tlsPort,
                   strictPort: strictPort, expiredPort: expiredPort, username: username, password: password)
    }

    private func configuration(
        _ lab: Lab,
        port: Int,
        ca: String,
        mode: SQLServerEncryptionMode,
        certificateName: String?
    ) -> SQLServerConnection.Configuration {
        var configuration = SQLServerConnection.Configuration(
            hostname: lab.host,
            port: port,
            login: .init(database: "master", authentication: .sqlPassword(username: lab.username, password: lab.password)),
            tlsConfiguration: .withCACertificate(atPath: ca),
            encryptionMode: mode,
            hostNameInCertificate: certificateName
        )
        configuration.connectTimeoutSeconds = 20
        configuration.retryConfiguration = .init(maximumAttempts: 1)
        return configuration
    }

    private func assertEncryptedSession(_ connection: SQLServerConnection) async throws {
        let rows = try await connection.query("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID;")
        XCTAssertEqual(rows.first?.column("encrypt_option")?.string?.uppercased(), "TRUE")
    }

    private func assertTLSFailure(_ configuration: SQLServerConnection.Configuration, _ reason: String) async {
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration)
            try? await connection.close()
            XCTFail("Connection should fail: \(reason)")
        } catch {
            let text = String(describing: error).lowercased()
            XCTAssertTrue(text.contains("tls") || text.contains("ssl") || text.contains("certificate"),
                          "Expected a TLS failure (\(reason)), got \(error)")
        }
    }

    func testMandatoryWithLabCAAndMatchingNameConnects() async throws {
        let lab = try lab()
        let connection = try await SQLServerConnection.connect(
            configuration: configuration(lab, port: lab.tlsPort, ca: lab.caPath, mode: .mandatory, certificateName: lab.certificateName)
        )
        defer { Task { try? await connection.close() } }
        try await assertEncryptedSession(connection)
    }

    func testCertificateForAnotherHostIsRejected() async throws {
        let lab = try lab()
        await assertTLSFailure(
            configuration(lab, port: lab.tlsPort, ca: lab.caPath, mode: .mandatory, certificateName: "other.nio.test"),
            "certificate does not name the host"
        )
    }

    func testCertificateFromUntrustedCAIsRejected() async throws {
        let lab = try lab()
        await assertTLSFailure(
            configuration(lab, port: lab.tlsPort, ca: lab.otherCAPath, mode: .mandatory, certificateName: lab.certificateName),
            "certificate is not signed by a trusted CA"
        )
    }

    func testExpiredCertificateIsRejected() async throws {
        let lab = try lab()
        await assertTLSFailure(
            configuration(lab, port: lab.expiredPort, ca: lab.caPath, mode: .mandatory, certificateName: lab.certificateName),
            "certificate expired"
        )
    }

    func testStrictNegotiatesTLSFirstAndRunsQueries() async throws {
        let lab = try lab()
        let connection = try await SQLServerConnection.connect(
            configuration: configuration(lab, port: lab.strictPort, ca: lab.caPath, mode: .strict, certificateName: lab.certificateName)
        )
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

    func testStrictRejectsWrongHostName() async throws {
        let lab = try lab()
        await assertTLSFailure(
            configuration(lab, port: lab.strictPort, ca: lab.caPath, mode: .strict, certificateName: "other.nio.test"),
            "strict certificate does not name the host"
        )
    }

    func testMandatoryClientAgainstStrictServerFailsClearly() async throws {
        let lab = try lab()
        // A server that forces strict encryption does not accept TDS 7.x
        // PRELOGIN; the attempt must fail rather than hang.
        let started = ContinuousClock.now
        do {
            let connection = try await SQLServerConnection.connect(
                configuration: configuration(lab, port: lab.strictPort, ca: lab.caPath, mode: .mandatory, certificateName: lab.certificateName)
            )
            try? await connection.close()
            XCTFail("A TDS 7.x client should be refused by a strict-only server")
        } catch {
            // Expected.
        }
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(25))
    }
}
