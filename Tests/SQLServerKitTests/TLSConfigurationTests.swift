import XCTest
import NIOSSL
import SQLServerKit
@testable import SQLServerTDS

final class TLSConfigurationTests: XCTestCase, @unchecked Sendable {

    // MARK: - Static TLS Configurations

    func testTrustingServerCertificateHasNoneVerification() {
        let config: SQLServerTLSConfiguration = .trustingServerCertificate
        XCTAssertEqual(config.certificateVerification, CertificateVerification.none)
    }

    func testClientDefaultHasFullVerification() {
        let config = SQLServerClient.Configuration(
            hostname: "localhost",
            port: 1433,
            login: .init(database: "master", authentication: .sqlPassword(username: "sa", password: "test"))
        ).tlsConfiguration
        XCTAssertNotNil(config)
        XCTAssertEqual(config?.minimumTLSVersion, .tlsv12)
        XCTAssertEqual(config?.certificateVerification, CertificateVerification.fullVerification)
    }

    func testCustomCAStillVerifiesHostname() {
        let config: SQLServerTLSConfiguration = .withCACertificate(atPath: "/tmp/test-ca.pem")
        XCTAssertEqual(config.certificateVerification, CertificateVerification.fullVerification)
    }

    func testIPAddressIsOmittedFromTLSClientSNI() {
        XCTAssertNil(tdsTLSHostnameForSNI("127.0.0.1"))
        XCTAssertNil(tdsTLSHostnameForSNI("::1"))
        XCTAssertEqual(tdsTLSHostnameForSNI("db.example.com"), "db.example.com")
    }

    // MARK: - Configuration.init(tlsEnabled:trustServerCertificate:)

    func testTLSEnabledWithTrustProducesNoneVerification() {
        let config = SQLServerClient.Configuration(
            hostname: "localhost",
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "test"),
            tlsEnabled: true,
            trustServerCertificate: true
        )
        let tlsConfig = config.tlsConfiguration
        XCTAssertNotNil(tlsConfig)
        XCTAssertEqual(tlsConfig?.certificateVerification, CertificateVerification.none)
    }

    func testTLSEnabledWithoutTrustProducesFullVerification() {
        let config = SQLServerClient.Configuration(
            hostname: "localhost",
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "test"),
            tlsEnabled: true,
            trustServerCertificate: false
        )
        let tlsConfig = config.tlsConfiguration
        XCTAssertNotNil(tlsConfig)
        XCTAssertEqual(tlsConfig?.certificateVerification, CertificateVerification.fullVerification)
    }

    func testTLSEnabledDefaultsTrustToFalse() {
        let config = SQLServerClient.Configuration(
            hostname: "localhost",
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "test"),
            tlsEnabled: true
        )
        let tlsConfig = config.tlsConfiguration
        XCTAssertNotNil(tlsConfig)
        XCTAssertEqual(tlsConfig?.certificateVerification, CertificateVerification.fullVerification)
    }

    func testTLSDisabledIgnoresTrustFlag() {
        let config = SQLServerClient.Configuration(
            hostname: "localhost",
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "test"),
            tlsEnabled: false,
            trustServerCertificate: true
        )
        XCTAssertNil(config.tlsConfiguration)
    }

    func testUnencryptedLoginFailsBeforeConnecting() async throws {
        let config = SQLServerClient.Configuration(
            hostname: "127.0.0.1",
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "secret"),
            tlsEnabled: false,
            encryptionMode: .optional
        )
        do {
            let client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
            try await client.shutdownGracefully()
            XCTFail("Unencrypted LOGIN7 should be refused")
        } catch {
            XCTAssertTrue(String(describing: error).contains("TLS configuration is required"))
        }
    }

    func testStrictModeRequiresCertificateVerification() async throws {
        let config = SQLServerClient.Configuration(
            hostname: "127.0.0.1",
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "secret"),
            tlsEnabled: true,
            trustServerCertificate: true,
            encryptionMode: .strict
        )
        do {
            let client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
            try await client.shutdownGracefully()
            XCTFail("Strict mode must reject disabled certificate verification")
        } catch {
            XCTAssertTrue(String(describing: error).contains("full server certificate verification"))
        }
    }
}
