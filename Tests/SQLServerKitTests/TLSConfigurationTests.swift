import XCTest
import NIOSSL
@testable import SQLServerKit

final class TLSConfigurationTests: XCTestCase, @unchecked Sendable {

    // MARK: - Static TLS Configurations

    func testTrustingServerCertificateHasNoneVerification() {
        let config: SQLServerTLSConfiguration = .trustingServerCertificate
        XCTAssertEqual(config.certificateVerification, CertificateVerification.none)
    }

    func testClientDefaultHasFullVerification() {
        // SQLServerTLSConfiguration.clientDefault wraps makeClientConfiguration(),
        // which defaults to fullVerification. Verify via makeClientConfiguration()
        // directly to avoid the ambiguity with NIOSSL's static let of the same name.
        let config = TLSConfiguration.makeClientConfiguration()
        XCTAssertEqual(config.certificateVerification, CertificateVerification.fullVerification)
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
}
