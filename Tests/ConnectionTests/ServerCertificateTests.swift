import XCTest
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

/// A SQL Server without a certificate of its own (as `docker run` starts it) presents one it made
/// itself. Validated against the system's trust store, the failure says so.
final class ServerCertificateTests: XCTestCase, @unchecked Sendable {
    func testServersOwnSelfSignedCertificateIsNamed() async throws {
        let server = try requireSQLServerTestServer()
        guard server.trustServerCertificate, server.caFile == nil else {
            throw XCTSkip("\(server.variable) validates the certificate, so the server may not use a self-signed one")
        }
        var configuration = server.configuration
        configuration.tlsConfiguration = .clientDefault
        configuration.encryptionMode = .mandatory
        configuration.retryConfiguration = .init(maximumAttempts: 1)
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration)
            try? await connection.close()
            throw XCTSkip("The server's certificate is trusted by this machine")
        } catch SQLServerError.tlsFailed(let failure) {
            XCTAssertEqual(failure.kind, .certificateSelfSigned, failure.message)
            XCTAssertEqual(failure.certificate?.isSelfSigned, true)
        }
    }
}
