import XCTest
import NIOSSL
@testable import SQLServerTDS

/// TLS failures during login become `TDSError.tlsHandshake` with the reason a user can act on.
final class TLSErrorTranslationTests: XCTestCase {
    /// NIOSSL checks the host name itself when the certificate does not list the address the socket
    /// connected to; that failure is a name mismatch, like the driver's own check.
    func testNIOSSLHostnameFailureIsANameMismatch() {
        let translated = translatePreloginError(NIOSSLExtraError.failedToValidateHostname, attemptedTLS: true, expectedHost: "sql.corp.example")
        guard case TDSError.tlsHandshake(.certificateName, let message)? = translated as? TDSError else {
            return XCTFail("Expected a certificate-name failure, got \(translated)")
        }
        XCTAssertTrue(message.contains("'sql.corp.example'"), message)
    }

    func testErrorsOutsideTLSAreLeftAlone() {
        let translated = translatePreloginError(NIOSSLExtraError.failedToValidateHostname, attemptedTLS: false)
        XCTAssertTrue(translated is NIOSSLExtraError)
    }
}
