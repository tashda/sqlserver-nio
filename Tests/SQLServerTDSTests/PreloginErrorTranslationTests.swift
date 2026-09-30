import XCTest
import NIOSSL
@testable import SQLServerTDS

/// Verifies that raw NIOSSL handshake errors that bubble up during the TDS
/// PRELOGIN exchange are translated into a `TDSError.sslError` whose message
/// tells the user to enable Trust Server Certificate. Without this, callers
/// see the opaque string "uncleanShutdown" with no actionable guidance.
final class PreloginErrorTranslationTests: XCTestCase, @unchecked Sendable {

    func testUncleanShutdownDuringTLSExplainsTheLikelyVersionMismatch() {
        // SQL Server hangs up mid-handshake when it cannot agree on a TLS
        // version (verified against a TLS 1.0-only server in the test lab).
        let translated = translatePreloginError(NIOSSLError.uncleanShutdown, attemptedTLS: true)
        guard case let TDSError.tlsHandshake(kind, message) = translated else {
            return XCTFail("expected TDSError.tlsHandshake, got \(translated)")
        }
        XCTAssertEqual(kind, .other)
        XCTAssertTrue(message.contains("TLS 1.2"), message)
        XCTAssertFalse(message.lowercased() == "uncleanshutdown", "should not surface the raw NIOSSL string")
    }

    func testHandshakeFailedBecomesClassifiedTLSError() throws {
        // Verification and version failures are classified from BoringSSL's
        // reason text; LabTLSTests covers them against real servers. An
        // unknown reason still becomes a TLS error with the reason attached.
        let translated = translatePreloginError(
            NIOSSLError.handshakeFailed(BoringSSLError.unknownError([])),
            attemptedTLS: true
        )
        guard case let TDSError.tlsHandshake(_, message) = translated else {
            return XCTFail("expected TDSError.tlsHandshake, got \(translated)")
        }
        XCTAssertTrue(message.hasPrefix("TLS handshake failed"), message)
    }

    func testNonSSLErrorPassesThroughUnchanged() {
        let original = TDSError.protocolError("something else")
        let translated = translatePreloginError(original, attemptedTLS: true)
        guard case let TDSError.protocolError(message) = translated else {
            return XCTFail("expected pass-through TDSError.protocolError, got \(translated)")
        }
        XCTAssertEqual(message, "something else")
    }

    func testNoTranslationWhenTLSWasNotAttempted() {
        // If the caller didn't ask for TLS, any NIOSSL error is unexpected — we
        // still pass it through unchanged so the caller can see the raw cause.
        let translated = translatePreloginError(NIOSSLError.uncleanShutdown, attemptedTLS: false)
        XCTAssertTrue(translated is NIOSSLError, "expected raw NIOSSLError to pass through, got \(translated)")
    }
}
