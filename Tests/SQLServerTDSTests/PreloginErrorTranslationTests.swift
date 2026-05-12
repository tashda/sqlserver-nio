import XCTest
import NIOSSL
@testable import SQLServerTDS

/// Verifies that raw NIOSSL handshake errors that bubble up during the TDS
/// PRELOGIN exchange are translated into a `TDSError.sslError` whose message
/// tells the user to enable Trust Server Certificate. Without this, callers
/// see the opaque string "uncleanShutdown" with no actionable guidance.
final class PreloginErrorTranslationTests: XCTestCase, @unchecked Sendable {

    func testUncleanShutdownDuringTLSBecomesActionableSSLError() {
        let translated = translatePreloginError(NIOSSLError.uncleanShutdown, attemptedTLS: true)
        guard case let TDSError.sslError(message) = translated else {
            return XCTFail("expected TDSError.sslError, got \(translated)")
        }
        XCTAssertTrue(message.contains("Trust Server Certificate"), "message should reference the toggle: \(message)")
        XCTAssertFalse(message.lowercased() == "uncleanshutdown", "should not surface the raw NIOSSL string")
    }

    func testHandshakeFailedBecomesActionableSSLError() throws {
        // Construct a real handshakeFailed via NIOSSL by feeding a verify-failure
        // BoringSSLError reason. We can't easily fabricate one here, so we exercise
        // the translator with a stand-in that exercises the same case via the
        // generic NIOSSLError default path: feed an unknown NIOSSLError through.
        // Instead, validate handshakeFailed mapping by constructing it through the
        // public NIOSSL API: rely on a known reason.
        let translated = translatePreloginError(
            NIOSSLError.handshakeFailed(BoringSSLError.unknownError([])),
            attemptedTLS: true
        )
        guard case let TDSError.sslError(message) = translated else {
            return XCTFail("expected TDSError.sslError, got \(translated)")
        }
        XCTAssertTrue(message.contains("Trust Server Certificate"), "message should reference the toggle: \(message)")
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
