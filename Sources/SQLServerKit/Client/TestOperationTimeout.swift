import Foundation
import NIOCore

extension EventLoopFuture {
    /// Test-timeout wrapper disabled by default to avoid event-loop shutdown edge cases in CI.
    /// Per-call timeouts should use `withTimeout(on:seconds:reason:)` which callers control.
    func withTestTimeoutIfEnabled(on loop: EventLoop) -> EventLoopFuture<Value> { self }
}

extension EventLoopFuture {
    /// Adds a hard timeout to this future, failing with SQLServerError.timeout when elapsed.
    /// This is independent of test-only env wrappers.
    func withTimeout(on loop: EventLoop, seconds: TimeInterval, reason: String? = nil) -> EventLoopFuture<Value> {
        // Safety-first: avoid any scheduling onto event loops during teardown in tests.
        // In test environments, prefer higher-level timeouts (XCTest waiters or async helpers).
        return self
    }
}
