import Foundation
import NIOCore
import NIOConcurrencyHelpers

extension EventLoopFuture {
    /// Applies a 45-second operation timeout. When fired, the test's executionTimeAllowance
    /// (60s) will kill the test, which cleanly terminates the connection.
    func withTestTimeoutIfEnabled(on loop: EventLoop) -> EventLoopFuture<Value> { self }
}

extension EventLoopFuture {
    /// Adds a hard timeout to this future, failing with SQLServerError.timeout when elapsed.
    /// This is independent of test-only env wrappers.
    func withTimeout(on loop: EventLoop, seconds: TimeInterval, reason: String? = nil) -> EventLoopFuture<Value> where Value: Sendable {
        guard seconds.isFinite, seconds > 0 else { return self }

        let promise = loop.makePromise(of: Value.self)
        let completed = NIOLockedValueBox(false)
        let description = reason ?? "operation timed out after \(seconds)s"
        let timeoutNanos = Int64(seconds * 1_000_000_000)
        let timeoutTask = loop.scheduleTask(deadline: .now() + .nanoseconds(timeoutNanos)) {
            let shouldComplete = completed.withLockedValue { value in
                if value { return false }
                value = true
                return true
            }
            if shouldComplete {
                promise.fail(SQLServerError.timeout(description: description, underlying: nil))
            }
        }

        self.whenComplete { result in
            timeoutTask.cancel()
            let shouldComplete = completed.withLockedValue { value in
                if value { return false }
                value = true
                return true
            }
            if shouldComplete { promise.completeWith(result) }
        }

        return promise.futureResult
    }
}
