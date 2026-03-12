import Foundation
import NIOCore

public struct SQLServerRetryConfiguration {
    public typealias ShouldRetryHandler = @Sendable (Swift.Error) -> Bool
    public typealias BackoffStrategy = @Sendable (_ attempt: Int) -> TimeInterval

    public var maximumAttempts: Int
    public var backoffStrategy: BackoffStrategy
    public var shouldRetry: ShouldRetryHandler

    public static let defaultShouldRetry: ShouldRetryHandler = { error in
        switch SQLServerError.normalize(error) {
        case .connectionClosed:
            return true
        case .timeout:
            // Do not retry operations that already timed out; let callers decide.
            return false
        case .deadlockDetected:
            // Retry deadlock victims (error 1205)
            return true
        case .transient:
            return true
        default:
            return false
        }
    }

    public init(
        maximumAttempts: Int = 3,
        backoffStrategy: @escaping BackoffStrategy = { _ in 0.1 },
        shouldRetry: ShouldRetryHandler? = nil
    ) {
        precondition(maximumAttempts >= 1, "maximumAttempts must be at least 1")
        self.maximumAttempts = maximumAttempts
        self.backoffStrategy = backoffStrategy
        self.shouldRetry = shouldRetry ?? SQLServerRetryConfiguration.defaultShouldRetry
    }
}

extension SQLServerRetryConfiguration: Sendable {}

extension TimeInterval {
    internal var nioTimeAmount: NIOCore.TimeAmount {
        .nanoseconds(Int64(self * 1_000_000_000))
    }
}
