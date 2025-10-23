import NIOCore

public struct SQLServerRetryConfiguration {
    public typealias ShouldRetryHandler = @Sendable (Swift.Error) -> Bool
    public typealias BackoffStrategy = @Sendable (_ attempt: Int) -> TimeAmount

    public var maximumAttempts: Int
    public var backoffStrategy: BackoffStrategy
    public var shouldRetry: ShouldRetryHandler

    public init(
        maximumAttempts: Int = 3,
        backoffStrategy: @escaping BackoffStrategy = { _ in .milliseconds(100) },
        shouldRetry: @escaping ShouldRetryHandler = { _ in true }
    ) {
        precondition(maximumAttempts >= 1, "maximumAttempts must be at least 1")
        self.maximumAttempts = maximumAttempts
        self.backoffStrategy = backoffStrategy
        self.shouldRetry = shouldRetry
    }
}

extension SQLServerRetryConfiguration: Sendable {}
