import Foundation

/// Result of executing multiple pre-split batches sequentially on a single connection.
public struct BatchExecutionResult: Sendable {
    public let batchResults: [SingleBatchResult]

    public init(batchResults: [SingleBatchResult]) {
        self.batchResults = batchResults
    }

    public struct SingleBatchResult: Sendable {
        public let batchIndex: Int
        public let result: SQLServerExecutionResult?
        public let error: (any Error)?

        public init(batchIndex: Int, result: SQLServerExecutionResult?, error: (any Error)? = nil) {
            self.batchIndex = batchIndex
            self.result = result
            self.error = error
        }

        public var succeeded: Bool { error == nil }
    }
}

/// Events emitted during streaming multi-batch execution.
public enum BatchStreamEvent: Sendable {
    case batchStarted(index: Int)
    case batchEvent(index: Int, event: SQLServerStreamEvent)
    case batchCompleted(index: Int)
    case batchFailed(index: Int, error: any Error, messages: [SQLServerStreamMessage])
}
