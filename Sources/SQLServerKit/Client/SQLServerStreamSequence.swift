import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

// MARK: - Back-pressure strategy

/// Adaptive buffer that dynamically adjusts its target size based on consumption rate.
/// When the consumer keeps up, the target grows (up to `maximum`).
/// When the consumer lags, the target shrinks (down to `minimum`).
/// Modeled after postgres-nio's AdaptiveRowBuffer.
struct AdaptiveRowBuffer: NIOAsyncSequenceProducerBackPressureStrategy, Sendable {
    static let defaultMinimum = 1
    static let defaultMaximum = 16384
    static let defaultTarget = 256

    let minimum: Int
    let maximum: Int

    private var target: Int
    private var canShrink: Bool = false

    init(minimum: Int = Self.defaultMinimum, maximum: Int = Self.defaultMaximum, target: Int = Self.defaultTarget) {
        precondition(minimum <= target && target <= maximum)
        self.minimum = minimum
        self.maximum = maximum
        self.target = target
    }

    mutating func didYield(bufferDepth: Int) -> Bool {
        if bufferDepth > target, canShrink, target > minimum {
            target &>>= 1
        }
        canShrink = true
        return false
    }

    mutating func didConsume(bufferDepth: Int) -> Bool {
        if bufferDepth == 0, target < maximum {
            target &*= 2
            canShrink = false
        }
        return bufferDepth < target
    }
}

// MARK: - Delegate

/// Bridges NIO back-pressure demand signals to TDSConnection's read control.
/// When the consumer needs more data, `produceMore()` triggers a channel read.
/// When the stream terminates early (consumer cancelled), sends ATTENTION to
/// cancel the server-side query and restores auto-read.
final class SQLServerStreamDelegate: NIOAsyncSequenceProducerDelegate, @unchecked Sendable {
    private let connection: TDSConnection
    /// Set to true when `source.finish()` is called (normal completion).
    /// If still false when `didTerminate()` fires, the consumer cancelled early.
    private let finished: NIOLockedValueBox<Bool>

    init(connection: TDSConnection) {
        self.connection = connection
        self.finished = NIOLockedValueBox(false)
    }

    func markFinished() {
        finished.withLockedValue { $0 = true }
    }

    func produceMore() {
        connection.eventLoop.execute {
            self.connection.requestRead()
        }
    }

    func didTerminate() {
        let wasFinished = finished.withLockedValue { $0 }
        if !wasFinished {
            // Consumer cancelled before the query completed — send ATTENTION
            // to abort the server-side operation.
            connection.sendAttention()
        }
        connection.eventLoop.execute {
            self.connection.resumeAutoRead()
        }
    }
}

// MARK: - Public sequence type

/// A back-pressure-aware async sequence of `SQLServerStreamEvent` values.
///
/// Uses `NIOThrowingAsyncSequenceProducer` with an adaptive buffer strategy
/// to prevent unbounded memory growth and ensure the consumer naturally suspends
/// when the buffer drains, releasing whatever actor it's running on.
@available(macOS 12.0, *)
public struct SQLServerStreamSequence: AsyncSequence, Sendable {
    public typealias Element = SQLServerStreamEvent

    typealias BackingSequence = NIOThrowingAsyncSequenceProducer<
        SQLServerStreamEvent,
        any Error,
        AdaptiveRowBuffer,
        SQLServerStreamDelegate
    >

    let backing: BackingSequence

    init(_ backing: BackingSequence) {
        self.backing = backing
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(backing: backing.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        var backing: BackingSequence.AsyncIterator

        public mutating func next() async throws -> SQLServerStreamEvent? {
            try await backing.next()
        }
    }
}
