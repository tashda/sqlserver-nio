import Foundation
import NIO
import NIOConcurrencyHelpers
import Logging

enum PromiseTracker {
    private struct Entry { let label: String; let backtrace: [String] }
    
    private struct State {
        var nextID: Int = 1
        var pending: [Int: Entry] = [:]
    }
    
    private static let state = NIOLockedValueBox(State())
    private static let logger = Logger(label: "tds.promise.tracker")

    static func makeTrackedPromise<T>(on eventLoop: EventLoop, label: String) -> EventLoopPromise<T> {
        let promise = eventLoop.makePromise(of: T.self)
        track(promise, label: label)
        return promise
    }

    static func track<T>(_ promise: EventLoopPromise<T>, label: String) {
        let backtrace: [String] = Thread.callStackSymbols
        let id = state.withLockedValue { boxState -> Int in
            let id = boxState.nextID
            boxState.pending[id] = PromiseTracker.Entry(label: label, backtrace: backtrace)
            boxState.nextID += 1
            return id
        }

        let clearEntry: @Sendable () -> Void = {
            _ = PromiseTracker.state.withLockedValue { $0.pending.removeValue(forKey: id) }
        }
        promise.futureResult.whenSuccess { (_: T) in clearEntry() }
        promise.futureResult.whenFailure { (_: Error) in clearEntry() }
    }

    static func log() {
        dumpUnresolved(context: "Manual Log Trigger")
    }

    static func dumpUnresolved(context: String) {
        state.withLockedValue { boxState in
            guard !boxState.pending.isEmpty else {
                logger.info("[\(context)] No pending promises")
                return
            }

            logger.warning("[\(context)] Found \(boxState.pending.count) pending promises:")
            for (id, entry) in boxState.pending {
                logger.warning("  [\(id)] \(entry.label)")
                // For brevity, only first 5 symbols of backtrace
                for symbol in entry.backtrace.prefix(5) {
                    logger.warning("    \(symbol)")
                }
            }
        }
    }
}
