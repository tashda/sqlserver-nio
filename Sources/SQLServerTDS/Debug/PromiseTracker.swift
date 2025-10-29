import Foundation
import NIO
import Logging
import NIOConcurrencyHelpers

// Lightweight tracker to help locate leaking EventLoopPromises during tests.
enum PromiseTracker {
    private struct Entry { let label: String; let backtrace: [String] }
    private static var lock = NIOLock()
    private static var nextID: Int = 1
    private static var pending: [Int: Entry] = [:]
    private static let logger = Logger(label: "tds.promise.tracker")

    static func makeTrackedPromise<T>(on loop: EventLoop, label: String) -> EventLoopPromise<T> {
        let p: EventLoopPromise<T> = loop.makePromise()
        let id: Int = lock.withLock { () -> Int in
            let current = nextID
            nextID &+= 1
            return current
        }
        let entry = Entry(label: label, backtrace: Thread.callStackSymbols)
        lock.withLock { pending[id] = entry }
        p.futureResult.whenComplete { _ in
            lock.withLock {
                _ = pending.removeValue(forKey: id)
            }
        }
        logger.debug("makePromise label=\(label) loop=\(loop)")
        return p
    }

    static func dumpUnresolved(context: String) {
        let snapshot: [Entry] = lock.withLock { Array(pending.values) }
        guard !snapshot.isEmpty else { return }
        logger.error("Unresolved EventLoopPromises at \(context): count=\(snapshot.count)")
        for e in snapshot.prefix(16) {
            logger.error("label=\(e.label) backtrace=\n\(e.backtrace.joined(separator: "\n"))")
        }
    }
}
