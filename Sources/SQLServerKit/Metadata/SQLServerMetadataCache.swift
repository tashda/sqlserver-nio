import Logging
import NIO
import NIOConcurrencyHelpers

/// Simple result cache keyed by string identifiers.
final class MetadataCache<Value: Sendable>: Sendable {
    private let storage: NIOLockedValueBox<[String: Value]>
    private let logger: Logger?

    init(logger: Logger? = nil) {
        self.storage = NIOLockedValueBox([:])
        self.logger = logger
    }

    func value(forKey key: String) -> Value? {
        let result = storage.withLockedValue { $0[key] }
        if result != nil {
            logger?.debug("Metadata cache hit: \(key)")
        } else {
            logger?.debug("Metadata cache miss: \(key)")
        }
        return result
    }

    func setValue(_ value: Value, forKey key: String) {
        storage.withLockedValue { $0[key] = value }
    }
}
