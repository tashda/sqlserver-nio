import NIO
import NIOConcurrencyHelpers

/// Simple result cache keyed by string identifiers.
final class MetadataCache<Value: Sendable>: Sendable {
    private let storage: NIOLockedValueBox<[String: Value]>

    init() {
        self.storage = NIOLockedValueBox([:])
    }

    func value(forKey key: String) -> Value? {
        storage.withLockedValue { $0[key] }
    }

    func setValue(_ value: Value, forKey key: String) {
        storage.withLockedValue { $0[key] = value }
    }
}
