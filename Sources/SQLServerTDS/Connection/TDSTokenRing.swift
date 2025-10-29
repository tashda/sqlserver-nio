import Foundation
import NIOConcurrencyHelpers

final class TDSTokenRing {
    private let capacity: Int
    private var buffer: [String]
    private var index: Int = 0
    private let lock = NIOLock()

    init(capacity: Int = 128) {
        self.capacity = max(8, capacity)
        self.buffer = Array(repeating: "", count: self.capacity)
    }

    func record(_ event: String) {
        lock.withLockVoid {
            buffer[index] = "\(timestamp()) \(event)"
            index = (index + 1) % capacity
        }
    }

    func snapshot() -> [String] {
        return lock.withLock {
            var out: [String] = []
            out.reserveCapacity(capacity)
            // Dump starting from current index to preserve chronological order
            var i = index
            for _ in 0..<capacity {
                let line = buffer[i]
                if !line.isEmpty { out.append(line) }
                i = (i + 1) % capacity
            }
            return out
        }
    }

    private func timestamp() -> String {
        let now = Date()
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: now)
    }
}

