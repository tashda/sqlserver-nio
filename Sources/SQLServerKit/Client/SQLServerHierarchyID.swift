struct SQLServerHierarchyID {
    static func string(from bytes: [UInt8]) -> String? {
        if bytes.isEmpty {
            return "/"
        }

        var reader = BitReader(bytes: bytes)
        var steps: [[Int64]] = []

        while true {
            var step: [Int64] = []

            while true {
                guard let pattern = KnownPatterns.pattern(for: reader) else {
                    return step.isEmpty ? format(steps) : nil
                }

                let encodedValue = reader.read(pattern.bitLength)
                let (value, isLast) = pattern.decode(encodedValue)
                step.append(value)

                if isLast {
                    break
                }
            }

            steps.append(step)
        }
    }

    private static func format(_ steps: [[Int64]]) -> String {
        if steps.isEmpty {
            return "/"
        }

        let path = steps
            .map { $0.map(String.init).joined(separator: ".") }
            .joined(separator: "/")
        return "/\(path)/"
    }
}

private extension SQLServerHierarchyID {
    struct BitPattern {
        let minValue: Int64
        let maxValue: Int64
        let patternOnes: UInt64
        let patternMask: UInt64
        let bitLength: Int
        let prefixOnes: UInt64
        let prefixBitLength: Int

        init(minValue: Int64, maxValue: Int64, pattern: String) {
            self.minValue = minValue
            self.maxValue = maxValue
            self.patternOnes = Self.bitMask(pattern) { $0 == "1" }
            self.patternMask = Self.bitMask(pattern) { $0 == "x" }
            self.bitLength = pattern.count

            let prefix = String(pattern.prefix { $0 != "x" })
            self.prefixOnes = Self.bitMask(prefix) { $0 == "1" }
            self.prefixBitLength = prefix.count
        }

        func contains(_ value: Int64) -> Bool {
            minValue...maxValue ~= value
        }

        func decode(_ encodedValue: UInt64) -> (value: Int64, isLast: Bool) {
            let decodedValue = Self.compress(encodedValue, using: patternMask)
            let isLast = (encodedValue & 0x1) == 0x1
            let value = (isLast ? Int64(decodedValue) : Int64(decodedValue) - 1) + minValue
            return (value, isLast)
        }

        private static func bitMask(_ pattern: String, predicate: (Character) -> Bool) -> UInt64 {
            pattern.reduce(into: UInt64.zero) { result, character in
                result = (result << 1) | (predicate(character) ? 1 : 0)
            }
        }

        private static func compress(_ value: UInt64, using mask: UInt64) -> UInt64 {
            if mask == 0 {
                return 0
            }

            if (mask & 0x1) > 0 {
                return (compress(value >> 1, using: mask >> 1) << 1) | (value & 0x1)
            }

            return compress(value >> 1, using: mask >> 1)
        }
    }

    struct KnownPatterns {
        static let positive: [BitPattern] = [
            .init(minValue: 0, maxValue: 3, pattern: "01xxT"),
            .init(minValue: 4, maxValue: 7, pattern: "100xxT"),
            .init(minValue: 8, maxValue: 15, pattern: "101xxxT"),
            .init(minValue: 16, maxValue: 79, pattern: "110xx0x1xxxT"),
            .init(minValue: 80, maxValue: 1103, pattern: "1110xxx0xxx0x1xxxT"),
            .init(minValue: 1104, maxValue: 5199, pattern: "11110xxxxx0xxx0x1xxxT"),
            .init(minValue: 5200, maxValue: 4_294_972_495, pattern: "111110xxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT"),
            .init(minValue: 4_294_972_496, maxValue: 281_479_271_683_151, pattern: "111111xxxxxxxxxxxxxx0xxxxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT"),
        ]

        static let negative: [BitPattern] = [
            .init(minValue: -8, maxValue: -1, pattern: "00111xxxT"),
            .init(minValue: -72, maxValue: -9, pattern: "0010xx0x1xxxT"),
            .init(minValue: -4_168, maxValue: -73, pattern: "000110xxxxx0xxx0x1xxxT"),
            .init(minValue: -4_294_971_464, maxValue: -4_169, pattern: "000101xxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT"),
            .init(minValue: -281_479_271_682_120, maxValue: -4_294_971_465, pattern: "000100xxxxxxxxxxxxxx0xxxxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT"),
        ]

        static func pattern(for reader: BitReader) -> BitPattern? {
            let remaining = reader.remaining
            if remaining == 0 {
                return nil
            }

            if remaining < 8, reader.peek(remaining) == 0 {
                return nil
            }

            let candidates = reader.peek(2) == 0 ? negative : positive
            for pattern in candidates {
                if pattern.bitLength > remaining {
                    break
                }

                if pattern.prefixOnes == reader.peek(pattern.prefixBitLength) {
                    return pattern
                }
            }

            return nil
        }
    }

    struct BitReader {
        private let bytes: [UInt8]
        private(set) var bitPosition = 0

        init(bytes: [UInt8]) {
            self.bytes = bytes
        }

        var remaining: Int {
            bytes.count * 8 - bitPosition
        }

        mutating func read(_ bitCount: Int) -> UInt64 {
            let result = peek(bitCount)
            bitPosition += bitCount
            return result
        }

        func peek(_ bitCount: Int) -> UInt64 {
            if bitCount == 0 {
                return 0
            }

            let currentByte = bitPosition / 8
            let finalByte = (bitPosition + bitCount - 1) / 8

            if currentByte == finalByte {
                let offset = (8 - (bitPosition % 8)) - bitCount
                let mask = (UInt64(0xFF) >> (8 - bitCount)) << offset
                return (UInt64(bytes[currentByte]) & mask) >> offset
            }

            var result: UInt64 = 0
            let startOffset = bitPosition % 8
            let firstCompleteByte = startOffset == 0 ? currentByte : currentByte + 1
            let endOffset = (bitPosition + bitCount) % 8
            let lastCompleteByte = endOffset == 0 ? finalByte + 1 : finalByte

            if startOffset > 0 {
                let startMask = UInt64(0xFF) >> startOffset
                result = UInt64(bytes[currentByte]) & startMask
            }

            if firstCompleteByte < lastCompleteByte {
                for index in firstCompleteByte..<lastCompleteByte {
                    result = (result << 8) | UInt64(bytes[index])
                }
            }

            if endOffset > 0 {
                let endMask = (UInt64(0xFF) >> (8 - endOffset)) << (8 - endOffset)
                result = (result << endOffset) | ((UInt64(bytes[finalByte]) & endMask) >> (8 - endOffset))
            }

            return result
        }
    }
}
