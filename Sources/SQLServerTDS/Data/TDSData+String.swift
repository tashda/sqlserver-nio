import Foundation
import NIOCore

extension TDSData {
    public init(string: String) {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf8.count)
        buffer.writeString(string)
        self.init(metadata: String.tdsMetadata, value: buffer)
    }

    public var string: String? {
        if self.metadata.dataType == .sqlVariant {
            if let resolved = self.sqlVariantResolved(), let string = resolved.string {
                return string
            }

            if var payload = self.value, payload.readableBytes >= 2 {
                _ = payload.readInteger(as: UInt8.self)
                if let propertyLength = payload.readInteger(as: UInt8.self) {
                    if propertyLength > 0, payload.readableBytes >= Int(propertyLength) {
                        _ = payload.readSlice(length: Int(propertyLength))
                    }
                    let remaining = payload.readableBytes
                    if remaining > 0 {
                        if remaining.isMultiple(of: 2), let utf16 = payload.readUTF16String(length: remaining) {
                            return utf16
                        }
                        if let bytes = payload.readBytes(length: remaining), let utf8 = String(bytes: bytes, encoding: .utf8) {
                            return utf8
                        }
                    }
                }
            }

            return nil
        }

        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .bit, .bitn:
            return self.bool.map { $0 ? "1" : "0" }
        case .tinyInt, .smallInt, .int, .bigInt, .intn:
            if let intValue = self.int64 {
                return String(intValue)
            }
            if let uintValue = self.uint64 {
                return String(uintValue)
            }
            return nil
        case .real:
            return self.double.map { String($0) }
        case .float, .floatn:
            return self.double.map { String($0) }
        case .numeric, .numericLegacy, .decimal, .decimalLegacy:
            if let decimalValue = self.decimal {
                return NSDecimalNumber(decimal: decimalValue).stringValue
            }
            return self.double.map { String($0) }
        case .smallMoney, .money, .moneyn:
            return self.double.map { String($0) }
        case .charLegacy, .varcharLegacy, .char, .varchar, .text:
            guard let bytes = value.readBytes(length: value.readableBytes) else {
                return nil
            }
            if bytes.isEmpty {
                return ""
            }
            if let utf8 = String(bytes: bytes, encoding: .utf8) {
                return utf8
            }
            return String(decoding: bytes, as: UTF8.self)
        case .nchar, .nvarchar, .nText:
            guard value.readableBytes.isMultiple(of: 2) else {
                return nil
            }
            return value.readUTF16String(length: value.readableBytes)
        case .guid:
            guard value.readableBytes == 16,
                  let bytes = value.readBytes(length: 16) else {
                return nil
            }
            let hex: (UInt8) -> String = { String(format: "%02X", $0) }
            let d1 = [bytes[3], bytes[2], bytes[1], bytes[0]].map(hex).joined()
            let d2 = [bytes[5], bytes[4]].map(hex).joined()
            let d3 = [bytes[7], bytes[6]].map(hex).joined()
            let d4a = [bytes[8], bytes[9]].map(hex).joined()
            let d4b = bytes[10...15].map(hex).joined()
            return "\(d1)-\(d2)-\(d3)-\(d4a)-\(d4b)"
        case .smallDateTime, .datetime, .date, .datetime2, .datetimeOffset, .datetimen:
            return self.date.map { ISO8601DateFormatter().string(from: $0) }
        case .time:
            // time-only cannot be represented as a Date; format the raw ticks directly
            let scale = Int(self.metadata.scale)
            guard let rawTicks: Int = value.readByteLengthInteger(length: value.readableBytes) else {
                return nil
            }
            var ticks = rawTicks
            if scale < 7 {
                for _ in scale..<7 { ticks *= 10 }
            }
            let totalSeconds = Double(ticks) / 10_000_000.0
            let hours = Int(totalSeconds) / 3600
            let minutes = (Int(totalSeconds) % 3600) / 60
            let seconds = Int(totalSeconds) % 60
            let fractional = totalSeconds - Double(Int(totalSeconds))
            if fractional > 0 && scale > 0 {
                let frac = String(format: "%.\(scale)f", fractional).dropFirst(1) // drop leading "0"
                return String(format: "%02d:%02d:%02d%@", hours, minutes, seconds, String(frac))
            }
            return String(format: "%02d:%02d:%02d", hours, minutes, seconds)
        case .binaryLegacy, .varbinaryLegacy, .varbinary, .binary, .image:
            return self.bytes.map { $0.map { String(format: "0x%02X", $0) }.joined() }
        case .xml, .json:
            // xml and json are stored as UTF-16LE on the wire
            guard value.readableBytes.isMultiple(of: 2) else {
                return nil
            }
            return value.readUTF16String(length: value.readableBytes)
        case .vector:
            return nil
        case .clrUdt:
            return self.bytes.map { bytes in
                "0x" + bytes.map { String(format: "%02X", $0) }.joined()
            }
        case .null:
            return nil
        case .sqlVariant:
            // handled above before the switch
            return nil
        }
    }
}

extension String: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        TypeMetadata(dataType: .varchar)
    }

    public init?(tdsData: TDSData) {
        guard let string = tdsData.string else {
            return nil
        }
        self = string
    }

    public var tdsData: TDSData? {
        .init(string: self)
    }
}

extension TDSData: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(string: value)
    }
}
