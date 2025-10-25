import Foundation

/// Represents a literal that can be embedded directly into a SQL batch.
public enum SQLServerLiteralValue: Sendable {
    case null
    case string(String)
    case nString(String)
    case int(Int)
    case int64(Int64)
    case double(Double)
    case decimal(String)
    case bool(Bool)
    case date(Date)
    case uuid(UUID)
    case bytes([UInt8])
    case raw(String)
}

extension SQLServerLiteralValue {
    internal func sqlLiteral() -> String {
        switch self {
        case .null:
            return "NULL"
        case .string(let value):
            return "'\(Self.escape(value))'"
        case .nString(let value):
            return "N'\(Self.escape(value))'"
        case .int(let value):
            return "\(value)"
        case .int64(let value):
            return "\(value)"
        case .double(let value):
            return "\(value)"
        case .decimal(let text):
            return text
        case .bool(let flag):
            return flag ? "1" : "0"
        case .date(let date):
            return "'\(Self.dateFormatter.string(from: date))'"
        case .uuid(let uuid):
            return "'\(uuid.uuidString)'"
        case .bytes(let bytes):
            guard !bytes.isEmpty else { return "0x" }
            return "0x" + bytes.map { String(format: "%02X", $0) }.joined()
        case .raw(let fragment):
            return fragment
        }
    }
    
    private static func escape(_ text: String) -> String {
        text.replacingOccurrences(of: "'", with: "''")
    }
    
    private static let dateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withFullDate, .withFullTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter
    }()
}
