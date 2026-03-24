import Foundation

// MARK: - Dynamic Data Masking Types

/// Describes a mask function applied to a column.
public enum MaskFunction: Sendable, Hashable {
    case defaultMask
    case email
    case random(start: Int, end: Int)
    case partial(prefix: Int, padding: String, suffix: Int)
    case datetime(part: String)

    /// The T-SQL expression used in `ALTER COLUMN ... ADD MASKED WITH (FUNCTION = '...')`.
    public var sqlExpression: String {
        switch self {
        case .defaultMask:
            return "default()"
        case .email:
            return "email()"
        case .random(let start, let end):
            return "random(\(start), \(end))"
        case .partial(let prefix, let padding, let suffix):
            let escapedPadding = padding.replacingOccurrences(of: "'", with: "''")
            return "partial(\(prefix), '\(escapedPadding)', \(suffix))"
        case .datetime(let part):
            let escapedPart = part.replacingOccurrences(of: "'", with: "''")
            return "datetime('\(escapedPart)')"
        }
    }

    /// Attempts to parse a mask function string returned by SQL Server (e.g. `"partial(2, \"XXX\", 1)"`).
    public static func parse(_ value: String) -> MaskFunction? {
        let trimmed = value.trimmingCharacters(in: .whitespaces)
        if trimmed.hasPrefix("default") { return .defaultMask }
        if trimmed.hasPrefix("email") { return .email }
        if trimmed.hasPrefix("random") {
            let inner = extractParenContents(trimmed)
            let parts = inner.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
            guard parts.count == 2, let s = Int(parts[0]), let e = Int(parts[1]) else { return nil }
            return .random(start: s, end: e)
        }
        if trimmed.hasPrefix("partial") {
            let inner = extractParenContents(trimmed)
            let parts = inner.split(separator: ",", maxSplits: 2).map { $0.trimmingCharacters(in: .whitespaces) }
            guard parts.count == 3, let p = Int(parts[0]), let s = Int(parts[2]) else { return nil }
            let pad = parts[1].trimmingCharacters(in: CharacterSet(charactersIn: "\"' "))
            return .partial(prefix: p, padding: pad, suffix: s)
        }
        if trimmed.hasPrefix("datetime") {
            let inner = extractParenContents(trimmed)
            let part = inner.trimmingCharacters(in: CharacterSet(charactersIn: "\"' "))
            return .datetime(part: part)
        }
        return nil
    }

    private static func extractParenContents(_ value: String) -> String {
        guard let openIdx = value.firstIndex(of: "("),
              let closeIdx = value.lastIndex(of: ")") else { return "" }
        return String(value[value.index(after: openIdx)..<closeIdx])
    }
}

/// Information about a masked column from `sys.masked_columns`.
public struct MaskedColumnInfo: Sendable, Hashable, Identifiable {
    public var id: String { "\(schema).\(table).\(column)" }
    public let schema: String
    public let table: String
    public let column: String
    public let maskingFunction: String

    public init(schema: String, table: String, column: String, maskingFunction: String) {
        self.schema = schema
        self.table = table
        self.column = column
        self.maskingFunction = maskingFunction
    }
}
