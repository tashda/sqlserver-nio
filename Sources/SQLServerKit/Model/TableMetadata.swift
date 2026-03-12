import Foundation

public struct TableMetadata: Sendable {
    public let schema: String
    public let name: String
    /// SQL Server object type e.g. USER_TABLE, VIEW, TABLE_TYPE.
    public let type: String
    /// True when SQL Server marks the object as system-shipped.
    public let isSystemObject: Bool
    /// Optional MS_Description extended property for this table/view/type
    public let comment: String?

    public enum Kind: String, Sendable {
        case table
        case view
        case systemTable
        case tableType
        case other
    }

    public init(schema: String, name: String, type: String, isSystemObject: Bool, comment: String? = nil) {
        self.schema = schema
        self.name = name
        self.type = type
        self.isSystemObject = isSystemObject
        self.comment = comment
    }

    public var kind: Kind {
        let normalized = type
            .trimmingCharacters(in: .whitespacesAndNewlines.union(.controlCharacters))
            .uppercased()
        if normalized.contains("VIEW") {
            return .view
        }
        if normalized.contains("TABLE TYPE") {
            return .tableType
        }
        if normalized.contains("SYSTEM") {
            return .systemTable
        }
        if normalized.contains("TABLE") {
            return .table
        }
        return .other
    }

    public var isView: Bool {
        kind == .view
    }
}
