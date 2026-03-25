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

    // MARK: - Temporal Table Properties

    /// Temporal type from `sys.tables.temporal_type`:
    /// 0 = non-temporal, 1 = history table, 2 = system-versioned table.
    public let temporalType: Int

    /// Schema of the linked history table (when `temporalType == 2`).
    public let historyTableSchema: String?

    /// Name of the linked history table (when `temporalType == 2`).
    public let historyTableName: String?

    /// Name of the period start column (e.g. `ValidFrom`).
    public let periodStartColumn: String?

    /// Name of the period end column (e.g. `ValidTo`).
    public let periodEndColumn: String?

    // MARK: - In-Memory OLTP Properties

    /// True when this table is memory-optimized (Hekaton / In-Memory OLTP).
    public let isMemoryOptimized: Bool

    /// Durability description for memory-optimized tables:
    /// `SCHEMA_AND_DATA` or `SCHEMA_ONLY`. Nil for disk-based tables.
    public let durabilityDescription: String?

    public enum Kind: String, Sendable {
        case table
        case view
        case systemTable
        case tableType
        case other
    }

    public init(
        schema: String,
        name: String,
        type: String,
        isSystemObject: Bool,
        comment: String? = nil,
        temporalType: Int = 0,
        historyTableSchema: String? = nil,
        historyTableName: String? = nil,
        periodStartColumn: String? = nil,
        periodEndColumn: String? = nil,
        isMemoryOptimized: Bool = false,
        durabilityDescription: String? = nil
    ) {
        self.schema = schema
        self.name = name
        self.type = type
        self.isSystemObject = isSystemObject
        self.comment = comment
        self.temporalType = temporalType
        self.historyTableSchema = historyTableSchema
        self.historyTableName = historyTableName
        self.periodStartColumn = periodStartColumn
        self.periodEndColumn = periodEndColumn
        self.isMemoryOptimized = isMemoryOptimized
        self.durabilityDescription = durabilityDescription
    }

    /// True when this table has system-versioning enabled (temporal type 2).
    public var isSystemVersioned: Bool { temporalType == 2 }

    /// True when this table is a history table for a system-versioned table.
    public var isHistoryTable: Bool { temporalType == 1 }

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
