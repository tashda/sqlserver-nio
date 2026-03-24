import Foundation

/// Information about a system-versioned (temporal) table.
public struct TemporalTableInfo: Sendable, Equatable {
    /// Schema of the system-versioned table.
    public let schema: String
    /// Name of the system-versioned table.
    public let name: String
    /// Schema of the linked history table.
    public let historySchema: String
    /// Name of the linked history table.
    public let historyTable: String
    /// Name of the period start column (e.g. `ValidFrom`).
    public let periodStartColumn: String
    /// Name of the period end column (e.g. `ValidTo`).
    public let periodEndColumn: String

    public init(
        schema: String,
        name: String,
        historySchema: String,
        historyTable: String,
        periodStartColumn: String,
        periodEndColumn: String
    ) {
        self.schema = schema
        self.name = name
        self.historySchema = historySchema
        self.historyTable = historyTable
        self.periodStartColumn = periodStartColumn
        self.periodEndColumn = periodEndColumn
    }
}
