/// Space and statistics information for a user table.
public struct SQLServerTableStat: Sendable {
    public let schemaName: String
    public let tableName: String
    /// "Heap" when the table has no clustered index, otherwise "Clustered".
    public let tableType: String
    public let rowCount: Int64
    public let dataSpaceKB: Int64
    public let indexSpaceKB: Int64
    public let unusedSpaceKB: Int64
    public let totalSpaceKB: Int64
    /// ISO 8601 timestamp of the most recent statistics update, if available.
    public let lastStatsUpdate: String?

    public init(
        schemaName: String,
        tableName: String,
        tableType: String,
        rowCount: Int64,
        dataSpaceKB: Int64,
        indexSpaceKB: Int64,
        unusedSpaceKB: Int64,
        totalSpaceKB: Int64,
        lastStatsUpdate: String? = nil
    ) {
        self.schemaName = schemaName
        self.tableName = tableName
        self.tableType = tableType
        self.rowCount = rowCount
        self.dataSpaceKB = dataSpaceKB
        self.indexSpaceKB = indexSpaceKB
        self.unusedSpaceKB = unusedSpaceKB
        self.totalSpaceKB = totalSpaceKB
        self.lastStatsUpdate = lastStatsUpdate
    }
}
