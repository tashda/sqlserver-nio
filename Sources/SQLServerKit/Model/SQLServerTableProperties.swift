import Foundation

/// Space usage and metadata for a SQL Server table.
public struct SQLServerTableProperties: Sendable {
    // MARK: - Space Usage
    public let rowCount: Int64
    public let reservedKB: Int64
    public let dataKB: Int64
    public let indexKB: Int64
    public let unusedKB: Int64

    // MARK: - Dates
    public let createDate: Date?
    public let modifyDate: Date?

    // MARK: - Storage & Configuration
    public let dataCompression: String?
    public let filegroup: String?
    public let lockEscalation: String?
    public let textFilegroup: String?
    public let filestreamFilegroup: String?

    // MARK: - Object Info
    public let isSystemObject: Bool?
    public let usesAnsiNulls: Bool?
    public let isReplicated: Bool?

    // MARK: - Partitioning
    public let isPartitioned: Bool?
    public let partitionScheme: String?
    public let partitionColumn: String?
    public let partitionCount: Int?

    // MARK: - Temporal / System Versioning
    public let isSystemVersioned: Bool?
    public let historyTableSchema: String?
    public let historyTableName: String?
    public let periodStartColumn: String?
    public let periodEndColumn: String?

    // MARK: - In-Memory OLTP
    public let isMemoryOptimized: Bool?
    public let memoryOptimizedDurability: String?

    // MARK: - Change Tracking
    public let changeTrackingEnabled: Bool?
    public let trackColumnsUpdated: Bool?

    public init(
        rowCount: Int64,
        reservedKB: Int64,
        dataKB: Int64,
        indexKB: Int64,
        unusedKB: Int64,
        createDate: Date? = nil,
        modifyDate: Date? = nil,
        dataCompression: String? = nil,
        filegroup: String? = nil,
        lockEscalation: String? = nil,
        textFilegroup: String? = nil,
        filestreamFilegroup: String? = nil,
        isSystemObject: Bool? = nil,
        usesAnsiNulls: Bool? = nil,
        isReplicated: Bool? = nil,
        isPartitioned: Bool? = nil,
        partitionScheme: String? = nil,
        partitionColumn: String? = nil,
        partitionCount: Int? = nil,
        isSystemVersioned: Bool? = nil,
        historyTableSchema: String? = nil,
        historyTableName: String? = nil,
        periodStartColumn: String? = nil,
        periodEndColumn: String? = nil,
        isMemoryOptimized: Bool? = nil,
        memoryOptimizedDurability: String? = nil,
        changeTrackingEnabled: Bool? = nil,
        trackColumnsUpdated: Bool? = nil
    ) {
        self.rowCount = rowCount
        self.reservedKB = reservedKB
        self.dataKB = dataKB
        self.indexKB = indexKB
        self.unusedKB = unusedKB
        self.createDate = createDate
        self.modifyDate = modifyDate
        self.dataCompression = dataCompression
        self.filegroup = filegroup
        self.lockEscalation = lockEscalation
        self.textFilegroup = textFilegroup
        self.filestreamFilegroup = filestreamFilegroup
        self.isSystemObject = isSystemObject
        self.usesAnsiNulls = usesAnsiNulls
        self.isReplicated = isReplicated
        self.isPartitioned = isPartitioned
        self.partitionScheme = partitionScheme
        self.partitionColumn = partitionColumn
        self.partitionCount = partitionCount
        self.isSystemVersioned = isSystemVersioned
        self.historyTableSchema = historyTableSchema
        self.historyTableName = historyTableName
        self.periodStartColumn = periodStartColumn
        self.periodEndColumn = periodEndColumn
        self.isMemoryOptimized = isMemoryOptimized
        self.memoryOptimizedDurability = memoryOptimizedDurability
        self.changeTrackingEnabled = changeTrackingEnabled
        self.trackColumnsUpdated = trackColumnsUpdated
    }
}
