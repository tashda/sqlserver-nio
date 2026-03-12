import Foundation

// MARK: - Index Types

public struct IndexColumn: Sendable {
    public let name: String
    public let sortDirection: SortDirection
    public let isIncluded: Bool
    
    public init(name: String, sortDirection: SortDirection = .ascending, isIncluded: Bool = false) {
        self.name = name
        self.sortDirection = sortDirection
        self.isIncluded = isIncluded
    }
    
    public enum SortDirection: String, Sendable {
        case ascending = "ASC"
        case descending = "DESC"
    }
}

public struct IndexOptions: Sendable {
    public let fillFactor: Int?
    public let padIndex: Bool
    public let ignoreDuplicateKey: Bool
    public let statisticsNoRecompute: Bool
    public let allowRowLocks: Bool
    public let allowPageLocks: Bool
    public let online: Bool
    public let maxDop: Int?
    public let dataCompression: DataCompression?
    public let fileGroup: String?
    public let partitionScheme: String?
    public let partitionColumns: [String]
    
    public init(
        fillFactor: Int? = nil,
        padIndex: Bool = false,
        ignoreDuplicateKey: Bool = false,
        statisticsNoRecompute: Bool = false,
        allowRowLocks: Bool = true,
        allowPageLocks: Bool = true,
        online: Bool = false,
        maxDop: Int? = nil,
        dataCompression: DataCompression? = nil,
        fileGroup: String? = nil,
        partitionScheme: String? = nil,
        partitionColumns: [String] = []
    ) {
        self.fillFactor = fillFactor
        self.padIndex = padIndex
        self.ignoreDuplicateKey = ignoreDuplicateKey
        self.statisticsNoRecompute = statisticsNoRecompute
        self.allowRowLocks = allowRowLocks
        self.allowPageLocks = allowPageLocks
        self.online = online
        self.maxDop = maxDop
        self.dataCompression = dataCompression
        self.fileGroup = fileGroup
        self.partitionScheme = partitionScheme
        self.partitionColumns = partitionColumns
    }
    
    public enum DataCompression: String, Sendable {
        case none = "NONE"
        case row = "ROW"
        case page = "PAGE"
    }
}

public struct IndexInfo: Sendable {
    public let name: String
    public let tableName: String
    public let schemaName: String
    public let indexType: IndexType
    public let isUnique: Bool
    public let isPrimaryKey: Bool
    public let columns: [IndexColumnInfo]
    
    public enum IndexType: String, Sendable {
        case clustered = "CLUSTERED"
        case nonclustered = "NONCLUSTERED"
        case heap = "HEAP"
    }
}

public struct IndexColumnInfo: Sendable {
    public let name: String
    public let keyOrdinal: Int
    public let isDescending: Bool
    public let isIncluded: Bool
}
