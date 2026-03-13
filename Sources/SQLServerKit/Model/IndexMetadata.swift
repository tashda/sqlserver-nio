import Foundation

public struct IndexColumnMetadata: Sendable {
    public let column: String
    public let ordinal: Int
    public let isDescending: Bool
    public let isIncluded: Bool

    public init(column: String, ordinal: Int, isDescending: Bool, isIncluded: Bool) {
        self.column = column
        self.ordinal = ordinal
        self.isDescending = isDescending
        self.isIncluded = isIncluded
    }
}

public struct IndexMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let isUnique: Bool
    public let isClustered: Bool
    public let isPrimaryKey: Bool
    public let isUniqueConstraint: Bool
    public let filterDefinition: String?
    public let columns: [IndexColumnMetadata]

    public init(
        schema: String,
        table: String,
        name: String,
        isUnique: Bool,
        isClustered: Bool,
        isPrimaryKey: Bool,
        isUniqueConstraint: Bool,
        filterDefinition: String? = nil,
        columns: [IndexColumnMetadata]
    ) {
        self.schema = schema
        self.table = table
        self.name = name
        self.isUnique = isUnique
        self.isClustered = isClustered
        self.isPrimaryKey = isPrimaryKey
        self.isUniqueConstraint = isUniqueConstraint
        self.filterDefinition = filterDefinition
        self.columns = columns
    }
}
