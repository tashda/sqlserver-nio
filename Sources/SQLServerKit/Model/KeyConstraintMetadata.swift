import Foundation

public struct KeyColumnMetadata: Sendable {
    public let column: String
    public let ordinal: Int
    public let isDescending: Bool

    public init(column: String, ordinal: Int, isDescending: Bool) {
        self.column = column
        self.ordinal = ordinal
        self.isDescending = isDescending
    }
}

public struct KeyConstraintMetadata: Sendable {
    public enum ConstraintType: String, Sendable {
        case primaryKey = "PRIMARY_KEY"
        case unique = "UNIQUE"
    }

    public let schema: String
    public let table: String
    public let name: String
    public let type: ConstraintType
    public let isClustered: Bool
    public let columns: [KeyColumnMetadata]

    public init(schema: String, table: String, name: String, type: ConstraintType, isClustered: Bool, columns: [KeyColumnMetadata]) {
        self.schema = schema
        self.table = table
        self.name = name
        self.type = type
        self.isClustered = isClustered
        self.columns = columns
    }
}
