import Foundation

/// Represents column information for SQL Server metadata queries
public struct ColumnInfo: Sendable {
    public let name: String
    public let dataType: String
    public let isPrimaryKey: Bool
    public let isNullable: Bool
    public let maxLength: Int?

    public init(
        name: String,
        dataType: String,
        isPrimaryKey: Bool = false,
        isNullable: Bool = false,
        maxLength: Int? = nil
    ) {
        self.name = name
        self.dataType = dataType
        self.isPrimaryKey = isPrimaryKey
        self.isNullable = isNullable
        self.maxLength = maxLength
    }
}