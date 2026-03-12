import Foundation

public struct ForeignKeyColumnMetadata: Sendable {
    public let parentColumn: String
    public let referencedColumn: String
    public let ordinal: Int

    public init(parentColumn: String, referencedColumn: String, ordinal: Int) {
        self.parentColumn = parentColumn
        self.referencedColumn = referencedColumn
        self.ordinal = ordinal
    }
}

public struct ForeignKeyMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let referencedSchema: String
    public let referencedTable: String
    public let deleteAction: String
    public let updateAction: String
    public let columns: [ForeignKeyColumnMetadata]

    public init(
        schema: String,
        table: String,
        name: String,
        referencedSchema: String,
        referencedTable: String,
        deleteAction: String,
        updateAction: String,
        columns: [ForeignKeyColumnMetadata]
    ) {
        self.schema = schema
        self.table = table
        self.name = name
        self.referencedSchema = referencedSchema
        self.referencedTable = referencedTable
        self.deleteAction = deleteAction
        self.updateAction = updateAction
        self.columns = columns
    }
}

extension ForeignKeyMetadata {
    public static func mapAction(_ code: Int) -> String {
        switch code {
        case 0: return "NO ACTION"
        case 1: return "CASCADE"
        case 2: return "SET NULL"
        case 3: return "SET DEFAULT"
        default: return "NO ACTION"
        }
    }
}
