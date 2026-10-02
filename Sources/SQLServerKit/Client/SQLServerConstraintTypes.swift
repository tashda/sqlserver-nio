import Foundation

// MARK: - Constraint Types

public struct ForeignKeyOptions: Sendable {
    public let onDelete: ReferentialAction
    public let onUpdate: ReferentialAction
    /// When false, existing rows are not checked (`WITH NOCHECK`) and SQL Server marks the key not trusted.
    public let checkExisting: Bool
    /// Creates the key not trusted, which is the same as `checkExisting: false`.
    public let isNotTrusted: Bool
    /// `NOT FOR REPLICATION`: replication agents' changes are not checked.
    public let notForReplication: Bool

    public init(
        onDelete: ReferentialAction = .noAction,
        onUpdate: ReferentialAction = .noAction,
        checkExisting: Bool = true,
        isNotTrusted: Bool = false,
        notForReplication: Bool = false
    ) {
        self.onDelete = onDelete
        self.onUpdate = onUpdate
        self.checkExisting = checkExisting
        self.isNotTrusted = isNotTrusted
        self.notForReplication = notForReplication
    }
    
    public enum ReferentialAction: String, Sendable {
        case noAction = "NO ACTION"
        case cascade = "CASCADE"
        case setNull = "SET NULL"
        case setDefault = "SET DEFAULT"
    }
}

public struct ConstraintInfo: Sendable {
    public let name: String
    public let type: ConstraintType
    public let tableName: String
    public let schemaName: String
    public let columns: [String]
    public let definition: String?
    public let referencedTable: String?
    public let referencedColumns: [String]?
    public let deleteAction: String?
    public let updateAction: String?
    
    public enum ConstraintType: String, Sendable {
        case primaryKey = "PRIMARY KEY"
        case foreignKey = "FOREIGN KEY"
        case unique = "UNIQUE"
        case check = "CHECK"
        case `default` = "DEFAULT"
    }
}
