import Foundation

public struct SQLServerTableStructure: Sendable {
    public let table: TableMetadata
    public let columns: [ColumnMetadata]
    public let primaryKey: KeyConstraintMetadata?

    public init(table: TableMetadata, columns: [ColumnMetadata], primaryKey: KeyConstraintMetadata? = nil) {
        self.table = table
        self.columns = columns
        self.primaryKey = primaryKey
    }
}

public struct SQLServerSchemaStructure: Sendable {
    public let name: String
    public let tables: [SQLServerTableStructure]
    public let views: [SQLServerTableStructure]
    public let functions: [RoutineMetadata]
    public let procedures: [RoutineMetadata]
    public let triggers: [TriggerMetadata]
    public let synonyms: [SynonymMetadata]

    public init(
        name: String,
        tables: [SQLServerTableStructure],
        views: [SQLServerTableStructure],
        functions: [RoutineMetadata],
        procedures: [RoutineMetadata],
        triggers: [TriggerMetadata],
        synonyms: [SynonymMetadata] = []
    ) {
        self.name = name
        self.tables = tables
        self.views = views
        self.functions = functions
        self.procedures = procedures
        self.triggers = triggers
        self.synonyms = synonyms
    }
}

public struct SQLServerDatabaseStructure: Sendable {
    public let database: String?
    public let schemas: [SQLServerSchemaStructure]

    public init(database: String? = nil, schemas: [SQLServerSchemaStructure]) {
        self.database = database
        self.schemas = schemas
    }
}
