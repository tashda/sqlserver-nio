import Foundation
import NIO

extension SQLServerConnection {
    // MARK: - Metadata Operations

    public func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        metadataClient.listDatabases()
    }

    public func databaseState(name: String) -> EventLoopFuture<DatabaseMetadata> {
        let loop = self.eventLoop
        if #available(macOS 12.0, *) {
            return loop.makeFutureWithTask {
                try await self.metadataClient.databaseState(name: name)
            }
        } else {
            return loop.makeFailedFuture(SQLServerError.unsupportedPlatform)
        }
    }

    @available(macOS 12.0, *)
    public func databaseState(name: String) async throws -> DatabaseMetadata {
        try await metadataClient.databaseState(name: name)
    }

    public func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        metadataClient.listSchemas(in: database)
    }

    public func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TableMetadata]> {
        metadataClient.listTables(database: database, schema: schema, includeComments: includeComments)
    }

    public func listColumns(database: String? = nil, schema: String, table: String, objectTypeHint: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[ColumnMetadata]> {
        metadataClient.listColumns(database: database, schema: schema, table: table, objectTypeHint: objectTypeHint, includeComments: includeComments)
    }

    public func listColumnsForSchema(database: String? = nil, schema: String, includeComments: Bool = false) -> EventLoopFuture<[ColumnMetadata]> {
        metadataClient.listColumnsForSchema(database: database, schema: schema, includeComments: includeComments)
    }

    public func listColumnsForDatabase(database: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[ColumnMetadata]> {
        metadataClient.listColumnsForDatabase(database: database, includeComments: includeComments)
    }

    public func listParameters(database: String? = nil, schema: String, object: String) -> EventLoopFuture<[ParameterMetadata]> {
        metadataClient.listParameters(database: database, schema: schema, object: object)
    }

    public func listPrimaryKeys(database: String? = nil, schema: String? = nil, table: String? = nil) -> EventLoopFuture<[KeyConstraintMetadata]> {
        metadataClient.listPrimaryKeys(database: database, schema: schema, table: table)
    }

    public func listPrimaryKeysFromCatalog(database: String? = nil, schema: String? = nil, table: String? = nil) -> EventLoopFuture<[KeyConstraintMetadata]> {
        metadataClient.listPrimaryKeysFromCatalog(database: database, schema: schema, table: table)
    }

    public func listUniqueConstraints(database: String? = nil, schema: String? = nil, table: String? = nil) -> EventLoopFuture<[KeyConstraintMetadata]> {
        metadataClient.listUniqueConstraints(database: database, schema: schema, table: table)
    }

    public func listIndexes(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[IndexMetadata]> {
        metadataClient.listIndexes(database: database, schema: schema, table: table)
    }

    public func listForeignKeys(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[ForeignKeyMetadata]> {
        metadataClient.listForeignKeys(database: database, schema: schema, table: table)
    }

    public func listDependencies(database: String? = nil, schema: String, object: String) -> EventLoopFuture<[DependencyMetadata]> {
        metadataClient.listDependencies(database: database, schema: schema, object: object)
    }

    public func listTriggers(database: String? = nil, schema: String? = nil, table: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TriggerMetadata]> {
        metadataClient.listTriggers(database: database, schema: schema, table: table, includeComments: includeComments)
    }

    public func listProcedures(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[RoutineMetadata]> {
        metadataClient.listProcedures(database: database, schema: schema, includeComments: includeComments)
    }

    public func listFunctions(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[RoutineMetadata]> {
        metadataClient.listFunctions(database: database, schema: schema, includeComments: includeComments)
    }

    public func fetchObjectDefinitions(_ identifiers: [SQLServerMetadataObjectIdentifier]) -> EventLoopFuture<[ObjectDefinition]> {
        metadataClient.fetchObjectDefinitions(identifiers)
    }

    public func fetchObjectDefinition(database: String? = nil, schema: String, name: String, kind: SQLServerMetadataObjectIdentifier.Kind) -> EventLoopFuture<ObjectDefinition?> {
        let identifier = SQLServerMetadataObjectIdentifier(database: database, schema: schema, name: name, kind: kind)
        return metadataClient.fetchObjectDefinitions([identifier]).map { $0.first }
    }

    public func loadSchemaStructure(database: String? = nil, schema: String, includeComments: Bool = false) -> EventLoopFuture<SQLServerSchemaStructure> {
        metadataClient.loadSchemaStructure(database: database, schema: schema, includeComments: includeComments)
    }

    public func loadDatabaseStructure(database: String? = nil, includeComments: Bool = false) -> EventLoopFuture<SQLServerDatabaseStructure> {
        metadataClient.loadDatabaseStructure(database: database, includeComments: includeComments)
    }

    public func searchMetadata(query: String, database: String? = nil, schema: String? = nil, scopes: MetadataSearchScope = .default) -> EventLoopFuture<[MetadataSearchHit]> {
        metadataClient.searchMetadata(query: query, database: database, schema: schema, scopes: scopes)
    }

    public func serverVersion() -> EventLoopFuture<String> {
        metadataClient.serverVersion()
    }

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await metadataClient.serverVersion().get()
    }

    // MARK: - SQL Agent Operations

    public func fetchAgentStatus() -> EventLoopFuture<SQLServerAgentStatus> {
        metadataClient.fetchAgentStatus()
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerAgentStatus {
        try await metadataClient.fetchAgentStatus()
    }
}
