import Foundation
import NIO

extension SQLServerConnection {
    // MARK: - Metadata Operations

    @available(macOS 12.0, *)
    public func objectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind
    ) async throws -> ObjectDefinition? {
        try await fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind).get()
    }

    internal func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        metadataClient.listDatabases()
    }

    internal func databaseState(name: String) -> EventLoopFuture<DatabaseMetadata> {
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

    @available(macOS 12.0, *)
    public func listDatabases() async throws -> [DatabaseMetadata] {
        try await listDatabases().get()
    }

    @available(macOS 12.0, *)
    public func listSchemas(in database: String? = nil) async throws -> [SchemaMetadata] {
        try await listSchemas(in: database).get()
    }

    @available(macOS 12.0, *)
    public func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) async throws -> [TableMetadata] {
        try await listTables(database: database, schema: schema, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func listColumns(database: String? = nil, schema: String, table: String, objectTypeHint: String? = nil, includeComments: Bool = false) async throws -> [ColumnMetadata] {
        try await listColumns(database: database, schema: schema, table: table, objectTypeHint: objectTypeHint, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func listColumnsForSchema(database: String? = nil, schema: String, includeComments: Bool = false) async throws -> [ColumnMetadata] {
        try await listColumnsForSchema(database: database, schema: schema, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func listColumnsForDatabase(database: String? = nil, includeComments: Bool = false) async throws -> [ColumnMetadata] {
        try await listColumnsForDatabase(database: database, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func listParameters(database: String? = nil, schema: String, object: String) async throws -> [ParameterMetadata] {
        try await listParameters(database: database, schema: schema, object: object).get()
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeys(database: String? = nil, schema: String? = nil, table: String? = nil) async throws -> [KeyConstraintMetadata] {
        try await listPrimaryKeys(database: database, schema: schema, table: table).get()
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeysFromCatalog(database: String? = nil, schema: String? = nil, table: String? = nil) async throws -> [KeyConstraintMetadata] {
        try await listPrimaryKeysFromCatalog(database: database, schema: schema, table: table).get()
    }

    @available(macOS 12.0, *)
    public func listUniqueConstraints(database: String? = nil, schema: String? = nil, table: String? = nil) async throws -> [KeyConstraintMetadata] {
        try await listUniqueConstraints(database: database, schema: schema, table: table).get()
    }

    @available(macOS 12.0, *)
    public func listIndexes(database: String? = nil, schema: String, table: String) async throws -> [IndexMetadata] {
        try await listIndexes(database: database, schema: schema, table: table).get()
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(database: String? = nil, schema: String, table: String) async throws -> [ForeignKeyMetadata] {
        try await listForeignKeys(database: database, schema: schema, table: table).get()
    }

    @available(macOS 12.0, *)
    public func listDependencies(database: String? = nil, schema: String, object: String) async throws -> [DependencyMetadata] {
        try await listDependencies(database: database, schema: schema, object: object).get()
    }

    @available(macOS 12.0, *)
    public func objectDependencies(database: String? = nil, schema: String, name: String) async throws -> [SQLServerObjectDependency] {
        try await objectDependencies(database: database, schema: schema, name: name).get()
    }

    @available(macOS 12.0, *)
    public func tableProperties(database: String? = nil, schema: String, table: String) async throws -> SQLServerTableProperties {
        try await tableProperties(database: database, schema: schema, table: table).get()
    }

    @available(macOS 12.0, *)
    public func objectDefinitionString(database: String? = nil, schema: String, name: String) async throws -> String? {
        try await objectDefinitionString(database: database, schema: schema, name: name).get()
    }

    @available(macOS 12.0, *)
    public func listTriggers(database: String? = nil, schema: String? = nil, table: String? = nil, includeComments: Bool = false) async throws -> [TriggerMetadata] {
        try await listTriggers(database: database, schema: schema, table: table, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func listProcedures(database: String? = nil, schema: String? = nil, includeComments: Bool = false) async throws -> [RoutineMetadata] {
        try await listProcedures(database: database, schema: schema, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func listFunctions(database: String? = nil, schema: String? = nil, includeComments: Bool = false) async throws -> [RoutineMetadata] {
        try await listFunctions(database: database, schema: schema, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(_ identifiers: [SQLServerMetadataObjectIdentifier]) async throws -> [ObjectDefinition] {
        try await fetchObjectDefinitions(identifiers).get()
    }

    @available(macOS 12.0, *)
    public func loadSchemaStructure(database: String? = nil, schema: String, includeComments: Bool = false) async throws -> SQLServerSchemaStructure {
        try await loadSchemaStructure(database: database, schema: schema, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func loadDatabaseStructure(database: String? = nil, includeComments: Bool = false) async throws -> SQLServerDatabaseStructure {
        try await loadDatabaseStructure(database: database, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func searchMetadata(query: String, database: String? = nil, schema: String? = nil, scopes: MetadataSearchScope = .default) async throws -> [MetadataSearchHit] {
        try await searchMetadata(query: query, database: database, schema: schema, scopes: scopes).get()
    }

    internal func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        metadataClient.listSchemas(in: database)
    }

    internal func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TableMetadata]> {
        metadataClient.listTables(database: database, schema: schema, includeComments: includeComments)
    }

    internal func listColumns(database: String? = nil, schema: String, table: String, objectTypeHint: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[ColumnMetadata]> {
        metadataClient.listColumns(database: database, schema: schema, table: table, objectTypeHint: objectTypeHint, includeComments: includeComments)
    }

    internal func listColumnsForSchema(database: String? = nil, schema: String, includeComments: Bool = false) -> EventLoopFuture<[ColumnMetadata]> {
        metadataClient.listColumnsForSchema(database: database, schema: schema, includeComments: includeComments)
    }

    internal func listColumnsForDatabase(database: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[ColumnMetadata]> {
        metadataClient.listColumnsForDatabase(database: database, includeComments: includeComments)
    }

    internal func listParameters(database: String? = nil, schema: String, object: String) -> EventLoopFuture<[ParameterMetadata]> {
        metadataClient.listParameters(database: database, schema: schema, object: object)
    }

    internal func listPrimaryKeys(database: String? = nil, schema: String? = nil, table: String? = nil) -> EventLoopFuture<[KeyConstraintMetadata]> {
        metadataClient.listPrimaryKeys(database: database, schema: schema, table: table)
    }

    internal func listPrimaryKeysFromCatalog(database: String? = nil, schema: String? = nil, table: String? = nil) -> EventLoopFuture<[KeyConstraintMetadata]> {
        metadataClient.listPrimaryKeysFromCatalog(database: database, schema: schema, table: table)
    }

    internal func listUniqueConstraints(database: String? = nil, schema: String? = nil, table: String? = nil) -> EventLoopFuture<[KeyConstraintMetadata]> {
        metadataClient.listUniqueConstraints(database: database, schema: schema, table: table)
    }

    internal func listIndexes(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[IndexMetadata]> {
        metadataClient.listIndexes(database: database, schema: schema, table: table)
    }

    internal func listForeignKeys(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[ForeignKeyMetadata]> {
        metadataClient.listForeignKeys(database: database, schema: schema, table: table)
    }

    internal func listDependencies(database: String? = nil, schema: String, object: String) -> EventLoopFuture<[DependencyMetadata]> {
        metadataClient.listDependencies(database: database, schema: schema, object: object)
    }

    internal func objectDependencies(database: String? = nil, schema: String, name: String) -> EventLoopFuture<[SQLServerObjectDependency]> {
        metadataClient.objectDependencies(database: database, schema: schema, name: name)
    }

    internal func tableProperties(database: String? = nil, schema: String, table: String) -> EventLoopFuture<SQLServerTableProperties> {
        metadataClient.tableProperties(database: database, schema: schema, table: table)
    }

    internal func objectDefinitionString(database: String? = nil, schema: String, name: String) -> EventLoopFuture<String?> {
        metadataClient.objectDefinitionString(database: database, schema: schema, name: name)
    }

    internal func listTriggers(database: String? = nil, schema: String? = nil, table: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TriggerMetadata]> {
        metadataClient.listTriggers(database: database, schema: schema, table: table, includeComments: includeComments)
    }

    internal func listProcedures(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[RoutineMetadata]> {
        metadataClient.listProcedures(database: database, schema: schema, includeComments: includeComments)
    }

    internal func listFunctions(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[RoutineMetadata]> {
        metadataClient.listFunctions(database: database, schema: schema, includeComments: includeComments)
    }

    internal func fetchObjectDefinitions(_ identifiers: [SQLServerMetadataObjectIdentifier]) -> EventLoopFuture<[ObjectDefinition]> {
        metadataClient.fetchObjectDefinitions(identifiers)
    }

    internal func fetchObjectDefinition(database: String? = nil, schema: String, name: String, kind: SQLServerMetadataObjectIdentifier.Kind) -> EventLoopFuture<ObjectDefinition?> {
        let identifier = SQLServerMetadataObjectIdentifier(database: database, schema: schema, name: name, kind: kind)
        return metadataClient.fetchObjectDefinitions([identifier]).map { $0.first }
    }

    internal func loadSchemaStructure(database: String? = nil, schema: String, includeComments: Bool = false) -> EventLoopFuture<SQLServerSchemaStructure> {
        metadataClient.loadSchemaStructure(database: database, schema: schema, includeComments: includeComments)
    }

    internal func loadDatabaseStructure(database: String? = nil, includeComments: Bool = false) -> EventLoopFuture<SQLServerDatabaseStructure> {
        metadataClient.loadDatabaseStructure(database: database, includeComments: includeComments)
    }

    internal func searchMetadata(query: String, database: String? = nil, schema: String? = nil, scopes: MetadataSearchScope = .default) -> EventLoopFuture<[MetadataSearchHit]> {
        metadataClient.searchMetadata(query: query, database: database, schema: schema, scopes: scopes)
    }

    // MARK: - Per-Type Search (internal ELF)

    internal func searchTables(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[TableSearchResult]> {
        metadataClient.searchTables(query: query, database: database, limit: limit)
    }

    internal func searchViews(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[ViewSearchResult]> {
        metadataClient.searchViews(query: query, database: database, limit: limit)
    }

    internal func searchFunctions(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[RoutineSearchResult]> {
        metadataClient.searchFunctions(query: query, database: database, limit: limit)
    }

    internal func searchProcedures(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[RoutineSearchResult]> {
        metadataClient.searchProcedures(query: query, database: database, limit: limit)
    }

    internal func searchTriggers(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[TriggerSearchResult]> {
        metadataClient.searchTriggers(query: query, database: database, limit: limit)
    }

    internal func searchColumns(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[ColumnSearchResult]> {
        metadataClient.searchColumns(query: query, database: database, limit: limit)
    }

    internal func searchIndexes(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[IndexSearchResult]> {
        metadataClient.searchIndexes(query: query, database: database, limit: limit)
    }

    internal func searchForeignKeys(query: String, database: String? = nil, limit: Int = 50) -> EventLoopFuture<[ForeignKeySearchResult]> {
        metadataClient.searchForeignKeys(query: query, database: database, limit: limit)
    }

    // MARK: - Per-Type Search (public async)

    @available(macOS 12.0, *)
    public func searchTables(query: String, database: String? = nil, limit: Int = 50) async throws -> [TableSearchResult] {
        try await searchTables(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchViews(query: String, database: String? = nil, limit: Int = 50) async throws -> [ViewSearchResult] {
        try await searchViews(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchFunctions(query: String, database: String? = nil, limit: Int = 50) async throws -> [RoutineSearchResult] {
        try await searchFunctions(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchProcedures(query: String, database: String? = nil, limit: Int = 50) async throws -> [RoutineSearchResult] {
        try await searchProcedures(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchTriggers(query: String, database: String? = nil, limit: Int = 50) async throws -> [TriggerSearchResult] {
        try await searchTriggers(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchColumns(query: String, database: String? = nil, limit: Int = 50) async throws -> [ColumnSearchResult] {
        try await searchColumns(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchIndexes(query: String, database: String? = nil, limit: Int = 50) async throws -> [IndexSearchResult] {
        try await searchIndexes(query: query, database: database, limit: limit).get()
    }

    @available(macOS 12.0, *)
    public func searchForeignKeys(query: String, database: String? = nil, limit: Int = 50) async throws -> [ForeignKeySearchResult] {
        try await searchForeignKeys(query: query, database: database, limit: limit).get()
    }

    internal func serverVersion() -> EventLoopFuture<String> {
        metadataClient.serverVersion()
    }

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await metadataClient.serverVersion().get()
    }

    // MARK: - SQL Agent Operations

    internal func fetchAgentStatus() -> EventLoopFuture<SQLServerAgentStatus> {
        metadataClient.fetchAgentStatus()
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerAgentStatus {
        try await metadataClient.fetchAgentStatus()
    }
}
