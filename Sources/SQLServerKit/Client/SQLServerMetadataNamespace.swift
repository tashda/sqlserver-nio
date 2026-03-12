import Foundation

public final class SQLServerMetadataNamespace: @unchecked Sendable {
    internal let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Server

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await client.withConnection { connection in
            try await connection.serverVersion()
        }
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerAgentStatus {
        try await client.withConnection { connection in
            try await connection.fetchAgentStatus()
        }
    }

    // MARK: - Databases

    @available(macOS 12.0, *)
    public func listDatabases() async throws -> [DatabaseMetadata] {
        try await client.withConnection { connection in
            try await connection.listDatabases()
        }
    }

    @available(macOS 12.0, *)
    public func databaseState(name: String) async throws -> DatabaseMetadata {
        try await client.withConnection { connection in
            try await connection.databaseState(name: name)
        }
    }

    // MARK: - Schemas

    @available(macOS 12.0, *)
    public func listSchemas(in database: String? = nil) async throws -> [SchemaMetadata] {
        try await client.withConnection { connection in
            try await connection.listSchemas(in: database)
        }
    }

    // MARK: - Tables & Views

    @available(macOS 12.0, *)
    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [TableMetadata] {
        try await client.withConnection { connection in
            try await connection.listTables(database: database, schema: schema, includeComments: includeComments)
        }
    }

    // MARK: - Columns

    @available(macOS 12.0, *)
    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false
    ) async throws -> [ColumnMetadata] {
        try await client.withConnection { connection in
            try await connection.listColumns(
                database: database,
                schema: schema,
                table: table,
                objectTypeHint: objectTypeHint,
                includeComments: includeComments
            )
        }
    }

    @available(macOS 12.0, *)
    public func listColumnsForSchema(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false
    ) async throws -> [ColumnMetadata] {
        try await client.withConnection { connection in
            try await connection.listColumnsForSchema(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func listColumnsForDatabase(
        database: String? = nil,
        includeComments: Bool = false
    ) async throws -> [ColumnMetadata] {
        try await client.withConnection { connection in
            try await connection.listColumnsForDatabase(database: database, includeComments: includeComments)
        }
    }

    // MARK: - Parameters

    @available(macOS 12.0, *)
    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String
    ) async throws -> [ParameterMetadata] {
        try await client.withConnection { connection in
            try await connection.listParameters(database: database, schema: schema, object: object)
        }
    }

    // MARK: - Keys & Constraints

    @available(macOS 12.0, *)
    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await client.withConnection { connection in
            try await connection.listPrimaryKeys(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeysFromCatalog(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await client.withConnection { connection in
            try await connection.listPrimaryKeysFromCatalog(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await client.withConnection { connection in
            try await connection.listUniqueConstraints(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String
    ) async throws -> [ForeignKeyMetadata] {
        try await client.withConnection { connection in
            try await connection.listForeignKeys(database: database, schema: schema, table: table)
        }
    }

    // MARK: - Indexes

    @available(macOS 12.0, *)
    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String
    ) async throws -> [IndexMetadata] {
        try await client.withConnection { connection in
            try await connection.listIndexes(database: database, schema: schema, table: table)
        }
    }

    // MARK: - Dependencies

    @available(macOS 12.0, *)
    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String
    ) async throws -> [DependencyMetadata] {
        try await client.withConnection { connection in
            try await connection.listDependencies(database: database, schema: schema, object: object)
        }
    }

    // MARK: - Triggers

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false
    ) async throws -> [TriggerMetadata] {
        try await client.withConnection { connection in
            try await connection.listTriggers(database: database, schema: schema, table: table, includeComments: includeComments)
        }
    }

    // MARK: - Routines

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [RoutineMetadata] {
        try await client.withConnection { connection in
            try await connection.listProcedures(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [RoutineMetadata] {
        try await client.withConnection { connection in
            try await connection.listFunctions(database: database, schema: schema, includeComments: includeComments)
        }
    }

    // MARK: - Object Definitions

    @available(macOS 12.0, *)
    public func objectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind
    ) async throws -> ObjectDefinition? {
        try await client.withConnection { connection in
            try await connection.objectDefinition(database: database, schema: schema, name: name, kind: kind)
        }
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) async throws -> [ObjectDefinition] {
        try await client.withConnection { connection in
            try await connection.fetchObjectDefinitions(identifiers)
        }
    }

    // MARK: - Structure Loading

    @available(macOS 12.0, *)
    public func loadSchemaStructure(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false
    ) async throws -> SQLServerSchemaStructure {
        try await client.withConnection { connection in
            try await connection.loadSchemaStructure(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func loadDatabaseStructure(
        database: String? = nil,
        includeComments: Bool = false
    ) async throws -> SQLServerDatabaseStructure {
        try await client.withConnection { connection in
            try await connection.loadDatabaseStructure(database: database, includeComments: includeComments)
        }
    }

    // MARK: - Search

    @available(macOS 12.0, *)
    public func search(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default
    ) async throws -> [MetadataSearchHit] {
        try await client.withConnection { connection in
            try await connection.searchMetadata(query: query, database: database, schema: schema, scopes: scopes)
        }
    }
}
