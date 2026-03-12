import Foundation
import NIO

extension SQLServerClient {
    @available(macOS 12.0, *)
    public func objectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) async throws -> ObjectDefinition? {
        try await fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind, on: eventLoop)
    }

    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier],
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ObjectDefinition]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.fetchObjectDefinitions(identifiers)
        }
    }

    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<ObjectDefinition?> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind)
        }
    }

    // MARK: - SQL Agent Status

    public func fetchAgentStatus(on eventLoop: EventLoop? = nil) -> EventLoopFuture<SQLServerAgentStatus> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.fetchAgentStatus()
        }
    }

    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[MetadataSearchHit]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.searchMetadata(query: query, database: database, schema: schema, scopes: scopes)
        }
    }

    public func listDatabases(on eventLoop: EventLoop? = nil) -> EventLoopFuture<[DatabaseMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listDatabases()
        }
    }

    public func listSchemas(
        in database: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[SchemaMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listSchemas(in: database)
        }
    }

    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[TableMetadata]> {
        return withConnection(on: eventLoop) { connection in
            connection.listTables(database: database, schema: schema, includeComments: includeComments)
        }
    }

    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ColumnMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listColumns(
                database: database,
                schema: schema,
                table: table,
                objectTypeHint: objectTypeHint,
                includeComments: includeComments
            )
        }
    }

    public func listColumnsForSchema(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ColumnMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listColumnsForSchema(
                database: database,
                schema: schema,
                includeComments: includeComments
            )
        }
    }

    public func listColumnsForDatabase(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ColumnMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listColumnsForDatabase(
                database: database,
                includeComments: includeComments
            )
        }
    }

    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ParameterMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listParameters(database: database, schema: schema, object: object)
        }
    }

    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listPrimaryKeys(database: database, schema: schema, table: table)
        }
    }

    public func listPrimaryKeysFromCatalog(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listPrimaryKeysFromCatalog(database: database, schema: schema, table: table)
        }
    }

    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listUniqueConstraints(database: database, schema: schema, table: table)
        }
    }

    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[IndexMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listIndexes(database: database, schema: schema, table: table)
        }
    }

    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ForeignKeyMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listForeignKeys(database: database, schema: schema, table: table)
        }
    }

    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[DependencyMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listDependencies(database: database, schema: schema, object: object)
        }
    }

    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[TriggerMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listTriggers(database: database, schema: schema, table: table, includeComments: includeComments)
        }
    }

    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listProcedures(database: database, schema: schema, includeComments: includeComments)
        }
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listFunctions(database: database, schema: schema, includeComments: includeComments)
        }
    }

    public func loadSchemaStructure(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerSchemaStructure> {
        let loop = eventLoop ?? eventLoopGroup.next()
        @Sendable
        func attempt(_ index: Int) -> EventLoopFuture<SQLServerSchemaStructure> {
            withConnection(on: loop) { connection in
                connection.loadSchemaStructure(database: database, schema: schema, includeComments: includeComments)
            }.flatMapError { error in
                let normalized = SQLServerError.normalize(error)
                if case .timeout = normalized, index < 2 {
                    return attempt(index + 1)
                }
                return loop.makeFailedFuture(normalized)
            }
        }
        return attempt(1)
    }

    public func loadDatabaseStructure(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerDatabaseStructure> {
        let loop = eventLoop ?? eventLoopGroup.next()
        @Sendable
        func attempt(_ index: Int) -> EventLoopFuture<SQLServerDatabaseStructure> {
            withConnection(on: loop) { connection in
                connection.loadDatabaseStructure(database: database, includeComments: includeComments)
            }.flatMapError { error in
                let normalized = SQLServerError.normalize(error)
                if case .timeout = normalized, index < 2 {
                    return attempt(index + 1)
                }
                return loop.makeFailedFuture(normalized)
            }
        }
        return attempt(1)
    }

    public func serverVersion(on eventLoop: EventLoop? = nil) -> EventLoopFuture<String> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.serverVersion()
        }
    }

    @available(macOS 12.0, *)
    public func renameTable(
        name: String,
        newName: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        try await withConnection { connection in
            try await connection.renameTable(name: name, newName: newName, schema: schema, database: database)
        }
    }

    @available(macOS 12.0, *)
    public func dropTable(
        name: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        try await withConnection { connection in
            try await connection.dropTable(name: name, schema: schema, database: database)
        }
    }

    @available(macOS 12.0, *)
    public func truncateTable(
        name: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        try await withConnection { connection in
            try await connection.truncateTable(name: name, schema: schema, database: database)
        }
    }
}
