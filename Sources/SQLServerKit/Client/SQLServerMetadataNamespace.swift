import Foundation
import NIO

public final class SQLServerMetadataNamespace: @unchecked Sendable {
    internal let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    @available(macOS 12.0, *)
    public func listDatabases(on eventLoop: EventLoop? = nil) async throws -> [DatabaseMetadata] {
        try await client.listDatabases(on: eventLoop)
    }

    @available(macOS 12.0, *)
    public func listSchemas(in database: String? = nil, on eventLoop: EventLoop? = nil) async throws -> [SchemaMetadata] {
        try await client.listSchemas(in: database, on: eventLoop)
    }

    @available(macOS 12.0, *)
    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TableMetadata] {
        try await client.listTables(database: database, schema: schema, includeComments: includeComments, on: eventLoop)
    }

    @available(macOS 12.0, *)
    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await client.listColumns(
            database: database,
            schema: schema,
            table: table,
            objectTypeHint: objectTypeHint,
            includeComments: includeComments,
            on: eventLoop
        )
    }

    @available(macOS 12.0, *)
    public func listIndexes(database: String? = nil, schema: String, table: String, on eventLoop: EventLoop? = nil) async throws -> [IndexMetadata] {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.listIndexes(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(database: String? = nil, schema: String, table: String, on eventLoop: EventLoop? = nil) async throws -> [ForeignKeyMetadata] {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.listForeignKeys(database: database, schema: schema, table: table).get()
        }
    }

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TriggerMetadata] {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.listTriggers(database: database, schema: schema, table: table, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.listProcedures(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.listFunctions(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func objectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) async throws -> ObjectDefinition? {
        try await client.objectDefinition(database: database, schema: schema, name: name, kind: kind, on: eventLoop)
    }

    @available(macOS 12.0, *)
    public func loadSchemaStructure(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerSchemaStructure {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.loadSchemaStructure(database: database, schema: schema, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func loadDatabaseStructure(
        database: String? = nil,
        includeComments: Bool = false,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerDatabaseStructure {
        try await client.withConnection(on: eventLoop) { connection in
            try await connection.loadDatabaseStructure(database: database, includeComments: includeComments).get()
        }
    }

    @available(macOS 12.0, *)
    public func search(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default,
        on eventLoop: EventLoop? = nil
    ) async throws -> [MetadataSearchHit] {
        try await client.searchMetadata(query: query, database: database, schema: schema, scopes: scopes, on: eventLoop)
    }
}
