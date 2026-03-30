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

    /// Checks the current login's server-level permissions, roles, and msdb role membership.
    ///
    /// Fetches everything in two queries (server-level checks + msdb roles) so the result
    /// can be cached and reused without per-operation round-trips.
    /// All SQL functions used work on SQL Server 2016+.
    @available(macOS 12.0, *)
    public func checkServerPermissions() async throws -> ServerPermissions {
        let serverSQL = """
        SELECT
            IS_SRVROLEMEMBER('sysadmin') AS is_sysadmin,
            IS_SRVROLEMEMBER('serveradmin') AS is_server_admin,
            IS_SRVROLEMEMBER('securityadmin') AS is_security_admin,
            IS_SRVROLEMEMBER('dbcreator') AS is_db_creator,
            HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') AS has_view_server_state,
            HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY LOGIN') AS has_alter_any_login,
            HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY DATABASE') AS has_alter_any_database,
            HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY CREDENTIAL') AS has_alter_any_credential,
            HAS_DBACCESS('master') AS has_master_access,
            HAS_DBACCESS('msdb') AS has_msdb_access
        """
        let rows = try await client.query(serverSQL)
        let row = rows.first

        let hasMsdbAccess = (row?.column("has_msdb_access")?.int ?? 0) == 1
        var msdbRoles: Set<String> = []

        if hasMsdbAccess {
            let msdbSQL = """
            SELECT r.name AS role_name
            FROM msdb.sys.database_role_members drm
            JOIN msdb.sys.database_principals r ON r.principal_id = drm.role_principal_id
            JOIN msdb.sys.database_principals u ON u.principal_id = drm.member_principal_id
            WHERE u.sid = SUSER_SID()
              AND r.name IN (
                  N'SQLAgentUserRole', N'SQLAgentReaderRole', N'SQLAgentOperatorRole',
                  N'DatabaseMailUserRole', N'db_owner'
              )
            """
            let roleRows = try await client.query(msdbSQL)
            msdbRoles = Set(roleRows.compactMap { $0.column("role_name")?.string })
        }

        return ServerPermissions(
            isSysadmin: (row?.column("is_sysadmin")?.int ?? 0) == 1,
            isServerAdmin: (row?.column("is_server_admin")?.int ?? 0) == 1,
            isSecurityAdmin: (row?.column("is_security_admin")?.int ?? 0) == 1,
            isDBCreator: (row?.column("is_db_creator")?.int ?? 0) == 1,
            hasViewServerState: (row?.column("has_view_server_state")?.int ?? 0) == 1,
            hasAlterAnyLogin: (row?.column("has_alter_any_login")?.int ?? 0) == 1,
            hasAlterAnyDatabase: (row?.column("has_alter_any_database")?.int ?? 0) == 1,
            hasAlterAnyCredential: (row?.column("has_alter_any_credential")?.int ?? 0) == 1,
            hasMasterAccess: (row?.column("has_master_access")?.int ?? 0) == 1,
            hasMsdbAccess: hasMsdbAccess,
            msdbRoles: msdbRoles
        )
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

    /// Checks whether a database has containment enabled (PARTIAL or FULL).
    @available(macOS 12.0, *)
    public func isDatabaseContained(database: String) async throws -> Bool {
        let escaped = database.replacingOccurrences(of: "'", with: "''")
        let sql = "SELECT containment FROM sys.databases WHERE name = N'\(escaped)'"
        let rows = try await client.query(sql)
        guard let row = rows.first, let containment = row.column("containment")?.int else {
            return false
        }
        // 0 = NONE, 1 = PARTIAL
        return containment != 0
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

    /// Returns bidirectional dependencies for an object (both what it references and what references it).
    @available(macOS 12.0, *)
    public func objectDependencies(
        database: String? = nil,
        schema: String,
        name: String
    ) async throws -> [SQLServerObjectDependency] {
        try await client.withConnection { connection in
            try await connection.objectDependencies(database: database, schema: schema, name: name)
        }
    }

    // MARK: - Table Properties

    /// Returns row count, space usage, and create/modify dates for a table.
    @available(macOS 12.0, *)
    public func tableProperties(
        database: String? = nil,
        schema: String,
        table: String
    ) async throws -> SQLServerTableProperties {
        try await client.withConnection { connection in
            try await connection.tableProperties(database: database, schema: schema, table: table)
        }
    }

    // MARK: - Object Definition (raw string)

    /// Returns the T-SQL definition of a programmable object as a raw string, or nil if not found.
    @available(macOS 12.0, *)
    public func objectDefinitionString(
        database: String? = nil,
        schema: String,
        name: String
    ) async throws -> String? {
        try await client.withConnection { connection in
            try await connection.objectDefinitionString(database: database, schema: schema, name: name)
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

    // MARK: - Synonyms

    @available(macOS 12.0, *)
    public func listSynonyms(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [SynonymMetadata] {
        try await client.withConnection { connection in
            try await connection.listSynonyms(database: database, schema: schema, includeComments: includeComments)
        }
    }

    // MARK: - Sequences

    @available(macOS 12.0, *)
    public func listSequences(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [SequenceMetadata] {
        try await client.withConnection { connection in
            try await connection.listSequences(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func sequenceDetails(
        database: String? = nil,
        schema: String,
        name: String
    ) async throws -> SequenceMetadata? {
        try await client.withConnection { connection in
            try await connection.sequenceDetails(database: database, schema: schema, name: name)
        }
    }

    // MARK: - User-Defined Types

    @available(macOS 12.0, *)
    public func listUserTypes(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [UserTypeMetadata] {
        try await client.withConnection { connection in
            try await connection.listUserTypes(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func userTypeDetails(
        database: String? = nil,
        schema: String,
        name: String
    ) async throws -> UserTypeMetadata? {
        try await client.withConnection { connection in
            try await connection.userTypeDetails(database: database, schema: schema, name: name)
        }
    }

    // MARK: - Object Comments

    /// Fetch the MS_Description extended property for any schema-scoped object.
    @available(macOS 12.0, *)
    public func objectComment(
        database: String? = nil,
        schema: String,
        name: String
    ) async throws -> String? {
        try await client.withConnection { connection in
            try await connection.objectComment(database: database, schema: schema, name: name)
        }
    }

    // MARK: - Trigger Details

    /// Fetch detailed metadata for a specific trigger including definition, events, and comment.
    @available(macOS 12.0, *)
    public func triggerDetails(
        database: String? = nil,
        schema: String,
        table: String,
        name: String
    ) async throws -> TriggerDetails? {
        try await client.withConnection { connection in
            try await connection.triggerDetails(database: database, schema: schema, table: table, name: name)
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
    public func listObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) async throws -> [ObjectDefinition] {
        try await client.withConnection { connection in
            try await connection.listObjectDefinitions(identifiers)
        }
    }

    @available(*, deprecated, renamed: "listObjectDefinitions(_:)")
    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) async throws -> [ObjectDefinition] {
        try await listObjectDefinitions(identifiers)
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

    // MARK: - Per-Type Search

    @available(macOS 12.0, *)
    public func searchTables(query: String, database: String? = nil, limit: Int = 50) async throws -> [TableSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchTables(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchViews(query: String, database: String? = nil, limit: Int = 50) async throws -> [ViewSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchViews(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchFunctions(query: String, database: String? = nil, limit: Int = 50) async throws -> [RoutineSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchFunctions(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchProcedures(query: String, database: String? = nil, limit: Int = 50) async throws -> [RoutineSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchProcedures(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchTriggers(query: String, database: String? = nil, limit: Int = 50) async throws -> [TriggerSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchTriggers(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchColumns(query: String, database: String? = nil, limit: Int = 50) async throws -> [ColumnSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchColumns(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchIndexes(query: String, database: String? = nil, limit: Int = 50) async throws -> [IndexSearchResult] {
        try await client.withConnection { connection in
            try await connection.searchIndexes(query: query, database: database, limit: limit)
        }
    }

    @available(macOS 12.0, *)
    public func searchForeignKeys(query: String, database: String? = nil, limit: Int = 50) async throws -> [ForeignKeySearchResult] {
        try await client.withConnection { connection in
            try await connection.searchForeignKeys(query: query, database: database, limit: limit)
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
