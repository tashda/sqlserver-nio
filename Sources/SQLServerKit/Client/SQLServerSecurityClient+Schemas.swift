import NIO
import SQLServerTDS

extension SQLServerSecurityClient {
    // MARK: - Schema helpers (security-flavored)

    internal func listSchemas() -> EventLoopFuture<[SchemaInfo]> {
        let sql = """
        SELECT s.name, dp.name AS owner
        FROM sys.schemas AS s
        LEFT JOIN sys.database_principals AS dp ON s.principal_id = dp.principal_id
        WHERE s.schema_id <> 4 -- exclude sys
        ORDER BY s.name;
        """
        return run(sql).map { rows in
            rows.map { r in SchemaInfo(name: r.column("name")?.string ?? "", owner: r.column("owner")?.string) }
        }
    }

    internal func createSchema(name: String, authorization: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE SCHEMA \(Self.escapeIdentifier(name))"
        if let auth = authorization { sql += " AUTHORIZATION \(Self.escapeIdentifier(auth))" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func dropSchema(name: String) -> EventLoopFuture<Void> {
        let sql = "DROP SCHEMA \(Self.escapeIdentifier(name));"
        return run(sql).map { _ in () }
    }

    /// Drops a schema. When `cascade` is true, attempts to drop all objects in the schema first (views, functions, procedures, synonyms, foreign keys, tables, types), then drops the schema.
    /// Note: This operates within the current database context and requires appropriate privileges. It mirrors SSMS behavior for manual cascades.
    internal func dropSchema(name: String, cascade: Bool) -> EventLoopFuture<Void> {
        guard cascade else { return dropSchema(name: name) }
        let schemaLit = name.replacingOccurrences(of: "'", with: "''")
        let script = """
        SET NOCOUNT ON;
        DECLARE @schema sysname = N'\(schemaLit)';
        BEGIN TRY
            BEGIN TRAN;

            -- 1) Drop foreign keys on tables in the schema
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + N'ALTER TABLE '
                + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(parent_object_id))
                + N' DROP CONSTRAINT ' + QUOTENAME(name) + N';\n'
            FROM sys.foreign_keys
            WHERE parent_object_id IN (
                SELECT o.object_id FROM sys.objects AS o WHERE o.type = 'U' AND o.schema_id = SCHEMA_ID(@schema)
            );
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 2) Drop views
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP VIEW ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'V';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 3) Drop functions (scalar + table-valued)
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP FUNCTION ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type IN ('FN','TF','IF','FS','FT');
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 4) Drop procedures
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP PROCEDURE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'P';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 5) Drop synonyms
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP SYNONYM ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'SN';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 6) Drop tables (no FKs remain here)
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'U';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 7) Drop user-defined types in schema
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP TYPE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';\n'
            FROM sys.types AS t JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.is_user_defined = 1;
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 8) Finally drop schema
            EXEC('DROP SCHEMA ' + QUOTENAME(@schema));
            COMMIT;
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0 ROLLBACK;
            THROW;
        END CATCH
        """
        return run(script).map { _ in () }
    }

    internal func alterAuthorizationOnSchema(schema: String, principal: String) -> EventLoopFuture<Void> {
        let sql = "ALTER AUTHORIZATION ON SCHEMA::\(Self.escapeIdentifier(schema)) TO \(Self.escapeIdentifier(principal));"
        return run(sql).map { _ in () }
    }

    internal func transferObjectToSchema(objectSchema: String, objectName: String, newSchema: String) -> EventLoopFuture<Void> {
        let sql = "ALTER SCHEMA \(Self.escapeIdentifier(newSchema)) TRANSFER OBJECT::\(Self.escapeIdentifier(objectSchema)).\(Self.escapeIdentifier(objectName));"
        return run(sql).map { _ in () }
    }

    @available(macOS 12.0, *)
    public func listSchemas() async throws -> [SchemaInfo] {
        try await listSchemas().get()
    }

    @available(macOS 12.0, *)
    public func createSchema(name: String, authorization: String? = nil) async throws {
        _ = try await createSchema(name: name, authorization: authorization).get()
    }

    @available(macOS 12.0, *)
    public func dropSchema(name: String) async throws {
        _ = try await dropSchema(name: name).get()
    }

    @available(macOS 12.0, *)
    public func dropSchema(name: String, cascade: Bool) async throws {
        _ = try await dropSchema(name: name, cascade: cascade).get()
    }

    @available(macOS 12.0, *)
    public func alterAuthorizationOnSchema(schema: String, principal: String) async throws {
        _ = try await alterAuthorizationOnSchema(schema: schema, principal: principal).get()
    }

    @available(macOS 12.0, *)
    public func transferObjectToSchema(objectSchema: String, objectName: String, newSchema: String) async throws {
        _ = try await transferObjectToSchema(objectSchema: objectSchema, objectName: objectName, newSchema: newSchema).get()
    }
}
