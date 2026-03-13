import Foundation
import SQLServerTDS

extension SQLServerAdministrationClient {
    // MARK: - Column Operations

    @available(macOS 12.0, *)
    public func addColumn(
        table: String,
        name: String,
        dataType: String,
        isNullable: Bool = true,
        defaultValue: String? = nil,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let fullTable = qualifiedName(table: table, schema: schema, database: database ?? self.database)
        let escapedColumn = Self.escapeIdentifier(name)
        let nullClause = isNullable ? "NULL" : "NOT NULL"
        var sql = "ALTER TABLE \(fullTable) ADD \(escapedColumn) \(dataType) \(nullClause)"
        if let defaultValue, !defaultValue.isEmpty {
            sql += " DEFAULT \(defaultValue)"
        }
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func addComputedColumn(
        table: String,
        name: String,
        expression: String,
        persisted: Bool = false,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let fullTable = qualifiedName(table: table, schema: schema, database: database ?? self.database)
        let escapedColumn = Self.escapeIdentifier(name)
        var sql = "ALTER TABLE \(fullTable) ADD \(escapedColumn) AS (\(expression))"
        if persisted {
            sql += " PERSISTED"
        }
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func dropColumn(
        table: String,
        column: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let fullTable = qualifiedName(table: table, schema: schema, database: database ?? self.database)
        let escapedColumn = Self.escapeIdentifier(column)
        _ = try await client.execute("ALTER TABLE \(fullTable) DROP COLUMN \(escapedColumn)")
    }

    @available(macOS 12.0, *)
    public func renameColumn(
        table: String,
        from oldName: String,
        to newName: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let escapedOld = "\(schema).\(table).\(oldName)".replacingOccurrences(of: "'", with: "''")
        let escapedNew = newName.replacingOccurrences(of: "'", with: "''")
        let renameSql = "EXEC sp_rename N'\(escapedOld)', N'\(escapedNew)', N'COLUMN';"

        if let db = database ?? self.database {
            try await client.withDatabase(db) { connection in
                _ = try await connection.execute(renameSql)
            }
        } else {
            _ = try await client.execute(renameSql)
        }
    }

    @available(macOS 12.0, *)
    public func alterColumnType(
        table: String,
        column: String,
        newType: String,
        isNullable: Bool,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let fullTable = qualifiedName(table: table, schema: schema, database: database ?? self.database)
        let escapedColumn = Self.escapeIdentifier(column)
        let nullClause = isNullable ? "NULL" : "NOT NULL"
        _ = try await client.execute("ALTER TABLE \(fullTable) ALTER COLUMN \(escapedColumn) \(newType) \(nullClause)")
    }

    @available(macOS 12.0, *)
    public func alterColumnNullability(
        table: String,
        column: String,
        isNullable: Bool,
        currentType: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let fullTable = qualifiedName(table: table, schema: schema, database: database ?? self.database)
        let escapedColumn = Self.escapeIdentifier(column)
        let nullClause = isNullable ? "NULL" : "NOT NULL"
        _ = try await client.execute("ALTER TABLE \(fullTable) ALTER COLUMN \(escapedColumn) \(currentType) \(nullClause)")
    }

    // MARK: - Default Constraint by Column

    @available(macOS 12.0, *)
    public func dropDefaultConstraintForColumn(
        table: String,
        column: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")

        let sql = """
        DECLARE @constraint NVARCHAR(256);
        SELECT @constraint = d.name FROM sys.default_constraints d
        JOIN sys.columns c ON d.parent_column_id = c.column_id AND d.parent_object_id = c.object_id
        WHERE d.parent_object_id = OBJECT_ID('\(escapedSchema).\(escapedTable)') AND c.name = '\(escapedColumn)';
        IF @constraint IS NOT NULL
            EXEC('ALTER TABLE \(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(table)) DROP CONSTRAINT [' + @constraint + ']');
        """

        if let db = database ?? self.database {
            try await client.withDatabase(db) { connection in
                _ = try await connection.execute(sql)
            }
        } else {
            _ = try await client.execute(sql)
        }
    }

    // MARK: - Helpers

    private func qualifiedName(table: String, schema: String, database: String?) -> String {
        if let database, !database.isEmpty {
            return "\(Self.escapeIdentifier(database)).\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(table))"
        }
        return "\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(table))"
    }
}
