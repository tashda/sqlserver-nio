import NIO
import SQLServerTDS

extension SQLServerSecurityClient {

    // MARK: - Dynamic Data Masking

    /// Lists all masked columns in the current database, optionally filtered by schema and/or table.
    @available(macOS 12.0, *)
    public func listMaskedColumns(schema: String? = nil, table: String? = nil) async throws -> [MaskedColumnInfo] {
        var sql = """
        SELECT s.name AS schema_name, t.name AS table_name,
               c.name AS column_name, c.masking_function
        FROM sys.masked_columns AS c
        INNER JOIN sys.tables AS t ON t.object_id = c.object_id
        INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE c.is_masked = 1
        """
        if let schema {
            sql += " AND s.name = N'\(schema.replacingOccurrences(of: "'", with: "''"))'"
        }
        if let table {
            sql += " AND t.name = N'\(table.replacingOccurrences(of: "'", with: "''"))'"
        }
        sql += " ORDER BY s.name, t.name, c.column_id"

        let rows = try await query(sql)
        return rows.map { row in
            MaskedColumnInfo(
                schema: row.column("schema_name")?.string ?? "",
                table: row.column("table_name")?.string ?? "",
                column: row.column("column_name")?.string ?? "",
                maskingFunction: row.column("masking_function")?.string ?? ""
            )
        }
    }

    /// Adds a dynamic data mask to a column.
    @available(macOS 12.0, *)
    public func addMask(schema: String, table: String, column: String, function: MaskFunction) async throws {
        let escapedSchema = Self.escapeIdentifier(schema)
        let escapedTable = Self.escapeIdentifier(table)
        let escapedColumn = Self.escapeIdentifier(column)
        let sql = "ALTER TABLE \(escapedSchema).\(escapedTable) ALTER COLUMN \(escapedColumn) ADD MASKED WITH (FUNCTION = '\(function.sqlExpression)')"
        _ = try await exec(sql)
    }

    /// Removes a dynamic data mask from a column.
    @available(macOS 12.0, *)
    public func dropMask(schema: String, table: String, column: String) async throws {
        let escapedSchema = Self.escapeIdentifier(schema)
        let escapedTable = Self.escapeIdentifier(table)
        let escapedColumn = Self.escapeIdentifier(column)
        let sql = "ALTER TABLE \(escapedSchema).\(escapedTable) ALTER COLUMN \(escapedColumn) DROP MASKED"
        _ = try await exec(sql)
    }

    /// Grants UNMASK permission to a principal.
    @available(macOS 12.0, *)
    public func grantUnmask(to principal: String) async throws {
        let escapedPrincipal = Self.escapeIdentifier(principal)
        _ = try await exec("GRANT UNMASK TO \(escapedPrincipal)")
    }

    /// Revokes UNMASK permission from a principal.
    @available(macOS 12.0, *)
    public func revokeUnmask(from principal: String) async throws {
        let escapedPrincipal = Self.escapeIdentifier(principal)
        _ = try await exec("REVOKE UNMASK FROM \(escapedPrincipal)")
    }
}
