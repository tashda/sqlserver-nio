import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - List Check Constraints

    /// Lists all check constraints on a table.
    @available(macOS 12.0, *)
    public func listCheckConstraints(
        database: String? = nil,
        schema: String = "dbo",
        table: String
    ) async throws -> [SQLServerCheckConstraint] {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedTable = SQLServerSQL.escapeLiteral(table)
        let sql = """
        SELECT cc.name, cc.definition
        FROM sys.check_constraints cc
        WHERE cc.parent_object_id = OBJECT_ID(N'\(escapedSchema).\(escapedTable)')
        ORDER BY cc.name
        """

        let rows: [SQLServerRow]
        if let database {
            rows = try await client.withDatabase(database) { connection in
                try await connection.query(sql).get()
            }
        } else {
            rows = try await client.query(sql)
        }

        return rows.map { row in
            SQLServerCheckConstraint(
                name: row.column("name")?.string ?? "",
                definition: row.column("definition")?.string ?? ""
            )
        }
    }

    // MARK: - Drop Default Constraint for Column

    /// Drops the default constraint on a specific column, if one exists.
    @available(macOS 12.0, *)
    public func dropDefaultConstraintForColumn(
        schema: String = "dbo",
        table: String,
        column: String,
        database: String? = nil
    ) async throws {
        let escapedSchema = SQLServerSQL.escapeLiteral(schema)
        let escapedTable = SQLServerSQL.escapeLiteral(table)
        let escapedColumn = SQLServerSQL.escapeLiteral(column)
        let escapedSchemaId = SQLServerSQL.escapeIdentifier(schema)
        let escapedTableId = SQLServerSQL.escapeIdentifier(table)

        let sql = """
        DECLARE @constraint NVARCHAR(256);
        SELECT @constraint = d.name FROM sys.default_constraints d
        JOIN sys.columns c ON d.parent_column_id = c.column_id AND d.parent_object_id = c.object_id
        WHERE d.parent_object_id = OBJECT_ID(N'\(escapedSchema).\(escapedTable)') AND c.name = N'\(escapedColumn)';
        IF @constraint IS NOT NULL EXEC('ALTER TABLE \(escapedSchemaId).\(escapedTableId) DROP CONSTRAINT [' + @constraint + ']');
        """

        if let database {
            _ = try await client.withDatabase(database) { connection in
                try await connection.execute(sql).get()
            }
        } else {
            _ = try await client.execute(sql)
        }
    }
}
