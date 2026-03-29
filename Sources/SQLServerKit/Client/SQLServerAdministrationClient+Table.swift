import Foundation
import SQLServerTDS

extension SQLServerAdministrationClient {
    // MARK: - Table Management

    internal func createTable(name: String, columns: [SQLServerColumnDefinition]) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createTable(name: name, columns: columns)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    public func createTable(name: String, columns: [SQLServerColumnDefinition]) async throws {
        try await client.withConnection { connection in
            try await connection.createTable(name: name, columns: columns, database: self.database)
        }
    }
    
    /// Adds a comment to a table
    @available(macOS 12.0, *)
    public func addTableComment(tableName: String, schema: String = "dbo", comment: String) async throws {
        let sql = """
        EXEC sp_addextendedproperty
            @name = N'MS_Description',
            @value = N'\(SQLServerSQL.escapeLiteral(comment))',
            @level0type = N'SCHEMA', @level0name = N'\(SQLServerSQL.escapeLiteral(schema))',
            @level1type = N'TABLE', @level1name = N'\(SQLServerSQL.escapeLiteral(tableName))';
        """
        _ = try await client.execute(sql)
    }

    /// Adds a comment to a column
    @available(macOS 12.0, *)
    public func addColumnComment(tableName: String, columnName: String, schema: String = "dbo", comment: String) async throws {
        let sql = commentSqlString(forColumn: columnName, inTable: tableName, schema: schema, comment: comment)
        _ = try await client.execute(sql)
    }

    /// Updates an existing column comment
    @available(macOS 12.0, *)
    public func updateColumnComment(tableName: String, columnName: String, schema: String = "dbo", comment: String) async throws {
        let sql = updateCommentSqlString(forColumn: columnName, inTable: tableName, schema: schema, comment: comment)
        _ = try await client.execute(sql)
    }

    /// Updates an existing table comment
    @available(macOS 12.0, *)
    public func updateTableComment(tableName: String, schema: String = "dbo", comment: String) async throws {
        let sql = updateCommentSqlString(forTable: tableName, schema: schema, comment: comment)
        _ = try await client.execute(sql)
    }

    /// Removes a column comment
    @available(macOS 12.0, *)
    public func removeColumnComment(tableName: String, columnName: String, schema: String = "dbo") async throws {
        let sql = removeCommentSqlString(forColumn: columnName, inTable: tableName, schema: schema)
        _ = try await client.execute(sql)
    }

    /// Removes a table comment
    @available(macOS 12.0, *)
    public func removeTableComment(tableName: String, schema: String = "dbo") async throws {
        let sql = removeCommentSqlString(forTable: tableName, schema: schema)
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func dropTable(name: String, schema: String = "dbo", database: String? = nil, ifExists: Bool = false) async throws {
        if ifExists {
            let escapedSchema = SQLServerSQL.escapeIdentifier(schema)
            let escapedName = SQLServerSQL.escapeIdentifier(name)
            let objectId = SQLServerSQL.escapeLiteral("\(schema).\(name)")
            let sql: String
            if let db = database ?? self.database {
                let escapedDb = SQLServerSQL.escapeIdentifier(db)
                sql = "USE \(escapedDb); IF OBJECT_ID(N'\(objectId)', 'U') IS NOT NULL DROP TABLE \(escapedSchema).\(escapedName)"
            } else {
                sql = "IF OBJECT_ID(N'\(objectId)', 'U') IS NOT NULL DROP TABLE \(escapedSchema).\(escapedName)"
            }
            _ = try await client.execute(sql)
        } else {
            try await client.withConnection { connection in
                try await connection.dropTable(name: name, schema: schema, database: database ?? self.database)
            }
        }
    }

    /// Returns space usage information for a table via `sp_spaceused`.
    @available(macOS 12.0, *)
    public func spaceUsed(schema: String = "dbo", table: String, database: String? = nil) async throws -> SQLServerSpaceUsed {
        let objectName = SQLServerSQL.escapeLiteral("\(schema).\(table)")
        let sql = "EXEC sp_spaceused N'\(objectName)'"

        let rows: [SQLServerRow]
        if let db = database ?? self.database {
            rows = try await client.withDatabase(db) { connection in
                try await connection.query(sql).get()
            }
        } else {
            rows = try await client.query(sql)
        }

        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "sp_spaceused returned no results for [\(schema)].[\(table)]")
        }

        return SQLServerSpaceUsed(
            rows: row.column("rows")?.string ?? "0",
            reserved: row.column("reserved")?.string ?? "0 KB",
            data: row.column("data")?.string ?? "0 KB",
            indexSize: row.column("index_size")?.string ?? "0 KB",
            unused: row.column("unused")?.string ?? "0 KB"
        )
    }

    @available(macOS 12.0, *)
    public func renameTable(
        name: String,
        newName: String,
        schema: String = "dbo",
        database: String? = nil
    ) async throws {
        try await client.withConnection { connection in
            try await connection.renameTable(name: name, newName: newName, schema: schema, database: database ?? self.database)
        }
    }

    @available(macOS 12.0, *)
    public func truncateTable(name: String, schema: String = "dbo", database: String? = nil) async throws {
        try await client.withConnection { connection in
            try await connection.truncateTable(name: name, schema: schema, database: database ?? self.database)
        }
    }

    private func commentSqlString(forColumn column: String, inTable table: String, schema: String, comment: String) -> String {
        """
        EXEC sp_addextendedproperty
            N'MS_Description',
            N'\(SQLServerSQL.escapeLiteral(comment))',
            N'SCHEMA',
            N'\(SQLServerSQL.escapeLiteral(schema))',
            N'TABLE',
            N'\(SQLServerSQL.escapeLiteral(table))',
            N'COLUMN',
            N'\(SQLServerSQL.escapeLiteral(column))'
        """
    }

    private func updateCommentSqlString(forColumn column: String, inTable table: String, schema: String, comment: String) -> String {
        """
        EXEC sp_updateextendedproperty
            N'MS_Description',
            N'\(SQLServerSQL.escapeLiteral(comment))',
            N'SCHEMA',
            N'\(SQLServerSQL.escapeLiteral(schema))',
            N'TABLE',
            N'\(SQLServerSQL.escapeLiteral(table))',
            N'COLUMN',
            N'\(SQLServerSQL.escapeLiteral(column))'
        """
    }

    private func updateCommentSqlString(forTable table: String, schema: String, comment: String) -> String {
        """
        EXEC sp_updateextendedproperty
            N'MS_Description',
            N'\(SQLServerSQL.escapeLiteral(comment))',
            N'SCHEMA',
            N'\(SQLServerSQL.escapeLiteral(schema))',
            N'TABLE',
            N'\(SQLServerSQL.escapeLiteral(table))'
        """
    }

    private func removeCommentSqlString(forColumn column: String, inTable table: String, schema: String) -> String {
        """
        EXEC sp_dropextendedproperty
            N'MS_Description',
            N'SCHEMA',
            N'\(SQLServerSQL.escapeLiteral(schema))',
            N'TABLE',
            N'\(SQLServerSQL.escapeLiteral(table))',
            N'COLUMN',
            N'\(SQLServerSQL.escapeLiteral(column))'
        """
    }

    private func removeCommentSqlString(forTable table: String, schema: String) -> String {
        """
        EXEC sp_dropextendedproperty
            N'MS_Description',
            N'SCHEMA',
            N'\(SQLServerSQL.escapeLiteral(schema))',
            N'TABLE',
            N'\(SQLServerSQL.escapeLiteral(table))'
        """
    }

    internal func sqlString(for column: SQLServerColumnDefinition) throws -> String {
        let name = SQLServerSQL.escapeIdentifier(column.name)
        switch column.definition {
        case .computed(let expression, let persisted):
            return "\(name) AS (\(expression))\(persisted ? " PERSISTED" : "")"
        case .standard(let std):
            var parts = [String]()
            parts.append(name)
            parts.append(std.dataType.toSqlString())

            if let collate = std.collation, !collate.isEmpty {
                parts.append("COLLATE \(collate)")
            }

            if let identity = std.identity {
                parts.append("IDENTITY(\(identity.seed), \(identity.increment))")
            }

            if std.isSparse {
                parts.append("SPARSE")
            }

            parts.append(std.isNullable ? "NULL" : "NOT NULL")

            if std.isRowGuidCol {
                parts.append("ROWGUIDCOL")
            }

            if let defaultValue = std.defaultValue {
                parts.append("DEFAULT \(defaultValue)")
            }

            if std.isUnique {
                parts.append("UNIQUE")
            }

            return parts.joined(separator: " ")
        }
    }
}
