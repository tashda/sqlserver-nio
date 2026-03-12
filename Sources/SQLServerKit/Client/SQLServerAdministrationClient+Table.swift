import Foundation
import SQLServerTDS

extension SQLServerAdministrationClient {
    // MARK: - Table Management

    public func createTable(name: String, columns: [SQLServerColumnDefinition]) -> EventLoopFuture<Void> {
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
    public func addTableComment(tableName: String, comment: String) async throws {
        let sql = """
        EXEC sp_addextendedproperty
            @name = N'MS_Description',
            @value = N'\(comment.replacingOccurrences(of: "'", with: "''"))',
            @level0type = N'SCHEMA', @level0name = N'dbo',
            @level1type = N'TABLE', @level1name = N'\(tableName.replacingOccurrences(of: "'", with: "''"))';
        """
        _ = try await client.execute(sql)
    }
    
    /// Adds a comment to a column
    @available(macOS 12.0, *)
    public func addColumnComment(tableName: String, columnName: String, comment: String) async throws {
        let sql = try commentSqlString(forColumn: columnName, inTable: tableName, comment: comment)
        _ = try await client.execute(sql)
    }
    
    /// Updates an existing column comment
    @available(macOS 12.0, *)
    public func updateColumnComment(tableName: String, columnName: String, comment: String) async throws {
        let sql = try updateCommentSqlString(forColumn: columnName, inTable: tableName, comment: comment)
        _ = try await client.execute(sql)
    }
    
    /// Updates an existing table comment
    @available(macOS 12.0, *)
    public func updateTableComment(tableName: String, comment: String) async throws {
        let sql = try updateCommentSqlString(forTable: tableName, comment: comment)
        _ = try await client.execute(sql)
    }
    
    /// Removes a column comment
    @available(macOS 12.0, *)
    public func removeColumnComment(tableName: String, columnName: String) async throws {
        let sql = try removeCommentSqlString(forColumn: columnName, inTable: tableName)
        _ = try await client.execute(sql)
    }
    
    /// Removes a table comment
    @available(macOS 12.0, *)
    public func removeTableComment(tableName: String) async throws {
        let sql = try removeCommentSqlString(forTable: tableName)
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func dropTable(name: String, schema: String = "dbo", database: String? = nil) async throws {
        try await client.withConnection { connection in
            try await connection.dropTable(name: name, schema: schema, database: database ?? self.database)
        }
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

    @available(macOS 12.0, *)
    private func commentSqlString(forColumn column: String, inTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        
        let sql = """
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
        return sql
    }
    
    internal func updateCommentSqlString(forColumn column: String, inTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_updateextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
        return sql
    }
    
    internal func updateCommentSqlString(forTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_updateextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)'
        """
        return sql
    }
    
    internal func removeCommentSqlString(forColumn column: String, inTable table: String) throws -> String {
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_dropextendedproperty 
            N'MS_Description',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
        return sql
    }
    
    internal func removeCommentSqlString(forTable table: String) throws -> String {
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_dropextendedproperty 
            N'MS_Description',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)'
        """
        return sql
    }

    internal func sqlString(for column: SQLServerColumnDefinition) throws -> String {
        let name = Self.escapeIdentifier(column.name)
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
