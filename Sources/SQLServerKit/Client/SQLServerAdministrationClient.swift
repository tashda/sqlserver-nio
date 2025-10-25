import NIO
import SQLServerTDS

public struct SQLServerLoginInfo: Sendable {
    public let name: String
    public let type: String
    public let isDisabled: Bool
}

public struct SQLServerRoleInfo: Sendable {
    public let name: String
    public let isFixedRole: Bool
}

public final class SQLServerAdministrationClient {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    public func listServerLogins(includeDisabled: Bool = true) -> EventLoopFuture<[SQLServerLoginInfo]> {
        let filter = includeDisabled ? "" : "WHERE sp.is_disabled = 0"
        let sql = """
        SELECT sp.name, sp.type_desc, sp.is_disabled
        FROM sys.server_principals AS sp
        WHERE sp.type IN ('S','U','G','E','X')
        \(filter.isEmpty ? "" : filter)
        ORDER BY sp.name;
        """

        return client.query(sql).map {
            rows in
            rows.compactMap {
                row in
                guard
                    let name = row.column("name")?.string,
                    let type = row.column("type_desc")?.string,
                    let disabled = row.column("is_disabled")?.int
                else {
                    return nil
                }
                return SQLServerLoginInfo(name: name, type: type, isDisabled: disabled != 0)
            }
        }
    }

    public func listServerRoles() -> EventLoopFuture<[SQLServerRoleInfo]> {
        let sql = """
        SELECT name, is_fixed_role = CAST(ISNULL(is_fixed_role, 0) AS INT)
        FROM sys.server_principals
        WHERE type = 'R'
        ORDER BY name;
        """

        return client.query(sql).map {
            rows in
            rows.compactMap {
                row in
                guard let name = row.column("name")?.string else { return nil }
                let isFixed = row.column("is_fixed_role")?.int ?? 0
                return SQLServerRoleInfo(name: name, isFixedRole: isFixed != 0)
            }
        }
    }

    @available(macOS 12.0, *)
    public func listServerLogins(includeDisabled: Bool = true) async throws -> [SQLServerLoginInfo] {
        try await listServerLogins(includeDisabled: includeDisabled).get()
    }

    @available(macOS 12.0, *)
    public func listServerRoles() async throws -> [SQLServerRoleInfo] {
        try await listServerRoles().get()
    }

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
        var primaryKeyColumns = [String]()
        var columnStrings = [String]()

        for col in columns {
            columnStrings.append(try sqlString(for: col))
            if case .standard(let standardColumn) = col.definition, standardColumn.isPrimaryKey {
                primaryKeyColumns.append(col.name)
            }
        }

        let primaryKeyConstraint: String
        if !primaryKeyColumns.isEmpty {
            let keylist = primaryKeyColumns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
            primaryKeyConstraint = ", CONSTRAINT [PK_\(name)] PRIMARY KEY (\(keylist))"
        } else {
            primaryKeyConstraint = ""
        }

        let columnsAndConstraints = columnStrings.joined(separator: ", ") + primaryKeyConstraint
        let createTableSql = "CREATE TABLE [dbo].\(Self.escapeIdentifier(name)) (\n  \(columnsAndConstraints)\n)"

        // Collect column comments
        var commentStatements: [String] = []
        for col in columns {
            if case .standard(let standardColumn) = col.definition, let comment = standardColumn.comment {
                let commentSql = try commentSqlString(forColumn: col.name, inTable: name, comment: comment)
                commentStatements.append(commentSql)
            }
        }
        
        // Execute using separate SQL batches with proper transaction control
        try await executeWithTransactionControl(createTableSql: createTableSql, commentStatements: commentStatements)
    }
    /// Executes table creation with comments using separate SQL batches with proper transaction control
    @available(macOS 12.0, *)
    private func executeWithTransactionControl(createTableSql: String, commentStatements: [String]) async throws {
        try await client.withConnection { connection in
            do {
                // Begin transaction
                _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
                
                // Create table
                _ = try await connection.underlying.rawSql(createTableSql).get()
                
                // Add comments
                for commentSql in commentStatements {
                    _ = try await connection.underlying.rawSql(commentSql).get()
                }
                
                // Commit transaction
                _ = try await connection.underlying.rawSql("COMMIT").get()
            } catch {
                // Rollback on any error
                do {
                    _ = try await connection.underlying.rawSql("ROLLBACK").get()
                } catch {
                    // Log rollback failure but don't mask original error
                    self.client.logger.warning("Failed to rollback transaction: \(error)")
                }
                throw error
            }
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
    


    public func dropTable(name: String) -> EventLoopFuture<Void> {
        let sql = "DROP TABLE \(Self.escapeIdentifier(name))"
        return client.execute(sql).flatMapThrowing { result in
            if let errorMessage = result.messages.first(where: { $0.kind == .error }) {
                throw SQLServerError.sqlExecutionError(message: errorMessage.message)
            }
            return ()
        }
    }

    @available(macOS 12.0, *)
    public func dropTable(name: String) async throws {
        let sql = "DROP TABLE \(Self.escapeIdentifier(name))"
        _ = try await client.execute(sql)
    }



    @available(macOS 12.0, *)
    private func commentSqlString(forColumn column: String, inTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        
        // Use the simplest possible syntax that should work
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
    
    private func commentSqlString(forTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)'
        """
        return sql
    }
    
    private func updateCommentSqlString(forColumn column: String, inTable table: String, comment: String) throws -> String {
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
    
    private func updateCommentSqlString(forTable table: String, comment: String) throws -> String {
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
    
    private func removeCommentSqlString(forColumn column: String, inTable table: String) throws -> String {
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
    
    private func removeCommentSqlString(forTable table: String) throws -> String {
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

    private func sqlString(for column: SQLServerColumnDefinition) throws -> String {
        let name = Self.escapeIdentifier(column.name)
        switch column.definition {
        case .computed(let expression):
            return "\(name) AS (\(expression))"
        case .standard(let std):
            var parts = [String]()
            parts.append(name)
            parts.append(std.dataType.toSqlString())

            if let identity = std.identity {
                parts.append("IDENTITY(\(identity.seed), \(identity.increment))")
            }

            if std.isSparse {
                parts.append("SPARSE")
            }

            parts.append(std.isNullable ? "NULL" : "NOT NULL")

            if let defaultValue = std.defaultValue {
                parts.append("DEFAULT \(defaultValue)")
            }

            if std.isUnique {
                parts.append("UNIQUE")
            }

            return parts.joined(separator: " ")
        }
    }

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}
