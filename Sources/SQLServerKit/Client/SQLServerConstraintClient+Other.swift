import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - Unique Constraints
    
    public func addUniqueConstraint(
        name: String,
        table: String,
        columns: [String],
        schema: String = "dbo",
        clustered: Bool = false
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addUniqueConstraint(name: name, table: table, columns: columns, schema: schema, clustered: clustered)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func addUniqueConstraint(
        name: String,
        table: String,
        columns: [String],
        schema: String = "dbo",
        clustered: Bool = false
    ) async throws {
        guard !columns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one column is required for unique constraint")
        }
        
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let columnList = columns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")
        let clusterType = clustered ? "CLUSTERED" : "NONCLUSTERED"
        
        let sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        UNIQUE \(clusterType) (\(columnList))
        """
        
        _ = try await client.execute(sql)
    }
    
    internal func dropUniqueConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropUniqueConstraint(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropUniqueConstraint(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Primary Key Constraints
    
    public func addPrimaryKey(
        name: String,
        table: String,
        columns: [String],
        schema: String = "dbo",
        clustered: Bool = true
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addPrimaryKey(name: name, table: table, columns: columns, schema: schema, clustered: clustered)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func addPrimaryKey(
        name: String,
        table: String,
        columns: [String],
        schema: String = "dbo",
        clustered: Bool = true
    ) async throws {
        guard !columns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one column is required for primary key constraint")
        }
        
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let columnList = columns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")
        let clusterType = clustered ? "CLUSTERED" : "NONCLUSTERED"
        
        let sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        PRIMARY KEY \(clusterType) (\(columnList))
        """
        
        _ = try await client.execute(sql)
    }
    
    internal func dropPrimaryKey(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropPrimaryKey(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropPrimaryKey(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Default Constraints
    
    public func addDefaultConstraint(
        name: String,
        table: String,
        column: String,
        defaultValue: String,
        schema: String = "dbo"
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addDefaultConstraint(name: name, table: table, column: column, defaultValue: defaultValue, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func addDefaultConstraint(
        name: String,
        table: String,
        column: String,
        defaultValue: String,
        schema: String = "dbo"
    ) async throws {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let escapedColumnName = SQLServerSQL.escapeIdentifier(column)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        DEFAULT \(defaultValue) FOR \(escapedColumnName)
        """
        
        _ = try await client.execute(sql)
    }
    
    internal func dropDefaultConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropDefaultConstraint(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropDefaultConstraint(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
}
