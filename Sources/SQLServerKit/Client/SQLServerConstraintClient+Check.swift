import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - Check Constraints
    
    public func addCheckConstraint(
        name: String,
        table: String,
        expression: String,
        schema: String = "dbo",
        checkExisting: Bool = true
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addCheckConstraint(name: name, table: table, expression: expression, schema: schema, checkExisting: checkExisting)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func addCheckConstraint(
        name: String,
        table: String,
        expression: String,
        schema: String = "dbo",
        checkExisting: Bool = true
    ) async throws {
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        var sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        CHECK (\(expression))
        """
        
        if !checkExisting {
            sql += " WITH NOCHECK"
        }
        
        _ = try await client.execute(sql)
    }
    
    internal func dropCheckConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropCheckConstraint(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropCheckConstraint(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
}
