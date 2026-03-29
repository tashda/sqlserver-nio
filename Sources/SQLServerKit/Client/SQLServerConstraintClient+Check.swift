import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - Check Constraints

    @discardableResult
    public func addCheckConstraint(
        name: String,
        table: String,
        expression: String,
        schema: String = "dbo",
        checkExisting: Bool = true
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
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
    @discardableResult
    public func addCheckConstraint(
        name: String,
        table: String,
        expression: String,
        schema: String = "dbo",
        checkExisting: Bool = true
    ) async throws -> [SQLServerStreamMessage] {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        var sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        CHECK (\(expression))
        """

        if !checkExisting {
            sql += " WITH NOCHECK"
        }

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func dropCheckConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
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
    @discardableResult
    public func dropCheckConstraint(name: String, table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        let result = try await client.execute(sql)
        return result.messages
    }
}
