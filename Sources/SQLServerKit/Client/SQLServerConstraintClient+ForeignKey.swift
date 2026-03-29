import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - Foreign Key Constraints

    @discardableResult
    public func addForeignKey(
        name: String,
        table: String,
        columns: [String],
        referencedTable: String,
        referencedColumns: [String],
        schema: String = "dbo",
        referencedSchema: String = "dbo",
        options: ForeignKeyOptions = ForeignKeyOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addForeignKey(
                    name: name,
                    table: table,
                    columns: columns,
                    referencedTable: referencedTable,
                    referencedColumns: referencedColumns,
                    schema: schema,
                    referencedSchema: referencedSchema,
                    options: options
                )
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func addForeignKey(
        name: String,
        table: String,
        columns: [String],
        referencedTable: String,
        referencedColumns: [String],
        schema: String = "dbo",
        referencedSchema: String = "dbo",
        options: ForeignKeyOptions = ForeignKeyOptions()
    ) async throws -> [SQLServerStreamMessage] {
        guard columns.count == referencedColumns.count else {
            throw SQLServerError.invalidArgument("Number of columns must match number of referenced columns")
        }

        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let escapedReferencedTableName = SQLServerSQL.escapeIdentifier(referencedTable)
        let referencedSchemaPrefix = referencedSchema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(referencedSchema))." : ""
        let fullReferencedTableName = "\(referencedSchemaPrefix)\(escapedReferencedTableName)"

        let columnList = columns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")
        let referencedColumnList = referencedColumns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")

        var sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        FOREIGN KEY (\(columnList))
        REFERENCES \(fullReferencedTableName) (\(referencedColumnList))
        """

        if options.onDelete != .noAction {
            sql += "\nON DELETE \(options.onDelete.rawValue)"
        }

        if options.onUpdate != .noAction {
            sql += "\nON UPDATE \(options.onUpdate.rawValue)"
        }

        if !options.checkExisting {
            sql += "\nWITH NOCHECK"
        }

        if options.isNotTrusted {
            sql += "\nNOT FOR REPLICATION"
        }

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func dropForeignKey(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropForeignKey(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func dropForeignKey(name: String, table: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedConstraintName = SQLServerSQL.escapeIdentifier(name)
        let escapedTableName = SQLServerSQL.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        let result = try await client.execute(sql)
        return result.messages
    }
}
