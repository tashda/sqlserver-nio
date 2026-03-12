import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - Foreign Key Constraints
    
    public func addForeignKey(
        name: String,
        table: String,
        columns: [String],
        referencedTable: String,
        referencedColumns: [String],
        schema: String = "dbo",
        referencedSchema: String = "dbo",
        options: ForeignKeyOptions = ForeignKeyOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func addForeignKey(
        name: String,
        table: String,
        columns: [String],
        referencedTable: String,
        referencedColumns: [String],
        schema: String = "dbo",
        referencedSchema: String = "dbo",
        options: ForeignKeyOptions = ForeignKeyOptions()
    ) async throws {
        guard columns.count == referencedColumns.count else {
            throw SQLServerError.invalidArgument("Number of columns must match number of referenced columns")
        }
        
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let escapedReferencedTableName = Self.escapeIdentifier(referencedTable)
        let referencedSchemaPrefix = referencedSchema != "dbo" ? "\(Self.escapeIdentifier(referencedSchema))." : ""
        let fullReferencedTableName = "\(referencedSchemaPrefix)\(escapedReferencedTableName)"
        
        let columnList = columns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
        let referencedColumnList = referencedColumns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
        
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
        
        _ = try await client.execute(sql)
    }
    
    public func dropForeignKey(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func dropForeignKey(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
}
