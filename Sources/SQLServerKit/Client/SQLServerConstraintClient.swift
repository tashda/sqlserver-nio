import NIO
import SQLServerTDS

// MARK: - Constraint Types

public struct ForeignKeyOptions: Sendable {
    public let onDelete: ReferentialAction
    public let onUpdate: ReferentialAction
    public let checkExisting: Bool
    public let isNotTrusted: Bool
    
    public init(
        onDelete: ReferentialAction = .noAction,
        onUpdate: ReferentialAction = .noAction,
        checkExisting: Bool = true,
        isNotTrusted: Bool = false
    ) {
        self.onDelete = onDelete
        self.onUpdate = onUpdate
        self.checkExisting = checkExisting
        self.isNotTrusted = isNotTrusted
    }
    
    public enum ReferentialAction: String, Sendable {
        case noAction = "NO ACTION"
        case cascade = "CASCADE"
        case setNull = "SET NULL"
        case setDefault = "SET DEFAULT"
    }
}

public struct ConstraintInfo: Sendable {
    public let name: String
    public let type: ConstraintType
    public let tableName: String
    public let schemaName: String
    public let columns: [String]
    public let definition: String?
    public let referencedTable: String?
    public let referencedColumns: [String]?
    public let deleteAction: String?
    public let updateAction: String?
    
    public enum ConstraintType: String, Sendable {
        case primaryKey = "PRIMARY KEY"
        case foreignKey = "FOREIGN KEY"
        case unique = "UNIQUE"
        case check = "CHECK"
        case `default` = "DEFAULT"
    }
}

// MARK: - SQLServerConstraintClient

public final class SQLServerConstraintClient {
    private let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
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
    
    public func dropCheckConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
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
        
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let columnList = columns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
        let clusterType = clustered ? "CLUSTERED" : "NONCLUSTERED"
        
        let sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        UNIQUE \(clusterType) (\(columnList))
        """
        
        _ = try await client.execute(sql)
    }
    
    public func dropUniqueConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
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
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
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
        
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let columnList = columns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
        let clusterType = clustered ? "CLUSTERED" : "NONCLUSTERED"
        
        let sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        PRIMARY KEY \(clusterType) (\(columnList))
        """
        
        _ = try await client.execute(sql)
    }
    
    public func dropPrimaryKey(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
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
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
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
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let escapedColumnName = Self.escapeIdentifier(column)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = """
        ALTER TABLE \(fullTableName)
        ADD CONSTRAINT \(escapedConstraintName)
        DEFAULT \(defaultValue) FOR \(escapedColumnName)
        """
        
        _ = try await client.execute(sql)
    }
    
    public func dropDefaultConstraint(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
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
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) DROP CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Constraint Information
    
    @available(macOS 12.0, *)
    public func constraintExists(name: String, table: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.key_constraints kc
        INNER JOIN sys.objects o ON kc.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE kc.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        UNION ALL
        
        SELECT COUNT(*) as count
        FROM sys.foreign_keys fk
        INNER JOIN sys.objects o ON fk.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE fk.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        UNION ALL
        
        SELECT COUNT(*) as count
        FROM sys.check_constraints cc
        INNER JOIN sys.objects o ON cc.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE cc.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        UNION ALL
        
        SELECT COUNT(*) as count
        FROM sys.default_constraints dc
        INNER JOIN sys.objects o ON dc.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE dc.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """
        
        let rows = try await client.query(sql)
        let totalCount = rows.compactMap { $0.column("count")?.int }.reduce(0, +)
        return totalCount > 0
    }
    
    @available(macOS 12.0, *)
    public func listTableConstraints(table: String, schema: String = "dbo") async throws -> [ConstraintInfo] {
        let sql = """
        -- Primary Key and Unique Constraints
        SELECT 
            kc.name as constraint_name,
            kc.type_desc as constraint_type,
            o.name as table_name,
            s.name as schema_name,
            c.name as column_name,
            ic.key_ordinal,
            NULL as definition,
            NULL as referenced_table,
            NULL as referenced_column,
            NULL as delete_action,
            NULL as update_action
        FROM sys.key_constraints kc
        INNER JOIN sys.objects o ON kc.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.index_columns ic ON kc.unique_index_id = ic.index_id AND kc.parent_object_id = ic.object_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        UNION ALL
        
        -- Foreign Key Constraints
        SELECT 
            fk.name as constraint_name,
            'FOREIGN KEY' as constraint_type,
            o.name as table_name,
            s.name as schema_name,
            c.name as column_name,
            fkc.constraint_column_id as key_ordinal,
            NULL as definition,
            ro.name as referenced_table,
            rc.name as referenced_column,
            fk.delete_referential_action_desc as delete_action,
            fk.update_referential_action_desc as update_action
        FROM sys.foreign_keys fk
        INNER JOIN sys.objects o ON fk.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
        INNER JOIN sys.objects ro ON fk.referenced_object_id = ro.object_id
        INNER JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        UNION ALL
        
        -- Check Constraints
        SELECT 
            cc.name as constraint_name,
            'CHECK' as constraint_type,
            o.name as table_name,
            s.name as schema_name,
            c.name as column_name,
            1 as key_ordinal,
            cc.definition,
            NULL as referenced_table,
            NULL as referenced_column,
            NULL as delete_action,
            NULL as update_action
        FROM sys.check_constraints cc
        INNER JOIN sys.objects o ON cc.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        LEFT JOIN sys.columns c ON cc.parent_object_id = c.object_id AND cc.parent_column_id = c.column_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        UNION ALL
        
        -- Default Constraints
        SELECT 
            dc.name as constraint_name,
            'DEFAULT' as constraint_type,
            o.name as table_name,
            s.name as schema_name,
            c.name as column_name,
            1 as key_ordinal,
            dc.definition,
            NULL as referenced_table,
            NULL as referenced_column,
            NULL as delete_action,
            NULL as update_action
        FROM sys.default_constraints dc
        INNER JOIN sys.objects o ON dc.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        
        ORDER BY constraint_name, key_ordinal
        """
        
        let rows = try await client.query(sql)
        
        // Group rows by constraint name
        var constraintGroups: [String: [TDSRow]] = [:]
        for row in rows {
            let constraintName = row.column("constraint_name")?.string ?? ""
            if constraintGroups[constraintName] == nil {
                constraintGroups[constraintName] = []
            }
            constraintGroups[constraintName]?.append(row)
        }
        
        // Convert to ConstraintInfo objects
        var constraints: [ConstraintInfo] = []
        for (constraintName, constraintRows) in constraintGroups {
            guard let firstRow = constraintRows.first else { continue }
            
            let typeString = firstRow.column("constraint_type")?.string ?? ""
            let constraintType: ConstraintInfo.ConstraintType
            switch typeString {
            case "PRIMARY_KEY_CONSTRAINT":
                constraintType = .primaryKey
            case "UNIQUE_CONSTRAINT":
                constraintType = .unique
            case "FOREIGN KEY":
                constraintType = .foreignKey
            case "CHECK":
                constraintType = .check
            case "DEFAULT":
                constraintType = .`default`
            default:
                continue
            }
            
            let tableName = firstRow.column("table_name")?.string ?? ""
            let schemaName = firstRow.column("schema_name")?.string ?? ""
            let definition = firstRow.column("definition")?.string
            let referencedTable = firstRow.column("referenced_table")?.string
            let deleteAction = firstRow.column("delete_action")?.string
            let updateAction = firstRow.column("update_action")?.string
            
            let columns = constraintRows.compactMap { $0.column("column_name")?.string }
            let referencedColumns = constraintRows.compactMap { $0.column("referenced_column")?.string }
            
            let constraintInfo = ConstraintInfo(
                name: constraintName,
                type: constraintType,
                tableName: tableName,
                schemaName: schemaName,
                columns: columns,
                definition: definition,
                referencedTable: referencedTable,
                referencedColumns: referencedColumns.isEmpty ? nil : referencedColumns,
                deleteAction: deleteAction,
                updateAction: updateAction
            )
            
            constraints.append(constraintInfo)
        }
        
        return constraints.sorted { $0.name < $1.name }
    }
    
    // MARK: - Constraint Validation
    
    @available(macOS 12.0, *)
    public func enableConstraint(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) CHECK CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
    
    @available(macOS 12.0, *)
    public func disableConstraint(name: String, table: String, schema: String = "dbo") async throws {
        let escapedConstraintName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER TABLE \(fullTableName) NOCHECK CONSTRAINT \(escapedConstraintName)"
        _ = try await client.execute(sql)
    }
    
    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}