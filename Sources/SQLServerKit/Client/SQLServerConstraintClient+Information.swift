import NIO
import SQLServerTDS

extension SQLServerConstraintClient {
    // MARK: - Constraint Information
    
    @available(macOS 12.0, *)
    public func constraintExists(name: String, table: String, schema: String = "dbo") async throws -> Bool {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT CASE WHEN EXISTS (
            SELECT 1
            FROM sys.objects c
            INNER JOIN sys.objects t ON c.parent_object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE c.name = N'\(escapedName)'
              AND t.name = N'\(escapedTable)'
              AND s.name = N'\(escapedSchema)'
              AND c.type IN ('PK', 'UQ', 'F', 'C', 'D')
        ) THEN 1 ELSE 0 END AS count
        """
        
        let rows = try await client.query(sql)
        return rows.first?.column("count")?.int == 1
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
        
        var constraintGroups: [String: [SQLServerRow]] = [:]
        for row in rows {
            let constraintName = row.column("constraint_name")?.string ?? ""
            if constraintGroups[constraintName] == nil {
                constraintGroups[constraintName] = []
            }
            constraintGroups[constraintName]?.append(row)
        }
        
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
}
