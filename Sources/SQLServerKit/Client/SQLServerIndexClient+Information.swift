import NIO
import SQLServerTDS

extension SQLServerIndexClient {
    // MARK: - Index Information
    
    @available(macOS 12.0, *)
    public func indexExists(name: String, table: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE i.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """
        
        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }
    
    @available(macOS 12.0, *)
    public func getIndexInfo(name: String, table: String, schema: String = "dbo") async throws -> IndexInfo? {
        let sql = """
        SELECT 
            i.name as index_name,
            o.name as table_name,
            s.name as schema_name,
            i.type_desc as index_type,
            i.is_unique,
            i.is_primary_key,
            ic.key_ordinal,
            c.name as column_name,
            ic.is_descending_key,
            ic.is_included_column
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY ic.key_ordinal, ic.index_column_id
        """
        
        let rows = try await client.query(sql)
        guard !rows.isEmpty else { return nil }
        
        let firstRow = rows[0]
        let indexName = firstRow.column("index_name")?.string ?? ""
        let tableName = firstRow.column("table_name")?.string ?? ""
        let schemaName = firstRow.column("schema_name")?.string ?? ""
        let indexTypeString = firstRow.column("index_type")?.string ?? ""
        let isUnique = (firstRow.column("is_unique")?.int ?? 0) != 0
        let isPrimaryKey = (firstRow.column("is_primary_key")?.int ?? 0) != 0
        
        let indexType: IndexInfo.IndexType
        switch indexTypeString {
        case "CLUSTERED":
            indexType = .clustered
        case "NONCLUSTERED":
            indexType = .nonclustered
        case "HEAP":
            indexType = .heap
        default:
            indexType = .nonclustered
        }
        
        let columns = rows.map { row in
            IndexColumnInfo(
                name: row.column("column_name")?.string ?? "",
                keyOrdinal: row.column("key_ordinal")?.int ?? 0,
                isDescending: (row.column("is_descending_key")?.int ?? 0) != 0,
                isIncluded: (row.column("is_included_column")?.int ?? 0) != 0
            )
        }
        
        return IndexInfo(
            name: indexName,
            tableName: tableName,
            schemaName: schemaName,
            indexType: indexType,
            isUnique: isUnique,
            isPrimaryKey: isPrimaryKey,
            columns: columns
        )
    }
    
    @available(macOS 12.0, *)
    public func listTableIndexes(table: String, schema: String = "dbo") async throws -> [IndexInfo] {
        let sql = """
        SELECT DISTINCT
            i.name as index_name,
            o.name as table_name,
            s.name as schema_name,
            i.type_desc as index_type,
            i.is_unique,
            i.is_primary_key
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        AND i.name IS NOT NULL
        ORDER BY i.name
        """
        
        let rows = try await client.query(sql)
        var indexes: [IndexInfo] = []
        
        for row in rows {
            let indexName = row.column("index_name")?.string ?? ""
            if let indexInfo = try await getIndexInfo(name: indexName, table: table, schema: schema) {
                indexes.append(indexInfo)
            }
        }
        
        return indexes
    }
}
