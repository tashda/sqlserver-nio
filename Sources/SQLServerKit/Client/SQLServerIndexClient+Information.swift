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

    /// Lists indexes with their fragmentation statistics across the current database.
    @available(macOS 12.0, *)
    public func listFragmentedIndexes(minFragmentationPercent: Double = 0.1) async throws -> [SQLServerIndexFragmentation] {
        let sql = """
        SELECT 
            s.name AS [schema_name],
            t.name AS [table_name],
            i.name AS [index_name],
            ips.avg_fragmentation_in_percent AS [fragmentation_percent],
            ips.page_count,
            ips.index_type_desc AS [index_type],
            ips.index_id,
            i.is_unique,
            i.is_primary_key,
            ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0) + ISNULL(ius.user_lookups, 0) AS [total_scans],
            ISNULL(ius.user_updates, 0) AS [total_updates],
            CAST((ips.page_count * 8.0) AS FLOAT) AS [size_kb],
            CAST((SELECT SUM(a.total_pages) * 8.0 FROM sys.partitions p JOIN sys.allocation_units a ON p.partition_id = a.container_id WHERE p.object_id = t.object_id) AS FLOAT) AS [table_size_kb],
            STATS_DATE(i.object_id, i.index_id) AS [last_stats_update]
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
        JOIN sys.tables AS t ON ips.object_id = t.object_id
        JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        LEFT JOIN sys.dm_db_index_usage_stats AS ius ON ius.database_id = DB_ID() AND ius.object_id = i.object_id AND ius.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent >= \(minFragmentationPercent)
        ORDER BY ips.avg_fragmentation_in_percent DESC;
        """
        
        let rows = try await client.query(sql)
        return rows.compactMap { row -> SQLServerIndexFragmentation? in
            guard let schemaName = row.column("schema_name")?.string,
                  let tableName = row.column("table_name")?.string,
                  let indexName = row.column("index_name")?.string else { return nil }

            let fragPercent = row.column("fragmentation_percent")?.double ?? 0
            let pages = row.column("page_count")?.int64 ?? 0
            let type = row.column("index_type")?.string ?? "UNKNOWN"
            let idxId = row.column("index_id")?.int ?? 0
            let unique = (row.column("is_unique")?.int ?? 0) != 0
            let pk = (row.column("is_primary_key")?.int ?? 0) != 0
            let scans = row.column("total_scans")?.int64 ?? 0
            let updates = row.column("total_updates")?.int64 ?? 0
            let sizeKB = row.column("size_kb")?.double ?? 0
            let tableSizeKB = row.column("table_size_kb")?.double ?? 0
            let statsDate = row.column("last_stats_update")?.date

            return SQLServerIndexFragmentation(
                schemaName: schemaName,
                tableName: tableName,
                indexName: indexName,
                fragmentationPercent: fragPercent,
                pageCount: pages,
                indexType: type,
                indexId: idxId,
                isUnique: unique,
                isPrimaryKey: pk,
                totalScans: scans,
                totalUpdates: updates,
                sizeKB: sizeKB,
                tableSizeKB: tableSizeKB,
                lastStatsUpdate: statsDate
            )
        }
    }
}

/// Represents fragmentation and usage statistics for a SQL Server index.
public struct SQLServerIndexFragmentation: Sendable, Identifiable {
    public var id: String { "\(schemaName).\(tableName).\(indexName)" }
    
    public let schemaName: String
    public let tableName: String
    public let indexName: String
    public let fragmentationPercent: Double
    public let pageCount: Int64
    public let indexType: String
    public let indexId: Int
    public let isUnique: Bool
    public let isPrimaryKey: Bool
    public let totalScans: Int64
    public let totalUpdates: Int64
    public let sizeKB: Double
    public let tableSizeKB: Double
    public let lastStatsUpdate: Date?
}
