import Foundation
import NIO

/// Namespace client for database performance tuning and diagnostics.
///
/// This client surfaces recommendations from SQL Server DMVs, such as missing indexes,
/// regressed queries (when Query Store is enabled), and index usage statistics.
public final class SQLServerTuningClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Missing Indexes

    /// Returns a list of the most impactful missing index recommendations for the current database.
    ///
    /// This uses `sys.dm_db_missing_index_*` DMVs to identify tables that are performing
    /// scans where a seek would be more efficient.
    ///
    /// - Parameter minImpact: Minimum average total user cost improvement to include (0-100).
    @available(macOS 12.0, *)
    public func listMissingIndexRecommendations(minImpact: Double = 10.0) async throws -> [SQLServerMissingIndexRecommendation] {
        let sql = """
        SELECT
            gs.group_handle,
            DB_NAME(id.database_id) AS [database_name],
            OBJECT_SCHEMA_NAME(id.object_id, id.database_id) AS [schema_name],
            OBJECT_NAME(id.object_id, id.database_id) AS [table_name],
            gs.avg_user_impact,
            gs.user_seeks,
            gs.user_scans,
            id.equality_columns,
            id.inequality_columns,
            id.included_columns
        FROM sys.dm_db_missing_index_group_stats gs
        JOIN sys.dm_db_missing_index_groups g ON gs.group_handle = g.index_group_handle
        JOIN sys.dm_db_missing_index_details id ON g.index_handle = id.index_handle
        WHERE gs.avg_user_impact >= \(minImpact)
        ORDER BY gs.avg_user_impact DESC, gs.user_seeks DESC
        """
        
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let handle = row.column("group_handle")?.int,
                  let database = row.column("database_name")?.string,
                  let schema = row.column("schema_name")?.string,
                  let table = row.column("table_name")?.string else {
                return nil
            }
            
            return SQLServerMissingIndexRecommendation(
                indexHandle: handle,
                databaseName: database,
                schemaName: schema,
                tableName: table,
                avgTotalUserCost: row.column("avg_user_impact")?.double ?? 0,
                userSeeks: row.column("user_seeks")?.int ?? 0,
                userScans: row.column("user_scans")?.int ?? 0,
                equalityColumns: parseColumnList(row.column("equality_columns")?.string),
                inequalityColumns: parseColumnList(row.column("inequality_columns")?.string),
                includedColumns: parseColumnList(row.column("included_columns")?.string)
            )
        }
    }

    private func parseColumnList(_ list: String?) -> [String] {
        guard let list = list, !list.isEmpty else { return [] }
        return list.components(separatedBy: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .map { $0.replacingOccurrences(of: "[", with: "").replacingOccurrences(of: "]", with: "") }
            .filter { !$0.isEmpty }
    }

    // MARK: - Index Usage Statistics

    /// Statistics about how an existing index is being used.
    public struct SQLServerIndexUsageStat: Sendable, Equatable, Identifiable {
        public var id: String { "\(schemaName).\(tableName).\(indexName)" }
        public let schemaName: String
        public let tableName: String
        public let indexName: String
        public let indexType: String
        public let userSeeks: Int64
        public let userScans: Int64
        public let userLookups: Int64
        public let userUpdates: Int64
        public let lastUserSeek: String?
        public let lastUserScan: String?
    }

    /// Returns index usage statistics for the current database.
    @available(macOS 12.0, *)
    public func indexUsageStats(minUpdates: Int = 0) async throws -> [SQLServerIndexUsageStat] {
        let sql = """
        SELECT
            s.name AS schema_name,
            o.name AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            ISNULL(us.user_seeks, 0) AS user_seeks,
            ISNULL(us.user_scans, 0) AS user_scans,
            ISNULL(us.user_lookups, 0) AS user_lookups,
            ISNULL(us.user_updates, 0) AS user_updates,
            CONVERT(VARCHAR(30), us.last_user_seek, 121) AS last_user_seek,
            CONVERT(VARCHAR(30), us.last_user_scan, 121) AS last_user_scan
        FROM sys.indexes i
        JOIN sys.objects o ON i.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        LEFT JOIN sys.dm_db_index_usage_stats us
            ON i.object_id = us.object_id AND i.index_id = us.index_id
            AND us.database_id = DB_ID()
        WHERE o.is_ms_shipped = 0 AND i.name IS NOT NULL
        ORDER BY ISNULL(us.user_updates, 0) DESC, ISNULL(us.user_seeks, 0) ASC
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let schema = row.column("schema_name")?.string,
                  let table = row.column("table_name")?.string,
                  let index = row.column("index_name")?.string else { return nil }
            return SQLServerIndexUsageStat(
                schemaName: schema,
                tableName: table,
                indexName: index,
                indexType: row.column("index_type")?.string ?? "",
                userSeeks: Int64(row.column("user_seeks")?.int ?? 0),
                userScans: Int64(row.column("user_scans")?.int ?? 0),
                userLookups: Int64(row.column("user_lookups")?.int ?? 0),
                userUpdates: Int64(row.column("user_updates")?.int ?? 0),
                lastUserSeek: row.column("last_user_seek")?.string,
                lastUserScan: row.column("last_user_scan")?.string
            )
        }
    }
}
