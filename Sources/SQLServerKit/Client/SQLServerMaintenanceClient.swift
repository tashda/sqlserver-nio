import Foundation
import Logging
import NIO

// MARK: - Maintenance Types

/// Result of a maintenance operation.
public struct SQLServerMaintenanceResult: Sendable, Equatable {
    public let operation: String
    public let messages: [String]
    public let succeeded: Bool

    public init(operation: String, messages: [String], succeeded: Bool) {
        self.operation = operation
        self.messages = messages
        self.succeeded = succeeded
    }
}

/// Options for DBCC SHRINKDATABASE / SHRINKFILE.
public enum SQLServerShrinkOption: Sendable {
    case defaultBehavior
    case noTruncate
    case truncateOnly
}

/// Result of a shrink operation with page-level detail.
public struct SQLServerShrinkResult: Sendable {
    public let dbId: Int
    public let fileId: Int
    public let currentSizePages: Int
    public let minimumSizePages: Int
    public let usedPages: Int
    public let estimatedPages: Int

    public init(dbId: Int, fileId: Int, currentSizePages: Int, minimumSizePages: Int, usedPages: Int, estimatedPages: Int) {
        self.dbId = dbId
        self.fileId = fileId
        self.currentSizePages = currentSizePages
        self.minimumSizePages = minimumSizePages
        self.usedPages = usedPages
        self.estimatedPages = estimatedPages
    }

    /// Current size in MB (pages are 8KB each).
    public var currentSizeMB: Double { Double(currentSizePages) * 8.0 / 1024.0 }
    /// Minimum size in MB.
    public var minimumSizeMB: Double { Double(minimumSizePages) * 8.0 / 1024.0 }
}

// MARK: - SQLServerMaintenanceClient

/// Namespace client for common SQL Server maintenance operations.
///
/// Provides direct execution of frequently used maintenance tasks such as
/// rebuilding indexes, updating statistics, checking database integrity,
/// and shrinking databases.
///
/// Usage:
/// ```swift
/// let result = try await client.maintenance.rebuildIndexes(schema: "dbo", table: "Orders")
/// let result = try await client.maintenance.checkDatabase(database: "MyDB")
/// ```
public final class SQLServerMaintenanceClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Rebuild Indexes

    /// Rebuilds all indexes on a table.
    @available(macOS 12.0, *)
    public func rebuildIndexes(schema: String, table: String, database: String? = nil) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX ALL ON [\(escapedSchema)].[\(escapedTable)] REBUILD"
        do {
            if let database {
                _ = try await client.withDatabase(database) { connection in
                    try await connection.execute(sql).get()
                }
            } else {
                _ = try await client.execute(sql)
            }
            return SQLServerMaintenanceResult(
                operation: "Rebuild Indexes",
                messages: ["All indexes on [\(schema)].[\(table)] rebuilt successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Rebuild Indexes' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Rebuild Indexes",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    /// Rebuilds a specific index on a table.
    @available(macOS 12.0, *)
    public func rebuildIndex(schema: String, table: String, name: String, database: String? = nil) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX [\(escapedName)] ON [\(escapedSchema)].[\(escapedTable)] REBUILD"
        do {
            if let database {
                _ = try await client.withDatabase(database) { connection in
                    try await connection.execute(sql).get()
                }
            } else {
                _ = try await client.execute(sql)
            }
            return SQLServerMaintenanceResult(
                operation: "Rebuild Index",
                messages: ["Index [\(name)] on [\(schema)].[\(table)] rebuilt successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Rebuild Index' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Rebuild Index",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    // MARK: - Update Statistics

    /// Updates statistics on a table.
    @available(macOS 12.0, *)
    public func updateStatistics(schema: String, table: String, database: String? = nil) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "UPDATE STATISTICS [\(escapedSchema)].[\(escapedTable)]"
        do {
            if let database {
                _ = try await client.withDatabase(database) { connection in
                    try await connection.execute(sql).get()
                }
            } else {
                _ = try await client.execute(sql)
            }
            return SQLServerMaintenanceResult(
                operation: "Update Statistics",
                messages: ["Statistics on [\(schema)].[\(table)] updated successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Update Statistics' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Update Statistics",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    /// Updates statistics on a specific index.
    @available(macOS 12.0, *)
    public func updateIndexStatistics(schema: String, table: String, index: String, database: String? = nil) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let escapedIndex = index.replacingOccurrences(of: "]", with: "]]")
        let sql = "UPDATE STATISTICS [\(escapedSchema)].[\(escapedTable)] [\(escapedIndex)] WITH FULLSCAN"
        do {
            if let database {
                _ = try await client.withDatabase(database) { connection in
                    try await connection.execute(sql).get()
                }
            } else {
                _ = try await client.execute(sql)
            }
            return SQLServerMaintenanceResult(
                operation: "Update Statistics",
                messages: ["Statistics on index [\(index)] for [\(schema)].[\(table)] updated successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Update Index Statistics' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Update Statistics",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    // MARK: - Check Database Integrity

    /// Runs DBCC CHECKDB on the specified database.
    @available(macOS 12.0, *)
    public func checkDatabase(database: String) async throws -> SQLServerMaintenanceResult {
        let escaped = database.replacingOccurrences(of: "]", with: "]]")
        let sql = "DBCC CHECKDB ([\(escaped)]) WITH NO_INFOMSGS"
        do {
            _ = try await client.execute(sql)
            return SQLServerMaintenanceResult(
                operation: "Check Database",
                messages: ["CHECKDB completed for [\(database)] with no errors."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Check Database' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Check Database",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    // MARK: - Shrink Database

    /// Shrinks the specified database.
    @available(macOS 12.0, *)
    public func shrinkDatabase(database: String) async throws -> SQLServerMaintenanceResult {
        let escaped = database.replacingOccurrences(of: "]", with: "]]")
        let sql = "DBCC SHRINKDATABASE ([\(escaped)])"
        do {
            _ = try await client.execute(sql)
            return SQLServerMaintenanceResult(
                operation: "Shrink Database",
                messages: ["Database [\(database)] shrunk successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Shrink Database' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Shrink Database",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    /// Shrinks the specified database with target percentage and option.
    @available(macOS 12.0, *)
    public func shrinkDatabase(database: String, targetPercent: Int, option: SQLServerShrinkOption = .defaultBehavior) async throws -> SQLServerMaintenanceResult {
        let escaped = database.replacingOccurrences(of: "]", with: "]]")
        var sql = "DBCC SHRINKDATABASE ([\(escaped)], \(targetPercent)"
        switch option {
        case .defaultBehavior: break
        case .noTruncate: sql += ", NOTRUNCATE"
        case .truncateOnly: sql += ", TRUNCATEONLY"
        }
        sql += ")"
        do {
            _ = try await client.execute(sql)
            return SQLServerMaintenanceResult(
                operation: "Shrink Database",
                messages: ["Database [\(database)] shrunk successfully with target \(targetPercent)%."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Shrink Database' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Shrink Database",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    /// Shrinks a specific database file.
    @available(macOS 12.0, *)
    public func shrinkFile(database: String, fileName: String, targetSizeMB: Int = 0, option: SQLServerShrinkOption = .defaultBehavior) async throws -> SQLServerMaintenanceResult {
        let escapedDb = database.replacingOccurrences(of: "]", with: "]]")
        let escapedFile = fileName.replacingOccurrences(of: "'", with: "''")
        var sql = "USE [\(escapedDb)]; DBCC SHRINKFILE(N'\(escapedFile)', \(targetSizeMB)"
        switch option {
        case .defaultBehavior: break
        case .noTruncate: sql += ", NOTRUNCATE"
        case .truncateOnly: sql += ", TRUNCATEONLY"
        }
        sql += ")"
        do {
            _ = try await client.execute(sql)
            return SQLServerMaintenanceResult(
                operation: "Shrink File",
                messages: ["File '\(fileName)' in [\(database)] shrunk successfully to target \(targetSizeMB) MB."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Shrink File' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Shrink File",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    // MARK: - Reorganize Indexes

    /// Reorganizes a specific index on a table (lighter than rebuild).
    @available(macOS 12.0, *)
    public func reorganizeIndex(schema: String, table: String, name: String, database: String? = nil) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX [\(escapedName)] ON [\(escapedSchema)].[\(escapedTable)] REORGANIZE"
        do {
            if let database {
                _ = try await client.withDatabase(database) { connection in
                    try await connection.execute(sql).get()
                }
            } else {
                _ = try await client.execute(sql)
            }
            return SQLServerMaintenanceResult(
                operation: "Reorganize Index",
                messages: ["Index [\(name)] on [\(schema)].[\(table)] reorganized successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Reorganize Index' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Reorganize Index",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    /// Reorganizes all indexes on a table (lighter than rebuild).
    @available(macOS 12.0, *)
    public func reorganizeIndexes(schema: String, table: String, database: String? = nil) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX ALL ON [\(escapedSchema)].[\(escapedTable)] REORGANIZE"
        do {
            if let database {
                _ = try await client.withDatabase(database) { connection in
                    try await connection.execute(sql).get()
                }
            } else {
                _ = try await client.execute(sql)
            }
            return SQLServerMaintenanceResult(
                operation: "Reorganize Indexes",
                messages: ["All indexes on [\(schema)].[\(table)] reorganized successfully."],
                succeeded: true
            )
        } catch {
            client.logger.warning("Maintenance operation 'Reorganize Indexes' failed: \(error)")
            return SQLServerMaintenanceResult(
                operation: "Reorganize Indexes",
                messages: [error.localizedDescription],
                succeeded: false
            )
        }
    }

    // MARK: - Table Stats

    /// Lists space and statistics information for all user tables in the current (or specified) database.
    @available(macOS 12.0, *)
    public func listTableStats(database: String? = nil) async throws -> [SQLServerTableStat] {
        let sql = """
        SELECT s.name AS schema_name, t.name AS table_name,
               CASE WHEN i.index_id = 0 THEN 'Heap' ELSE 'Clustered' END AS table_type,
               SUM(p.rows) AS row_count, SUM(a.data_pages) * 8 AS data_space_kb,
               (SUM(a.used_pages) - SUM(a.data_pages)) * 8 AS index_space_kb,
               (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS unused_space_kb,
               SUM(a.total_pages) * 8 AS total_space_kb,
               (SELECT MAX(sp.last_updated) FROM sys.stats st
                CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
                WHERE st.object_id = t.object_id) AS last_stats_update
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE t.is_ms_shipped = 0
        GROUP BY s.name, t.name, i.index_id, i.object_id, t.object_id
        ORDER BY SUM(p.rows) DESC
        """

        let rows: [SQLServerRow]
        if let database {
            rows = try await client.withDatabase(database) { connection in
                try await connection.query(sql).get()
            }
        } else {
            rows = try await client.query(sql)
        }

        return rows.map { row in
            SQLServerTableStat(
                schemaName: row.column("schema_name")?.string ?? "",
                tableName: row.column("table_name")?.string ?? "",
                tableType: row.column("table_type")?.string ?? "Heap",
                rowCount: Int64(row.column("row_count")?.int ?? 0),
                dataSpaceKB: Int64(row.column("data_space_kb")?.int ?? 0),
                indexSpaceKB: Int64(row.column("index_space_kb")?.int ?? 0),
                unusedSpaceKB: Int64(row.column("unused_space_kb")?.int ?? 0),
                totalSpaceKB: Int64(row.column("total_space_kb")?.int ?? 0),
                lastStatsUpdate: row.column("last_stats_update")?.string
            )
        }
    }

    // MARK: - Check Table

    /// Runs DBCC CHECKTABLE on the specified table.
    @available(macOS 12.0, *)
    @discardableResult
    public func checkTable(schema: String = "dbo", table: String, database: String? = nil) async throws -> [SQLServerStreamMessage] {
        let objectName = SQLServerSQL.escapeLiteral("\(schema).\(table)")
        let sql = "DBCC CHECKTABLE('\(objectName)') WITH NO_INFOMSGS"

        if let database {
            return try await client.withDatabase(database) { connection in
                let result = try await connection.execute(sql).get()
                return result.messages
            }
        } else {
            let result = try await client.execute(sql)
            return result.messages
        }
    }

    // MARK: - Rebuild Table

    /// Rebuilds a table (heap or clustered index rebuild).
    @available(macOS 12.0, *)
    @discardableResult
    public func rebuildTable(schema: String = "dbo", table: String, database: String? = nil) async throws -> [SQLServerStreamMessage] {
        let escapedSchema = SQLServerSQL.escapeIdentifier(schema)
        let escapedTable = SQLServerSQL.escapeIdentifier(table)
        let sql = "ALTER TABLE \(escapedSchema).\(escapedTable) REBUILD"

        if let database {
            return try await client.withDatabase(database) { connection in
                let result = try await connection.execute(sql).get()
                return result.messages
            }
        } else {
            let result = try await client.execute(sql)
            return result.messages
        }
    }

    // MARK: - Health Information

    /// Retrieves health and configuration status for the current database.
    @available(macOS 12.0, *)
    public func getDatabaseHealth(database: String? = nil) async throws -> SQLServerDatabaseHealth {
        let sql = """
        SELECT
            d.name,
            SUSER_SNAME(d.owner_sid) AS [owner],
            d.create_date,
            SUM(mf.size) * 8 / 1024.0 AS size_mb,
            d.recovery_model_desc,
            d.state_desc,
            d.compatibility_level,
            d.collation_name
        FROM sys.databases d
        JOIN sys.master_files mf ON d.database_id = mf.database_id
        WHERE d.database_id = DB_ID()
        GROUP BY d.name, d.owner_sid, d.create_date, d.recovery_model_desc, d.state_desc, d.compatibility_level, d.collation_name;
        """

        let rows: [SQLServerRow]
        if let database {
            rows = try await client.withDatabase(database) { connection in
                try await connection.query(sql).get()
            }
        } else {
            rows = try await client.query(sql)
        }
        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "Could not retrieve health stats for database.")
        }
        
        return SQLServerDatabaseHealth(
            name: row.column("name")?.string ?? "",
            owner: row.column("owner")?.string ?? "Unknown",
            createDate: row.column("create_date")?.date ?? Date(),
            sizeMB: row.column("size_mb")?.double ?? 0,
            recoveryModel: row.column("recovery_model_desc")?.string ?? "Unknown",
            status: row.column("state_desc")?.string ?? "Unknown",
            compatibilityLevel: row.column("compatibility_level")?.int ?? 0,
            collationName: row.column("collation_name")?.string
        )
    }
}
