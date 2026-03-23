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
    public func rebuildIndexes(schema: String, table: String) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX ALL ON [\(escapedSchema)].[\(escapedTable)] REBUILD"
        do {
            _ = try await client.execute(sql)
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
    public func rebuildIndex(schema: String, table: String, name: String) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX [\(escapedName)] ON [\(escapedSchema)].[\(escapedTable)] REBUILD"
        do {
            _ = try await client.execute(sql)
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
    public func updateStatistics(schema: String, table: String) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "UPDATE STATISTICS [\(escapedSchema)].[\(escapedTable)]"
        do {
            _ = try await client.execute(sql)
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
    public func updateIndexStatistics(schema: String, table: String, index: String) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let escapedIndex = index.replacingOccurrences(of: "]", with: "]]")
        let sql = "UPDATE STATISTICS [\(escapedSchema)].[\(escapedTable)] [\(escapedIndex)] WITH FULLSCAN"
        do {
            _ = try await client.execute(sql)
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

    // MARK: - Reorganize Indexes

    /// Reorganizes a specific index on a table (lighter than rebuild).
    @available(macOS 12.0, *)
    public func reorganizeIndex(schema: String, table: String, name: String) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let escapedName = name.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX [\(escapedName)] ON [\(escapedSchema)].[\(escapedTable)] REORGANIZE"
        do {
            _ = try await client.execute(sql)
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
    public func reorganizeIndexes(schema: String, table: String) async throws -> SQLServerMaintenanceResult {
        let escapedSchema = schema.replacingOccurrences(of: "]", with: "]]")
        let escapedTable = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER INDEX ALL ON [\(escapedSchema)].[\(escapedTable)] REORGANIZE"
        do {
            _ = try await client.execute(sql)
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

    // MARK: - Health Information

    /// Retrieves health and configuration status for the current database.
    @available(macOS 12.0, *)
    public func getDatabaseHealth() async throws -> SQLServerDatabaseHealth {
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
        
        let rows = try await client.query(sql)
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
