import Foundation
import NIO

// MARK: - Change Tracking / CDC Types

/// A table that has Change Data Capture enabled.
public struct SQLServerCDCTable: Sendable, Equatable, Identifiable {
    public var id: String { "\(schemaName).\(tableName)" }

    public let schemaName: String
    public let tableName: String
    public let captureInstance: String?
    public let isTrackedByCDC: Bool

    public init(
        schemaName: String,
        tableName: String,
        captureInstance: String?,
        isTrackedByCDC: Bool
    ) {
        self.schemaName = schemaName
        self.tableName = tableName
        self.captureInstance = captureInstance
        self.isTrackedByCDC = isTrackedByCDC
    }
}

/// Change Tracking status at the database level.
public struct SQLServerChangeTrackingStatus: Sendable, Equatable {
    public let databaseName: String
    public let isAutoCleanupOn: Bool
    public let retentionPeriod: Int
    public let retentionPeriodUnits: String

    public init(
        databaseName: String,
        isAutoCleanupOn: Bool,
        retentionPeriod: Int,
        retentionPeriodUnits: String
    ) {
        self.databaseName = databaseName
        self.isAutoCleanupOn = isAutoCleanupOn
        self.retentionPeriod = retentionPeriod
        self.retentionPeriodUnits = retentionPeriodUnits
    }
}

// MARK: - SQLServerChangeTrackingClient

/// Namespace client for SQL Server Change Data Capture and Change Tracking.
///
/// Change Data Capture (CDC) tracks DML changes to tables and records them
/// in change tables. Change Tracking tracks which rows changed without
/// recording the actual data modifications.
///
/// Usage:
/// ```swift
/// let cdcTables = try await client.changeTracking.listCDCTables()
/// try await client.changeTracking.enableCDC(schema: "dbo", table: "Orders", roleName: "cdc_reader")
/// let status = try await client.changeTracking.changeTrackingStatus()
/// ```
public final class SQLServerChangeTrackingClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List CDC Tables

    /// Returns all tables and their CDC tracking status.
    @available(macOS 12.0, *)
    public func listCDCTables() async throws -> [SQLServerCDCTable] {
        let sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            ct.capture_instance,
            t.is_tracked_by_cdc
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
        WHERE t.is_tracked_by_cdc = 1
        ORDER BY s.name, t.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let schema = row.column("schema_name")?.string,
                  let table = row.column("table_name")?.string else { return nil }
            return SQLServerCDCTable(
                schemaName: schema,
                tableName: table,
                captureInstance: row.column("capture_instance")?.string,
                isTrackedByCDC: (row.column("is_tracked_by_cdc")?.int ?? 0) == 1
            )
        }
    }

    // MARK: - Enable CDC

    /// Enables Change Data Capture on a table.
    @available(macOS 12.0, *)
    public func enableCDC(
        schema: String,
        table: String,
        roleName: String? = nil
    ) async throws {
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        var sql = """
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'\(escapedSchema)',
            @source_name = N'\(escapedTable)'
        """
        if let roleName {
            let escapedRole = roleName.replacingOccurrences(of: "'", with: "''")
            sql += ", @role_name = N'\(escapedRole)'"
        } else {
            sql += ", @role_name = NULL"
        }
        _ = try await client.execute(sql)
    }

    // MARK: - Disable CDC

    /// Disables Change Data Capture on a table.
    @available(macOS 12.0, *)
    public func disableCDC(
        schema: String,
        table: String,
        captureInstance: String? = nil
    ) async throws {
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        var sql = """
        EXEC sys.sp_cdc_disable_table
            @source_schema = N'\(escapedSchema)',
            @source_name = N'\(escapedTable)'
        """
        if let captureInstance {
            let escaped = captureInstance.replacingOccurrences(of: "'", with: "''")
            sql += ", @capture_instance = N'\(escaped)'"
        } else {
            sql += ", @capture_instance = N'all'"
        }
        _ = try await client.execute(sql)
    }

    // MARK: - Change Tracking Status

    /// Returns Change Tracking configuration for the current database.
    @available(macOS 12.0, *)
    public func changeTrackingStatus() async throws -> [SQLServerChangeTrackingStatus] {
        let sql = """
        SELECT
            DB_NAME(database_id) AS db_name,
            is_auto_cleanup_on,
            retention_period,
            retention_period_units_desc
        FROM sys.change_tracking_databases
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let dbName = row.column("db_name")?.string else { return nil }
            return SQLServerChangeTrackingStatus(
                databaseName: dbName,
                isAutoCleanupOn: (row.column("is_auto_cleanup_on")?.int ?? 0) == 1,
                retentionPeriod: row.column("retention_period")?.int ?? 0,
                retentionPeriodUnits: row.column("retention_period_units_desc")?.string ?? "UNKNOWN"
            )
        }
    }

    // MARK: - Change Tracking Configuration

    /// Enables Change Tracking on a database.
    @available(macOS 12.0, *)
    public func enableChangeTracking(
        database: String,
        retentionPeriod: Int = 2,
        retentionUnit: String = "DAYS",
        autoCleanup: Bool = true
    ) async throws {
        let escaped = database.replacingOccurrences(of: "]", with: "]]")
        let sql = """
        ALTER DATABASE [\(escaped)] SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = \(retentionPeriod) \(retentionUnit), AUTO_CLEANUP = \(autoCleanup ? "ON" : "OFF"));
        """
        _ = try await client.execute(sql)
    }

    /// Disables Change Tracking on a database.
    @available(macOS 12.0, *)
    public func disableChangeTracking(database: String) async throws {
        let escaped = database.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER DATABASE [\(escaped)] SET CHANGE_TRACKING = OFF;")
    }

    // MARK: - Change Tracking Tables

    /// A table with Change Tracking enabled.
    public struct SQLServerCTTable: Sendable, Equatable, Identifiable {
        public var id: String { "\(schemaName).\(tableName)" }
        public let schemaName: String
        public let tableName: String
        public let isTrackColumnsUpdatedOn: Bool
        public let minValidVersion: Int64
        public let beginVersion: Int64
    }

    /// Returns all tables with Change Tracking enabled.
    @available(macOS 12.0, *)
    public func listChangeTrackingTables() async throws -> [SQLServerCTTable] {
        let sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            ct.is_track_columns_updated_on,
            ct.min_valid_version,
            ct.begin_version
        FROM sys.change_tracking_tables ct
        JOIN sys.tables t ON ct.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        ORDER BY s.name, t.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let schema = row.column("schema_name")?.string,
                  let table = row.column("table_name")?.string else { return nil }
            return SQLServerCTTable(
                schemaName: schema,
                tableName: table,
                isTrackColumnsUpdatedOn: (row.column("is_track_columns_updated_on")?.int ?? 0) == 1,
                minValidVersion: Int64(row.column("min_valid_version")?.int ?? 0),
                beginVersion: Int64(row.column("begin_version")?.int ?? 0)
            )
        }
    }

    /// Enables Change Tracking on a specific table.
    @available(macOS 12.0, *)
    public func enableTableChangeTracking(schema: String, table: String, trackColumnsUpdated: Bool = true) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        let sql = "ALTER TABLE [\(s)].[\(t)] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = \(trackColumnsUpdated ? "ON" : "OFF"));"
        _ = try await client.execute(sql)
    }

    /// Disables Change Tracking on a specific table.
    @available(macOS 12.0, *)
    public func disableTableChangeTracking(schema: String, table: String) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER TABLE [\(s)].[\(t)] DISABLE CHANGE_TRACKING;")
    }
}
