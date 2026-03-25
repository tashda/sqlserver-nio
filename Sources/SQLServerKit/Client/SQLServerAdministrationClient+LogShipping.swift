import Foundation
import SQLServerTDS

// MARK: - Transaction Log Shipping

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {

    /// Fetches the log shipping configuration for a primary database.
    /// Returns nil if log shipping is not configured for this database.
    public func fetchLogShippingConfig(database: String) async throws -> SQLServerLogShippingConfig? {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")

        // Check primary configuration
        let primarySQL = """
        SELECT
            lsp.primary_id,
            lsp.primary_database,
            lsp.backup_directory,
            lsp.backup_share,
            lsp.backup_retention_period,
            lsp.backup_compression,
            lsp.monitor_server,
            lsp.monitor_server_security_mode,
            CONVERT(VARCHAR(23), lsp.last_backup_date, 121) AS last_backup_date,
            COALESCE(lsp.last_backup_file, '') AS last_backup_file
        FROM msdb.dbo.log_shipping_primary_databases lsp
        WHERE lsp.primary_database = N'\(escapedName)'
        """

        let primaryRows = try await client.query(primarySQL)
        guard let primaryRow = primaryRows.first else {
            return nil
        }

        // Fetch secondaries
        let primaryID = primaryRow.column("primary_id")?.string ?? ""
        let escapedPrimaryID = primaryID.replacingOccurrences(of: "'", with: "''")
        let secondarySQL = """
        SELECT
            lss.secondary_server,
            lss.secondary_database,
            CONVERT(VARCHAR(23), lss.last_copied_date, 121) AS last_copied_date,
            CONVERT(VARCHAR(23), lss.last_restored_date, 121) AS last_restored_date
        FROM msdb.dbo.log_shipping_primary_secondaries lss
        WHERE lss.primary_id = N'\(escapedPrimaryID)'
        """

        let secondaryRows = try await client.query(secondarySQL)
        let secondaries = secondaryRows.compactMap { row -> SQLServerLogShippingSecondary? in
            guard let server = row.column("secondary_server")?.string,
                  let db = row.column("secondary_database")?.string else { return nil }
            return SQLServerLogShippingSecondary(
                secondaryServer: server,
                secondaryDatabase: db,
                lastCopiedDate: row.column("last_copied_date")?.string,
                lastRestoredDate: row.column("last_restored_date")?.string
            )
        }

        return SQLServerLogShippingConfig(
            primaryID: primaryID,
            primaryDatabase: primaryRow.column("primary_database")?.string ?? database,
            backupDirectory: primaryRow.column("backup_directory")?.string ?? "",
            backupShare: primaryRow.column("backup_share")?.string ?? "",
            backupRetentionPeriodMinutes: primaryRow.column("backup_retention_period")?.int ?? 0,
            backupCompression: (primaryRow.column("backup_compression")?.int ?? 0) != 0,
            monitorServer: primaryRow.column("monitor_server")?.string,
            monitorServerSecurityMode: primaryRow.column("monitor_server_security_mode")?.int ?? 0,
            lastBackupDate: primaryRow.column("last_backup_date")?.string,
            lastBackupFile: primaryRow.column("last_backup_file")?.string ?? "",
            secondaries: secondaries
        )
    }

    /// Fetches the log shipping secondary configuration for a database that is a secondary.
    /// Returns nil if this database is not a log shipping secondary.
    public func fetchLogShippingSecondaryConfig(database: String) async throws -> SQLServerLogShippingSecondaryConfig? {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            lssd.secondary_id,
            lssd.secondary_database,
            ls.primary_server,
            ls.primary_database,
            lssd.restore_delay,
            lssd.restore_mode,
            lssd.disconnect_users,
            CONVERT(VARCHAR(23), lssd.last_restored_date, 121) AS last_restored_date,
            COALESCE(lssd.last_restored_file, '') AS last_restored_file
        FROM msdb.dbo.log_shipping_secondary_databases lssd
        JOIN msdb.dbo.log_shipping_secondary ls ON lssd.secondary_id = ls.secondary_id
        WHERE lssd.secondary_database = N'\(escapedName)'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else { return nil }

        return SQLServerLogShippingSecondaryConfig(
            secondaryID: row.column("secondary_id")?.string ?? "",
            secondaryDatabase: row.column("secondary_database")?.string ?? database,
            primaryServer: row.column("primary_server")?.string ?? "",
            primaryDatabase: row.column("primary_database")?.string ?? "",
            restoreDelayMinutes: row.column("restore_delay")?.int ?? 0,
            restoreMode: row.column("restore_mode")?.int ?? 0,
            disconnectUsers: (row.column("disconnect_users")?.int ?? 0) != 0,
            lastRestoredDate: row.column("last_restored_date")?.string,
            lastRestoredFile: row.column("last_restored_file")?.string ?? ""
        )
    }
}
