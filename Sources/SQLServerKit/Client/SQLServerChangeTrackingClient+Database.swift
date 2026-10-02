import Foundation

@available(macOS 12.0, *)
extension SQLServerChangeTrackingClient {
    /// Turns Change Data Capture on for the connection's database (`sys.sp_cdc_enable_db`), which
    /// `enableCDC(schema:table:)` needs first. Needs SQL Server Agent for the capture job.
    public func enableDatabaseCDC() async throws {
        _ = try await client.execute("EXEC sys.sp_cdc_enable_db")
    }

    /// Turns Change Data Capture off for the connection's database, with every capture instance.
    public func disableDatabaseCDC() async throws {
        _ = try await client.execute("EXEC sys.sp_cdc_disable_db")
    }

    /// Whether CDC is on for a database (`sys.databases.is_cdc_enabled`).
    public func isDatabaseCDCEnabled(database: String) async throws -> Bool {
        let rows = try await client.query("SELECT is_cdc_enabled FROM sys.databases WHERE name = N'\(database.replacingOccurrences(of: "'", with: "''"))'")
        return rows.first?.column("is_cdc_enabled")?.bool ?? false
    }
}
