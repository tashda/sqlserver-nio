import Foundation

// MARK: - Distribution Configuration Types

/// Configuration details for a SQL Server replication distributor.
public struct SQLServerDistributorConfiguration: Sendable, Equatable {
    public let distributorServer: String
    public let distributionDB: String
    public let workingDirectory: String?
    public let maxDistributionRetention: Int
    public let minDistributionRetention: Int

    public init(
        distributorServer: String,
        distributionDB: String,
        workingDirectory: String?,
        maxDistributionRetention: Int,
        minDistributionRetention: Int
    ) {
        self.distributorServer = distributorServer
        self.distributionDB = distributionDB
        self.workingDirectory = workingDirectory
        self.maxDistributionRetention = maxDistributionRetention
        self.minDistributionRetention = minDistributionRetention
    }
}

// MARK: - Distribution Setup

@available(macOS 12.0, *)
extension SQLServerReplicationClient {

    /// Fetches the current distributor configuration, or nil if not configured.
    public func distributorInfo() async throws -> SQLServerDistributorConfiguration? {
        let sql = "EXEC sp_helpdistributor"
        let rows = try await client.query(sql)
        guard let row = rows.first else { return nil }

        let server = row.column("distributor")?.string
        guard let server, !server.isEmpty else { return nil }

        return SQLServerDistributorConfiguration(
            distributorServer: server,
            distributionDB: row.column("distribution database")?.string ?? "distribution",
            workingDirectory: row.column("directory")?.string,
            maxDistributionRetention: row.column("max distrib retention")?.int ?? 72,
            minDistributionRetention: row.column("min distrib retention")?.int ?? 0
        )
    }

    /// Configures the local server as a distributor.
    /// Requires sysadmin role.
    public func configureDistributor(password: String) async throws {
        let escaped = password.replacingOccurrences(of: "'", with: "''")
        let sql = "EXEC sp_adddistributor @distributor = @@SERVERNAME, @password = N'\(escaped)'"
        _ = try await client.execute(sql)
    }

    /// Creates the distribution database.
    /// Must be called after `configureDistributor()`.
    public func configureDistributionDB(
        name: String = "distribution",
        snapshotFolder: String? = nil
    ) async throws {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        var sql = "EXEC sp_adddistributiondb @database = N'\(escapedName)'"
        if let folder = snapshotFolder {
            let escapedFolder = folder.replacingOccurrences(of: "'", with: "''")
            sql += ", @data_folder = N'\(escapedFolder)', @log_folder = N'\(escapedFolder)'"
        }
        _ = try await client.execute(sql)
    }

    /// Enables the local server as a publisher using the distribution database.
    /// Must be called after `configureDistributionDB()`.
    public func enablePublishing(
        distributionDB: String = "distribution",
        password: String
    ) async throws {
        let escapedDB = distributionDB.replacingOccurrences(of: "'", with: "''")
        let escapedPwd = password.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_adddistpublisher
            @publisher = @@SERVERNAME,
            @distribution_db = N'\(escapedDB)',
            @security_mode = 0,
            @login = N'sa',
            @password = N'\(escapedPwd)'
        """
        _ = try await client.execute(sql)
    }

    /// Removes the distributor configuration.
    /// - Parameter force: If true, drops even if publications/subscriptions exist.
    public func removeDistributor(force: Bool = false) async throws {
        // First remove the publisher
        _ = try? await client.execute("EXEC sp_dropdistpublisher @publisher = @@SERVERNAME")
        // Then drop the distribution database
        _ = try? await client.execute("EXEC sp_dropdistributiondb @database = N'distribution'")
        // Finally remove the distributor
        let sql = "EXEC sp_dropdistributor @no_checks = \(force ? 1 : 0)"
        _ = try await client.execute(sql)
    }
}
