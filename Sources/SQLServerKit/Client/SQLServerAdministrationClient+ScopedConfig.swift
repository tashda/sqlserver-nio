import Foundation
import SQLServerTDS

// MARK: - Database Scoped Configurations

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {

    /// Fetches all database scoped configurations from sys.database_scoped_configurations.
    /// Available on SQL Server 2016+ (compatibility level 130+).
    public func listScopedConfigurations(database: String) async throws -> [SQLServerScopedConfiguration] {
        let escapedDb = Self.escapeIdentifier(database)
        let sql = """
        USE \(escapedDb);
        SELECT
            configuration_id,
            name,
            CAST(value AS NVARCHAR(256)) AS value,
            CAST(value_for_secondary AS NVARCHAR(256)) AS value_for_secondary
        FROM sys.database_scoped_configurations
        ORDER BY name
        """

        let rows = try await client.query(sql)
        return rows.compactMap { row -> SQLServerScopedConfiguration? in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerScopedConfiguration(
                configurationID: row.column("configuration_id")?.int ?? 0,
                name: name,
                value: row.column("value")?.string ?? "",
                valueForSecondary: row.column("value_for_secondary")?.string
            )
        }
    }

    /// Sets a database scoped configuration value.
    /// Uses ALTER DATABASE SCOPED CONFIGURATION SET.
    @discardableResult
    public func alterScopedConfiguration(
        database: String,
        name: String,
        value: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        // Scoped config names are not identifiers — they are keywords used directly
        let sql = """
        USE \(escapedDb);
        ALTER DATABASE SCOPED CONFIGURATION SET \(name) = \(value);
        """
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets a database scoped configuration value for secondary replicas.
    @discardableResult
    public func alterScopedConfigurationForSecondary(
        database: String,
        name: String,
        value: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let sql = """
        USE \(escapedDb);
        ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET \(name) = \(value);
        """
        let result = try await client.execute(sql)
        return result.messages
    }
}
