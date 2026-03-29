import Foundation
import SQLServerTDS

// MARK: - FILESTREAM & Containment Options

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {

    /// Fetches FILESTREAM options for a database from sys.database_filestream_options.
    public func fetchFilestreamOptions(database: String) async throws -> SQLServerFilestreamOptions {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = """
        USE \(escapedDb);
        SELECT
            COALESCE(directory_name, '') AS directory_name,
            non_transacted_access,
            non_transacted_access_desc
        FROM sys.database_filestream_options
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            return SQLServerFilestreamOptions(
                directoryName: "",
                nonTransactedAccess: 0,
                nonTransactedAccessDescription: "OFF"
            )
        }

        return SQLServerFilestreamOptions(
            directoryName: row.column("directory_name")?.string ?? "",
            nonTransactedAccess: row.column("non_transacted_access")?.int ?? 0,
            nonTransactedAccessDescription: row.column("non_transacted_access_desc")?.string ?? "OFF"
        )
    }

    /// Sets the FILESTREAM directory name for a database.
    @discardableResult
    public func setFilestreamDirectoryName(
        database: String,
        directoryName: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let escapedDir = directoryName.replacingOccurrences(of: "'", with: "''")
        let sql = "ALTER DATABASE \(escapedDb) SET FILESTREAM (DIRECTORY_NAME = N'\(escapedDir)')"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets the FILESTREAM non-transacted access level for a database.
    @discardableResult
    public func setFilestreamNonTransactedAccess(
        database: String,
        access: SQLServerFilestreamAccessLevel
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(escapedDb) SET FILESTREAM (NON_TRANSACTED_ACCESS = \(access.rawValue))"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Fetches containment-specific properties for a database.
    /// Returns nil if the database is not a contained database.
    public func fetchContainmentProperties(database: String) async throws -> SQLServerContainmentProperties? {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            d.containment,
            d.containment_desc,
            d.default_fulltext_language_lcid,
            d.default_fulltext_language_name,
            d.default_language_lcid,
            d.default_language_name,
            d.is_nested_triggers_on,
            d.is_transform_noise_words_on,
            d.two_digit_year_cutoff
        FROM sys.databases d
        WHERE d.name = N'\(escapedName)'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else { return nil }

        let containment = row.column("containment")?.int ?? 0
        if containment == 0 { return nil }

        return SQLServerContainmentProperties(
            containmentDescription: row.column("containment_desc")?.string ?? "NONE",
            defaultFulltextLanguageLCID: row.column("default_fulltext_language_lcid")?.int ?? 1033,
            defaultFulltextLanguageName: row.column("default_fulltext_language_name")?.string ?? "English",
            defaultLanguageLCID: row.column("default_language_lcid")?.int ?? 0,
            defaultLanguageName: row.column("default_language_name")?.string ?? "us_english",
            isNestedTriggersOn: (row.column("is_nested_triggers_on")?.int ?? 1) != 0,
            isTransformNoiseWordsOn: (row.column("is_transform_noise_words_on")?.int ?? 0) != 0,
            twoDigitYearCutoff: row.column("two_digit_year_cutoff")?.int ?? 2049
        )
    }

    /// Fetches the cursor default scope for a database.
    public func fetchCursorDefaults(database: String) async throws -> SQLServerCursorDefaults {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            d.is_local_cursor_default,
            d.is_cursor_close_on_commit_on
        FROM sys.databases d
        WHERE d.name = N'\(escapedName)'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            return SQLServerCursorDefaults(isLocalCursorDefault: false, isCursorCloseOnCommitOn: false)
        }

        return SQLServerCursorDefaults(
            isLocalCursorDefault: (row.column("is_local_cursor_default")?.int ?? 0) != 0,
            isCursorCloseOnCommitOn: (row.column("is_cursor_close_on_commit_on")?.int ?? 0) != 0
        )
    }

    /// Fetches Service Broker properties for a database.
    public func fetchServiceBrokerProperties(database: String) async throws -> SQLServerServiceBrokerProperties {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            d.is_broker_enabled,
            d.is_honor_broker_priority_on,
            CAST(d.service_broker_guid AS NVARCHAR(64)) AS service_broker_guid
        FROM sys.databases d
        WHERE d.name = N'\(escapedName)'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            return SQLServerServiceBrokerProperties(
                isBrokerEnabled: false,
                isHonorBrokerPriorityOn: false,
                serviceBrokerGUID: ""
            )
        }

        return SQLServerServiceBrokerProperties(
            isBrokerEnabled: (row.column("is_broker_enabled")?.int ?? 0) != 0,
            isHonorBrokerPriorityOn: (row.column("is_honor_broker_priority_on")?.int ?? 0) != 0,
            serviceBrokerGUID: row.column("service_broker_guid")?.string ?? ""
        )
    }
}
