import Foundation

/// Client for PolyBase / data virtualization operations.
///
/// Provides listing and dropping of external data sources, external tables,
/// and external file formats. Also checks whether PolyBase is installed.
///
/// Usage:
/// ```swift
/// let installed = try await client.polyBase.isPolyBaseInstalled()
/// let sources = try await client.polyBase.listExternalDataSources(database: "MyDB")
/// ```
public final class SQLServerPolyBaseClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    /// Checks whether PolyBase is installed on this SQL Server instance.
    @available(macOS 12.0, *)
    public func isPolyBaseInstalled() async throws -> Bool {
        let sql = "SELECT ISNULL(SERVERPROPERTY('IsPolyBaseInstalled'), 0) AS installed"
        let rows = try await client.query(sql)
        return (rows.first?.column("installed")?.int ?? 0) != 0
    }

    // MARK: - External Data Sources

    /// Lists all external data sources in the database.
    @available(macOS 12.0, *)
    public func listExternalDataSources(database: String) async throws -> [ExternalDataSource] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT
            eds.name,
            eds.type_desc,
            eds.location,
            c.name AS credential_name,
            eds.database_name,
            eds.shard_map_name
        FROM \(db).sys.external_data_sources AS eds
        LEFT JOIN \(db).sys.database_scoped_credentials AS c ON c.credential_id = eds.credential_id
        ORDER BY eds.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ExternalDataSource(
                name: name,
                type: ExternalDataSourceType(fromDescription: row.column("type_desc")?.string ?? ""),
                location: row.column("location")?.string ?? "",
                credential: row.column("credential_name")?.string,
                databaseName: row.column("database_name")?.string,
                shardMapName: row.column("shard_map_name")?.string
            )
        }
    }

    /// Drops an external data source.
    @available(macOS 12.0, *)
    public func dropExternalDataSource(database: String, name: String) async throws {
        let db = Self.escapeIdentifier(database)
        let escaped = Self.escapeIdentifier(name)
        _ = try await client.execute("USE \(db); DROP EXTERNAL DATA SOURCE \(escaped)")
    }

    // MARK: - External Tables

    /// Lists all external tables in the database.
    @available(macOS 12.0, *)
    public func listExternalTables(database: String) async throws -> [ExternalTable] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT
            s.name AS schema_name,
            et.name,
            eds.name AS data_source_name,
            eff.name AS file_format_name,
            et.location
        FROM \(db).sys.external_tables AS et
        INNER JOIN \(db).sys.schemas AS s ON s.schema_id = et.schema_id
        INNER JOIN \(db).sys.external_data_sources AS eds ON eds.data_source_id = et.data_source_id
        LEFT JOIN \(db).sys.external_file_formats AS eff ON eff.file_format_id = et.file_format_id
        ORDER BY s.name, et.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard
                let schema = row.column("schema_name")?.string,
                let name = row.column("name")?.string
            else { return nil }
            return ExternalTable(
                schema: schema,
                name: name,
                dataSourceName: row.column("data_source_name")?.string ?? "",
                fileFormatName: row.column("file_format_name")?.string,
                location: row.column("location")?.string
            )
        }
    }

    /// Drops an external table.
    @available(macOS 12.0, *)
    public func dropExternalTable(database: String, schema: String, name: String) async throws {
        let db = Self.escapeIdentifier(database)
        let qualified = "\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(name))"
        _ = try await client.execute("USE \(db); DROP EXTERNAL TABLE \(qualified)")
    }

    // MARK: - External File Formats

    /// Lists all external file formats in the database.
    @available(macOS 12.0, *)
    public func listExternalFileFormats(database: String) async throws -> [ExternalFileFormat] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT
            name,
            format_type,
            field_terminator,
            string_delimiter
        FROM \(db).sys.external_file_formats
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return ExternalFileFormat(
                name: name,
                formatType: ExternalFileFormatType(fromDescription: row.column("format_type")?.string ?? ""),
                fieldTerminator: row.column("field_terminator")?.string,
                stringDelimiter: row.column("string_delimiter")?.string
            )
        }
    }

    /// Drops an external file format.
    @available(macOS 12.0, *)
    public func dropExternalFileFormat(database: String, name: String) async throws {
        let db = Self.escapeIdentifier(database)
        let escaped = Self.escapeIdentifier(name)
        _ = try await client.execute("USE \(db); DROP EXTERNAL FILE FORMAT \(escaped)")
    }
}
