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
        let db = SQLServerSQL.escapeIdentifier(database)
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
        let db = SQLServerSQL.escapeIdentifier(database)
        let escaped = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("USE \(db); DROP EXTERNAL DATA SOURCE \(escaped)")
    }

    // MARK: - External Tables

    /// Lists all external tables in the database.
    @available(macOS 12.0, *)
    public func listExternalTables(database: String) async throws -> [ExternalTable] {
        let db = SQLServerSQL.escapeIdentifier(database)
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
        let db = SQLServerSQL.escapeIdentifier(database)
        let qualified = "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))"
        _ = try await client.execute("USE \(db); DROP EXTERNAL TABLE \(qualified)")
    }

    // MARK: - External File Formats

    /// Lists all external file formats in the database.
    @available(macOS 12.0, *)
    public func listExternalFileFormats(database: String) async throws -> [ExternalFileFormat] {
        let db = SQLServerSQL.escapeIdentifier(database)
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
        let db = SQLServerSQL.escapeIdentifier(database)
        let escaped = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("USE \(db); DROP EXTERNAL FILE FORMAT \(escaped)")
    }

    // MARK: - Create Operations

    /// Creates an external data source.
    @available(macOS 12.0, *)
    public func createExternalDataSource(
        database: String,
        name: String,
        location: String,
        type: ExternalDataSourceType? = nil,
        credential: String? = nil,
        resourceManagerLocation: String? = nil
    ) async throws {
        let db = SQLServerSQL.escapeIdentifier(database)
        var withParts = ["LOCATION = N'\(SQLServerSQL.escapeLiteral(location))'"]
        if let type, type != .unknown { withParts.append("TYPE = \(type.rawValue)") }
        if let cred = credential { withParts.append("CREDENTIAL = \(SQLServerSQL.escapeIdentifier(cred))") }
        if let rml = resourceManagerLocation { withParts.append("RESOURCE_MANAGER_LOCATION = N'\(SQLServerSQL.escapeLiteral(rml))'") }
        let sql = "USE \(db); CREATE EXTERNAL DATA SOURCE \(SQLServerSQL.escapeIdentifier(name)) WITH (\(withParts.joined(separator: ", ")))"
        _ = try await client.execute(sql)
    }

    /// Creates an external file format.
    @available(macOS 12.0, *)
    public func createExternalFileFormat(
        database: String,
        name: String,
        formatType: ExternalFileFormatType,
        fieldTerminator: String? = nil,
        stringDelimiter: String? = nil,
        firstRow: Int? = nil,
        dateFormat: String? = nil,
        useTypeDefault: Bool? = nil
    ) async throws {
        let db = SQLServerSQL.escapeIdentifier(database)
        var withParts = ["FORMAT_TYPE = \(formatType.rawValue)"]
        if let ft = fieldTerminator { withParts.append("FIELD_TERMINATOR = N'\(SQLServerSQL.escapeLiteral(ft))'") }
        if let sd = stringDelimiter { withParts.append("STRING_DELIMITER = N'\(SQLServerSQL.escapeLiteral(sd))'") }
        if let fr = firstRow { withParts.append("FIRST_ROW = \(fr)") }
        if let df = dateFormat { withParts.append("DATE_FORMAT = N'\(SQLServerSQL.escapeLiteral(df))'") }
        if let utd = useTypeDefault { withParts.append("USE_TYPE_DEFAULT = \(utd ? "TRUE" : "FALSE")") }
        let sql = "USE \(db); CREATE EXTERNAL FILE FORMAT \(SQLServerSQL.escapeIdentifier(name)) WITH (\(withParts.joined(separator: ", ")))"
        _ = try await client.execute(sql)
    }

    /// Creates an external table.
    @available(macOS 12.0, *)
    public func createExternalTable(
        database: String,
        schema: String = "dbo",
        name: String,
        columns: [(name: String, dataType: String)],
        location: String,
        dataSource: String,
        fileFormat: String? = nil,
        rejectType: String? = nil,
        rejectValue: Double? = nil
    ) async throws {
        guard !columns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one column is required")
        }
        let db = SQLServerSQL.escapeIdentifier(database)
        let qualified = "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))"
        let colDefs = columns.map { "\(SQLServerSQL.escapeIdentifier($0.name)) \($0.dataType)" }.joined(separator: ",\n    ")
        var withParts = [
            "LOCATION = N'\(SQLServerSQL.escapeLiteral(location))'",
            "DATA_SOURCE = \(SQLServerSQL.escapeIdentifier(dataSource))"
        ]
        if let ff = fileFormat { withParts.append("FILE_FORMAT = \(SQLServerSQL.escapeIdentifier(ff))") }
        if let rt = rejectType { withParts.append("REJECT_TYPE = \(rt)") }
        if let rv = rejectValue { withParts.append("REJECT_VALUE = \(rv)") }
        let sql = "USE \(db); CREATE EXTERNAL TABLE \(qualified) (\n    \(colDefs)\n) WITH (\(withParts.joined(separator: ", ")))"
        _ = try await client.execute(sql)
    }
}
