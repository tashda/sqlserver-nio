import Foundation
import NIO

/// Client for analyzing SQL Server object dependencies and generating scripts.
public final class SQLServerDependencyClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    /// Returns all scriptable objects in the current database.
    ///
    /// Includes tables (U), views (V), stored procedures (P), scalar/inline/table functions
    /// (FN, IF, TF), triggers (TR), synonyms (SN), table types (TT), and sequences (SO).
    @available(macOS 12.0, *)
    public func listAllObjects(database: String? = nil) async throws -> [SQLServerObjectIdentifier] {
        let prefix = database.map { "[\($0)]." } ?? ""
        let sql = """
        SELECT SCHEMA_NAME(o.schema_id) AS [schema], o.name, o.type
        FROM \(prefix)sys.objects o
        WHERE o.is_ms_shipped = 0
          AND o.type IN ('U','V','P','FN','IF','TF','TR','SN','TT','SO')
        ORDER BY [schema], o.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let schema = row.column("schema")?.string,
                  let name = row.column("name")?.string,
                  let type = row.column("type")?.string else { return nil }
            return SQLServerObjectIdentifier(schema: schema, name: name, type: type.trimmingCharacters(in: .whitespaces))
        }
    }

    /// Fetches all cross-object dependencies in the current database.
    @available(macOS 12.0, *)
    public func fetchDependencies(database: String? = nil) async throws -> [SQLServerScriptingDependency] {
        let prefix = database.map { "[\($0)]." } ?? ""
        let sql = """
        SELECT
            SCHEMA_NAME(d.schema_id) AS dep_schema, d.name AS dep_name, d.type AS dep_type,
            SCHEMA_NAME(r.schema_id) AS ref_schema, r.name AS ref_name, r.type AS ref_type
        FROM \(prefix)sys.sql_expression_dependencies sed
        JOIN \(prefix)sys.objects d ON sed.referencing_id = d.object_id
        JOIN \(prefix)sys.objects r ON sed.referenced_id = r.object_id
        WHERE d.is_ms_shipped = 0 AND r.is_ms_shipped = 0
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let depSchema = row.column("dep_schema")?.string,
                  let depName = row.column("dep_name")?.string,
                  let depType = row.column("dep_type")?.string,
                  let refSchema = row.column("ref_schema")?.string,
                  let refName = row.column("ref_name")?.string,
                  let refType = row.column("ref_type")?.string else { return nil }

            return SQLServerScriptingDependency(
                dependentObject: SQLServerObjectIdentifier(schema: depSchema, name: depName, type: depType.trimmingCharacters(in: .whitespaces)),
                referencedObject: SQLServerObjectIdentifier(schema: refSchema, name: refName, type: refType.trimmingCharacters(in: .whitespaces))
            )
        }
    }

    /// Builds a full dependency graph for the current database.
    @available(macOS 12.0, *)
    public func buildGraph(database: String? = nil) async throws -> SQLServerDependencyGraph {
        async let objects = listAllObjects(database: database)
        async let dependencies = fetchDependencies(database: database)
        return try await SQLServerDependencyGraph(objects: objects, dependencies: dependencies)
    }

    /// Returns the T-SQL definition of a programmable object via `OBJECT_DEFINITION()`.
    ///
    /// For tables, returns `nil` (tables need `scriptTableDDL` instead).
    @available(macOS 12.0, *)
    public func scriptObjectDDL(
        database: String? = nil,
        schema: String,
        name: String,
        objectType: String
    ) async throws -> String? {
        let prefix = database.map { "[\($0)]." } ?? ""
        let escapedSchema = SQLServerDependencyClient.escapeLiteral(schema)
        let escapedName = SQLServerDependencyClient.escapeLiteral(name)

        // For tables we generate CREATE TABLE from catalog metadata
        if objectType == "U" {
            return try await scriptTableDDL(database: database, schema: schema, name: name)
        }

        // For synonyms, fetch base_object_name from sys.synonyms
        if objectType == "SN" {
            let sql = """
            SELECT base_object_name
            FROM \(prefix)sys.synonyms
            WHERE SCHEMA_NAME(schema_id) = N'\(escapedSchema)' AND name = N'\(escapedName)'
            """
            let rows = try await client.query(sql)
            guard let base = rows.first?.column("base_object_name")?.string else { return nil }
            return "CREATE SYNONYM [\(schema)].[\(name)] FOR \(base);"
        }

        // For sequences, fetch metadata from sys.sequences
        if objectType == "SO" {
            let sql = """
            SELECT data_type = TYPE_NAME(system_type_id),
                   start_value, increment, minimum_value, maximum_value, is_cycling
            FROM \(prefix)sys.sequences
            WHERE SCHEMA_NAME(schema_id) = N'\(escapedSchema)' AND name = N'\(escapedName)'
            """
            let rows = try await client.query(sql)
            guard let row = rows.first else { return nil }
            let dataType = row.column("data_type")?.string ?? "bigint"
            let start = row.column("start_value")?.string ?? "1"
            let increment = row.column("increment")?.string ?? "1"
            let minVal = row.column("minimum_value")?.string
            let maxVal = row.column("maximum_value")?.string
            let cycling = (row.column("is_cycling")?.int ?? 0) == 1

            var ddl = "CREATE SEQUENCE [\(schema)].[\(name)] AS \(dataType) START WITH \(start) INCREMENT BY \(increment)"
            if let minVal { ddl += " MINVALUE \(minVal)" }
            if let maxVal { ddl += " MAXVALUE \(maxVal)" }
            if cycling { ddl += " CYCLE" } else { ddl += " NO CYCLE" }
            ddl += ";"
            return ddl
        }

        // All other programmable objects: use OBJECT_DEFINITION
        let sql = "SELECT OBJECT_DEFINITION(OBJECT_ID(N'[\(escapedSchema)].[\(escapedName)]')) AS [definition]"
        let rows = try await client.query(sql)
        return rows.first?.column("definition")?.string
    }

    // MARK: - Table DDL

    /// Generates a CREATE TABLE statement from catalog metadata.
    @available(macOS 12.0, *)
    private func scriptTableDDL(database: String? = nil, schema: String, name: String) async throws -> String? {
        let prefix = database.map { "[\($0)]." } ?? ""
        let escapedSchema = SQLServerDependencyClient.escapeLiteral(schema)
        let escapedName = SQLServerDependencyClient.escapeLiteral(name)

        // Fetch columns
        let columnSQL = """
        SELECT c.name, TYPE_NAME(c.user_type_id) AS type_name,
               c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity,
               dc.definition AS default_def
        FROM \(prefix)sys.columns c
        LEFT JOIN \(prefix)sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE c.object_id = OBJECT_ID(N'[\(escapedSchema)].[\(escapedName)]')
        ORDER BY c.column_id
        """
        let columns = try await client.query(columnSQL)
        guard !columns.isEmpty else { return nil }

        var lines: [String] = []
        for col in columns {
            let colName = col.column("name")?.string ?? ""
            let typeName = col.column("type_name")?.string ?? "nvarchar"
            let maxLen = col.column("max_length")?.int ?? 0
            let precision = col.column("precision")?.int ?? 0
            let scale = col.column("scale")?.int ?? 0
            let nullable = (col.column("is_nullable")?.int ?? 0) == 1
            let identity = (col.column("is_identity")?.int ?? 0) == 1
            let defaultDef = col.column("default_def")?.string

            var typeSpec = typeName
            let sizedTypes = Set(["varchar", "nvarchar", "char", "nchar", "binary", "varbinary"])
            let precisionTypes = Set(["decimal", "numeric"])
            if sizedTypes.contains(typeName.lowercased()) {
                typeSpec = maxLen == -1 ? "\(typeName)(MAX)" : "\(typeName)(\(maxLen))"
            } else if precisionTypes.contains(typeName.lowercased()) {
                typeSpec = "\(typeName)(\(precision), \(scale))"
            }

            var line = "    [\(colName)] \(typeSpec)"
            if identity { line += " IDENTITY(1,1)" }
            line += nullable ? " NULL" : " NOT NULL"
            if let def = defaultDef { line += " DEFAULT \(def)" }
            lines.append(line)
        }

        return "CREATE TABLE [\(schema)].[\(name)] (\n\(lines.joined(separator: ",\n"))\n);"
    }

    // MARK: - Helpers

    internal static func escapeLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}
