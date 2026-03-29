import Foundation
import NIO

// MARK: - Full-Text Search Types

/// A full-text catalog on the server.
public struct SQLServerFullTextCatalog: Sendable, Equatable, Identifiable {
    public var id: String { name }

    public let catalogID: Int
    public let name: String
    public let isDefault: Bool
    public let isAccentSensitive: Bool

    public init(
        catalogID: Int,
        name: String,
        isDefault: Bool,
        isAccentSensitive: Bool
    ) {
        self.catalogID = catalogID
        self.name = name
        self.isDefault = isDefault
        self.isAccentSensitive = isAccentSensitive
    }
}

/// A full-text index on a table.
public struct SQLServerFullTextIndex: Sendable, Equatable, Identifiable {
    public var id: String { tableName }

    public let tableName: String
    public let catalogID: Int
    public let isEnabled: Bool

    public init(tableName: String, catalogID: Int, isEnabled: Bool) {
        self.tableName = tableName
        self.catalogID = catalogID
        self.isEnabled = isEnabled
    }
}

// MARK: - SQLServerFullTextClient

/// Namespace client for SQL Server Full-Text Search management.
///
/// Full-Text Search enables efficient text-based queries on character data
/// in SQL Server tables. This client provides read-only APIs for listing
/// full-text catalogs and indexes.
///
/// Usage:
/// ```swift
/// let catalogs = try await client.fullText.listCatalogs()
/// let indexes = try await client.fullText.listIndexes()
/// ```
public final class SQLServerFullTextClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List Catalogs

    /// Returns all full-text catalogs in the current database.
    @available(macOS 12.0, *)
    public func listCatalogs() async throws -> [SQLServerFullTextCatalog] {
        let sql = """
        SELECT fulltext_catalog_id, name, is_default, is_accent_sensitivity_on
        FROM sys.fulltext_catalogs
        ORDER BY name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerFullTextCatalog(
                catalogID: row.column("fulltext_catalog_id")?.int ?? 0,
                name: name,
                isDefault: (row.column("is_default")?.int ?? 0) == 1,
                isAccentSensitive: (row.column("is_accent_sensitivity_on")?.int ?? 0) == 1
            )
        }
    }

    // MARK: - List Indexes

    /// Returns all full-text indexes in the current database.
    @available(macOS 12.0, *)
    public func listIndexes() async throws -> [SQLServerFullTextIndex] {
        let sql = """
        SELECT
            OBJECT_NAME(object_id) AS table_name,
            fulltext_catalog_id,
            is_enabled
        FROM sys.fulltext_indexes
        ORDER BY OBJECT_NAME(object_id)
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let tableName = row.column("table_name")?.string else { return nil }
            return SQLServerFullTextIndex(
                tableName: tableName,
                catalogID: row.column("fulltext_catalog_id")?.int ?? 0,
                isEnabled: (row.column("is_enabled")?.int ?? 0) == 1
            )
        }
    }

    // MARK: - Catalog Management

    /// Creates a new full-text catalog.
    @available(macOS 12.0, *)
    public func createCatalog(name: String, isDefault: Bool = false, accentSensitive: Bool = true) async throws {
        let escaped = name.replacingOccurrences(of: "]", with: "]]")
        var sql = "CREATE FULLTEXT CATALOG [\(escaped)]"
        if isDefault { sql += " AS DEFAULT" }
        sql += " WITH ACCENT_SENSITIVITY = \(accentSensitive ? "ON" : "OFF");"
        _ = try await client.execute(sql)
    }

    /// Drops a full-text catalog.
    @available(macOS 12.0, *)
    public func dropCatalog(name: String) async throws {
        let escaped = name.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("DROP FULLTEXT CATALOG [\(escaped)];")
    }

    /// Rebuilds a full-text catalog (repopulates all indexes).
    @available(macOS 12.0, *)
    public func rebuildCatalog(name: String) async throws {
        let escaped = name.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER FULLTEXT CATALOG [\(escaped)] REBUILD;")
    }

    // MARK: - Index Management

    /// Creates a full-text index on a table.
    ///
    /// A table can have only one full-text index. The table must have a unique,
    /// single-column, non-nullable index to serve as the full-text key.
    ///
    /// - Parameters:
    ///   - schema: The schema name (e.g., "dbo").
    ///   - table: The table name.
    ///   - keyIndex: The name of the unique index on the table to use as the full-text key.
    ///   - catalogName: The full-text catalog to associate the index with. Pass `nil` to use the default catalog.
    ///   - columns: Column names to include in the full-text index.
    ///   - changeTracking: Change tracking mode. Defaults to `auto`.
    @available(macOS 12.0, *)
    public func createIndex(
        schema: String,
        table: String,
        keyIndex: String,
        catalogName: String? = nil,
        columns: [String],
        changeTracking: ChangeTrackingMode = .auto
    ) async throws {
        guard !columns.isEmpty else { return }
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        let k = keyIndex.replacingOccurrences(of: "]", with: "]]")
        let colList = columns.map { col in
            let c = col.replacingOccurrences(of: "]", with: "]]")
            return "[\(c)]"
        }.joined(separator: ", ")

        var sql = "CREATE FULLTEXT INDEX ON [\(s)].[\(t)] (\(colList)) KEY INDEX [\(k)]"
        if let catalog = catalogName {
            let cat = catalog.replacingOccurrences(of: "]", with: "]]")
            sql += " ON [\(cat)]"
        }
        sql += " WITH CHANGE_TRACKING = \(changeTracking.rawValue);"
        _ = try await client.execute(sql)
    }

    /// Change tracking mode for full-text indexes.
    public enum ChangeTrackingMode: String, Sendable {
        case auto = "AUTO"
        case manual = "MANUAL"
        case off = "OFF"
    }

    /// Enables a disabled full-text index.
    @available(macOS 12.0, *)
    public func enableIndex(schema: String, table: String) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER FULLTEXT INDEX ON [\(s)].[\(t)] ENABLE;")
    }

    /// Disables a full-text index without dropping it.
    @available(macOS 12.0, *)
    public func disableIndex(schema: String, table: String) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER FULLTEXT INDEX ON [\(s)].[\(t)] DISABLE;")
    }

    /// Drops a full-text index from a table.
    @available(macOS 12.0, *)
    public func dropIndex(schema: String, table: String) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("DROP FULLTEXT INDEX ON [\(s)].[\(t)];")
    }

    /// Starts a full-text index population.
    @available(macOS 12.0, *)
    public func startPopulation(schema: String, table: String, type: PopulationType = .full) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER FULLTEXT INDEX ON [\(s)].[\(t)] START \(type.rawValue) POPULATION;")
    }

    /// Stops a full-text index population.
    @available(macOS 12.0, *)
    public func stopPopulation(schema: String, table: String) async throws {
        let s = schema.replacingOccurrences(of: "]", with: "]]")
        let t = table.replacingOccurrences(of: "]", with: "]]")
        _ = try await client.execute("ALTER FULLTEXT INDEX ON [\(s)].[\(t)] STOP POPULATION;")
    }

    /// Population type for full-text index rebuild.
    public enum PopulationType: String, Sendable {
        case full = "FULL"
        case incremental = "INCREMENTAL"
        case update = "UPDATE"
    }
}
