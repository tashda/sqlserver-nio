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
}
