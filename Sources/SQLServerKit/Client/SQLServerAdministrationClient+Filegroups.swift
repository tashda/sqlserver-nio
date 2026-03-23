import Foundation
import SQLServerTDS

// MARK: - Filegroup Management

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {

    /// Fetches all filegroups for a database from sys.filegroups.
    /// Must be executed in the context of the target database.
    public func listFilegroups(database: String) async throws -> [SQLServerFilegroup] {
        let escapedDb = Self.escapeIdentifier(database)
        let sql = """
        USE \(escapedDb);
        SELECT
            fg.data_space_id,
            fg.name,
            fg.type_desc,
            fg.is_default,
            fg.is_read_only,
            fg.is_system,
            (SELECT COUNT(*) FROM sys.database_files df WHERE df.data_space_id = fg.data_space_id) AS file_count
        FROM sys.filegroups fg
        ORDER BY fg.data_space_id
        """

        let rows = try await client.query(sql)
        return rows.compactMap { row -> SQLServerFilegroup? in
            guard let name = row.column("name")?.string else { return nil }
            return SQLServerFilegroup(
                dataSpaceID: row.column("data_space_id")?.int ?? 0,
                name: name,
                typeDescription: row.column("type_desc")?.string ?? "ROWS_FILEGROUP",
                isDefault: (row.column("is_default")?.int ?? 0) != 0,
                isReadOnly: (row.column("is_read_only")?.int ?? 0) != 0,
                isSystem: (row.column("is_system")?.int ?? 0) != 0,
                fileCount: row.column("file_count")?.int ?? 0
            )
        }
    }

    /// Creates a new filegroup in the specified database.
    @discardableResult
    public func createFilegroup(
        database: String,
        name: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let escapedFG = Self.escapeIdentifier(name)
        let sql = "ALTER DATABASE \(escapedDb) ADD FILEGROUP \(escapedFG)"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Creates a new memory-optimized filegroup in the specified database.
    @discardableResult
    public func createMemoryOptimizedFilegroup(
        database: String,
        name: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let escapedFG = Self.escapeIdentifier(name)
        let sql = "ALTER DATABASE \(escapedDb) ADD FILEGROUP \(escapedFG) CONTAINS MEMORY_OPTIMIZED_DATA"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets a filegroup as read-only or read-write.
    @discardableResult
    public func alterFilegroupReadOnly(
        database: String,
        filegroup: String,
        readOnly: Bool
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let escapedFG = Self.escapeIdentifier(filegroup)
        let mode = readOnly ? "READONLY" : "READWRITE"
        let sql = "ALTER DATABASE \(escapedDb) MODIFY FILEGROUP \(escapedFG) \(mode)"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets a filegroup as the default filegroup for new tables.
    @discardableResult
    public func setDefaultFilegroup(
        database: String,
        filegroup: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let escapedFG = Self.escapeIdentifier(filegroup)
        let sql = "ALTER DATABASE \(escapedDb) MODIFY FILEGROUP \(escapedFG) DEFAULT"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Removes a filegroup from the database. The filegroup must contain no files.
    @discardableResult
    public func dropFilegroup(
        database: String,
        filegroup: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let escapedFG = Self.escapeIdentifier(filegroup)
        let sql = "ALTER DATABASE \(escapedDb) REMOVE FILEGROUP \(escapedFG)"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Renames a filegroup.
    @discardableResult
    public func renameFilegroup(
        database: String,
        oldName: String,
        newName: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(database)
        let escapedOld = Self.escapeIdentifier(oldName)
        let escapedNew = Self.escapeIdentifier(newName)
        let sql = "ALTER DATABASE \(escapedDb) MODIFY FILEGROUP \(escapedOld) NAME = \(escapedNew)"
        let result = try await client.execute(sql)
        return result.messages
    }
}
