import Foundation

/// A database snapshot entry.
public struct SQLServerDatabaseSnapshot: Sendable, Identifiable, Hashable {
    public var id: String { name }
    public let name: String
    public let sourceDatabaseName: String
    public let sourceDatabaseID: Int
    public let createDate: String
    public let compatibilityLevel: Int
    public let sparseFilePaths: [String]

    public init(name: String, sourceDatabaseName: String, sourceDatabaseID: Int, createDate: String, compatibilityLevel: Int, sparseFilePaths: [String]) {
        self.name = name
        self.sourceDatabaseName = sourceDatabaseName
        self.sourceDatabaseID = sourceDatabaseID
        self.createDate = createDate
        self.compatibilityLevel = compatibilityLevel
        self.sparseFilePaths = sparseFilePaths
    }
}

extension SQLServerAdministrationClient {
    // MARK: - Database Snapshots

    /// List all database snapshots on the server.
    @available(macOS 12.0, *)
    public func listSnapshots() async throws -> [SQLServerDatabaseSnapshot] {
        let sql = """
        SELECT
            d.name AS snapshot_name,
            DB_NAME(d.source_database_id) AS source_database_name,
            d.source_database_id,
            CONVERT(VARCHAR(23), d.create_date, 121) AS create_date,
            d.compatibility_level
        FROM sys.databases d
        WHERE d.source_database_id IS NOT NULL
        ORDER BY d.name
        """
        let rows = try await client.query(sql)

        var snapshots: [SQLServerDatabaseSnapshot] = []
        for row in rows {
            let snapshotName = row.column("snapshot_name")?.string ?? ""
            let sourceName = row.column("source_database_name")?.string ?? ""
            let sourceID = row.column("source_database_id")?.int ?? 0
            let createDate = row.column("create_date")?.string ?? ""
            let compatLevel = row.column("compatibility_level")?.int ?? 0

            // Fetch sparse file paths for this snapshot
            let escapedSnap = snapshotName.replacingOccurrences(of: "'", with: "''")
            let filesSQL = """
            SELECT mf.physical_name
            FROM sys.master_files mf
            WHERE mf.database_id = DB_ID(N'\(escapedSnap)')
            ORDER BY mf.file_id
            """
            let fileRows = try await client.query(filesSQL)
            let paths = fileRows.compactMap { $0.column("physical_name")?.string }

            snapshots.append(SQLServerDatabaseSnapshot(
                name: snapshotName,
                sourceDatabaseName: sourceName,
                sourceDatabaseID: sourceID,
                createDate: createDate,
                compatibilityLevel: compatLevel,
                sparseFilePaths: paths
            ))
        }
        return snapshots
    }

    /// Get snapshots for a specific source database.
    @available(macOS 12.0, *)
    public func getSnapshotsForDatabase(sourceDatabase: String) async throws -> [SQLServerDatabaseSnapshot] {
        let all = try await listSnapshots()
        return all.filter { $0.sourceDatabaseName.caseInsensitiveCompare(sourceDatabase) == .orderedSame }
    }

    /// Create a database snapshot.
    /// Automatically queries the source database files and generates appropriate sparse file paths.
    @available(macOS 12.0, *)
    @discardableResult
    public func createSnapshot(name: String, sourceDatabase: String) async throws -> [SQLServerStreamMessage] {
        // First, get source database data files (exclude log files — snapshots don't include log)
        let escapedSource = sourceDatabase.replacingOccurrences(of: "'", with: "''")
        let filesSQL = """
        SELECT mf.name AS logical_name, mf.physical_name
        FROM sys.master_files mf
        WHERE mf.database_id = DB_ID(N'\(escapedSource)')
            AND mf.type = 0
        ORDER BY mf.file_id
        """
        let fileRows = try await client.query(filesSQL)

        guard !fileRows.isEmpty else {
            throw SQLServerError.sqlExecutionError(message: "No data files found for database '\(sourceDatabase)'")
        }

        let escapedSnap = Self.escapeIdentifier(name)
        let escapedSourceId = Self.escapeIdentifier(sourceDatabase)

        let fileSpecs = fileRows.map { row in
            let logicalName = row.column("logical_name")?.string ?? ""
            let physicalName = row.column("physical_name")?.string ?? ""
            let escapedLogical = logicalName.replacingOccurrences(of: "'", with: "''")

            // Generate sparse file path: same directory, snapshot name + .ss extension
            let directory: String
            if let lastSlash = physicalName.lastIndex(of: "\\") {
                directory = String(physicalName[...lastSlash])
            } else if let lastSlash = physicalName.lastIndex(of: "/") {
                directory = String(physicalName[...lastSlash])
            } else {
                directory = ""
            }
            let snapshotFileName = "\(name)_\(logicalName).ss"
            let snapshotPath = (directory + snapshotFileName).replacingOccurrences(of: "'", with: "''")

            return "    (NAME = N'\(escapedLogical)', FILENAME = N'\(snapshotPath)')"
        }.joined(separator: ",\n")

        let sql = """
        CREATE DATABASE \(escapedSnap)
        ON
        \(fileSpecs)
        AS SNAPSHOT OF \(escapedSourceId)
        """

        let result = try await client.execute(sql)
        return result.messages
    }

    /// Revert a source database to a snapshot's state.
    /// Note: If multiple snapshots exist for the source database, all others must be dropped first.
    @available(macOS 12.0, *)
    @discardableResult
    public func revertToSnapshot(snapshotName: String) async throws -> [SQLServerStreamMessage] {
        // First, find the source database name
        let escapedSnap = snapshotName.replacingOccurrences(of: "'", with: "''")
        let findSQL = """
        SELECT DB_NAME(source_database_id) AS source_name
        FROM sys.databases
        WHERE name = N'\(escapedSnap)' AND source_database_id IS NOT NULL
        """
        let rows = try await client.query(findSQL)
        guard let sourceName = rows.first?.column("source_name")?.string else {
            throw SQLServerError.sqlExecutionError(message: "Snapshot '\(snapshotName)' not found or is not a database snapshot")
        }

        let escapedSourceId = Self.escapeIdentifier(sourceName)
        let sql = "RESTORE DATABASE \(escapedSourceId) FROM DATABASE_SNAPSHOT = N'\(escapedSnap)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Drop (delete) a database snapshot.
    @available(macOS 12.0, *)
    @discardableResult
    public func dropSnapshot(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let result = try await client.execute("DROP DATABASE \(escaped)")
        return result.messages
    }
}
