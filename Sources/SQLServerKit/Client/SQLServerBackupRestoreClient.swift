import Foundation
import NIO

// MARK: - Types

/// The type of SQL Server backup operation.
public enum SQLServerBackupType: String, Sendable, CaseIterable {
    case full = "Full"
    case differential = "Differential"
    case log = "Log"
}

/// Options for a SQL Server BACKUP operation.
public struct SQLServerBackupOptions: Sendable {
    public let database: String
    public let diskPath: String
    public let backupType: SQLServerBackupType
    public let backupName: String?
    public let description: String?
    public let compression: Bool
    public let copyOnly: Bool
    public let statsPercentage: Int

    public init(
        database: String,
        diskPath: String,
        backupType: SQLServerBackupType = .full,
        backupName: String? = nil,
        description: String? = nil,
        compression: Bool = false,
        copyOnly: Bool = false,
        statsPercentage: Int = 10
    ) {
        self.database = database
        self.diskPath = diskPath
        self.backupType = backupType
        self.backupName = backupName
        self.description = description
        self.compression = compression
        self.copyOnly = copyOnly
        self.statsPercentage = statsPercentage
    }
}

/// Options for a SQL Server RESTORE operation.
public struct SQLServerRestoreOptions: Sendable {
    public let database: String
    public let diskPath: String
    public let fileNumber: Int
    public let withRecovery: Bool
    public let statsPercentage: Int
    public let relocateFiles: [FileRelocation]

    public struct FileRelocation: Sendable {
        public let logicalName: String
        public let physicalPath: String

        public init(logicalName: String, physicalPath: String) {
            self.logicalName = logicalName
            self.physicalPath = physicalPath
        }
    }

    public init(
        database: String,
        diskPath: String,
        fileNumber: Int = 1,
        withRecovery: Bool = true,
        statsPercentage: Int = 5,
        relocateFiles: [FileRelocation] = []
    ) {
        self.database = database
        self.diskPath = diskPath
        self.fileNumber = fileNumber
        self.withRecovery = withRecovery
        self.statsPercentage = statsPercentage
        self.relocateFiles = relocateFiles
    }
}

/// A backup set header returned from RESTORE HEADERONLY.
public struct SQLServerBackupSetInfo: Sendable, Identifiable {
    public let id: Int
    public let backupName: String?
    public let backupDescription: String?
    public let backupType: Int
    public let backupSize: Int64?
    public let databaseName: String
    public let serverName: String?
    public let backupStartDate: String?
    public let backupFinishDate: String?
    public let firstLSN: String?
    public let lastLSN: String?
    public let compressed: Bool

    /// Human-readable backup type string.
    public var backupTypeDescription: String {
        switch backupType {
        case 1: return "Full"
        case 2: return "Transaction Log"
        case 5: return "Differential"
        default: return "Unknown (\(backupType))"
        }
    }

    public var formattedSize: String {
        guard let size = backupSize else { return "N/A" }
        let mb = Double(size) / (1024 * 1024)
        if mb >= 1024 {
            return String(format: "%.1f GB", mb / 1024)
        }
        return String(format: "%.1f MB", mb)
    }
}

/// A file entry returned from RESTORE FILELISTONLY.
public struct SQLServerBackupFileInfo: Sendable, Identifiable {
    public var id: String { logicalName }
    public let logicalName: String
    public let physicalName: String
    public let type: String
    public let fileGroupName: String?
    public let size: Int64?
    public let maxSize: Int64?

    /// Human-readable file type.
    public var typeDescription: String {
        switch type {
        case "D": return "Data"
        case "L": return "Log"
        case "F": return "Full-text"
        case "S": return "FileStream"
        default: return type
        }
    }
}

// MARK: - Client

/// Namespaced client for SQL Server backup and restore operations.
public final class SQLServerBackupRestoreClient: @unchecked Sendable {
    internal let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Backup

    /// Executes a BACKUP DATABASE or BACKUP LOG command.
    @available(macOS 12.0, *)
    public func backup(options: SQLServerBackupOptions) async throws -> [SQLServerStreamMessage] {
        let sql = buildBackupSQL(options)
        return try await executeLongRunning(sql)
    }

    // MARK: - Restore

    /// Executes a RESTORE DATABASE command.
    @available(macOS 12.0, *)
    public func restore(options: SQLServerRestoreOptions) async throws -> [SQLServerStreamMessage] {
        let sql = buildRestoreSQL(options)
        return try await executeLongRunning(sql)
    }

    /// Lists backup sets from a backup file (RESTORE HEADERONLY).
    @available(macOS 12.0, *)
    public func listBackupSets(diskPath: String) async throws -> [SQLServerBackupSetInfo] {
        let escaped = diskPath.replacingOccurrences(of: "'", with: "''")
        let sql = "RESTORE HEADERONLY FROM DISK = N'\(escaped)';"

        let rows = try await client.query(sql).get()
        return rows.enumerated().compactMap { index, row -> SQLServerBackupSetInfo? in
            guard let dbName = row.column("DatabaseName")?.string else { return nil }
            return SQLServerBackupSetInfo(
                id: index,
                backupName: row.column("BackupName")?.string,
                backupDescription: row.column("BackupDescription")?.string,
                backupType: row.column("BackupType")?.int ?? 0,
                backupSize: row.column("BackupSize")?.int64,
                databaseName: dbName,
                serverName: row.column("ServerName")?.string,
                backupStartDate: row.column("BackupStartDate")?.string,
                backupFinishDate: row.column("BackupFinishDate")?.string,
                firstLSN: row.column("FirstLSN")?.string,
                lastLSN: row.column("LastLSN")?.string,
                compressed: (row.column("CompressedBackupSize")?.int64 ?? 0) > 0
            )
        }
    }

    /// Lists files contained in a backup (RESTORE FILELISTONLY).
    @available(macOS 12.0, *)
    public func listBackupFiles(diskPath: String) async throws -> [SQLServerBackupFileInfo] {
        let escaped = diskPath.replacingOccurrences(of: "'", with: "''")
        let sql = "RESTORE FILELISTONLY FROM DISK = N'\(escaped)';"

        let rows = try await client.query(sql).get()
        return rows.compactMap { row -> SQLServerBackupFileInfo? in
            guard let logicalName = row.column("LogicalName")?.string,
                  let physicalName = row.column("PhysicalName")?.string,
                  let type = row.column("Type")?.string
            else { return nil }
            return SQLServerBackupFileInfo(
                logicalName: logicalName,
                physicalName: physicalName,
                type: type,
                fileGroupName: row.column("FileGroupName")?.string,
                size: row.column("Size")?.int64,
                maxSize: row.column("MaxSize")?.int64
            )
        }
    }

    // MARK: - SQL Building

    private func buildBackupSQL(_ options: SQLServerBackupOptions) -> String {
        let db = SQLServerAdministrationClient.escapeIdentifier(options.database)
        let path = options.diskPath.replacingOccurrences(of: "'", with: "''")

        var parts: [String] = []

        switch options.backupType {
        case .full, .differential:
            parts.append("BACKUP DATABASE \(db)")
        case .log:
            parts.append("BACKUP LOG \(db)")
        }

        parts.append("TO DISK = N'\(path)'")
        parts.append("WITH NOFORMAT, NOINIT")

        if let name = options.backupName {
            let escaped = name.replacingOccurrences(of: "'", with: "''")
            parts.append("NAME = N'\(escaped)'")
        }

        if let desc = options.description {
            let escaped = desc.replacingOccurrences(of: "'", with: "''")
            parts.append("DESCRIPTION = N'\(escaped)'")
        }

        if options.backupType == .differential {
            parts.append("DIFFERENTIAL")
        }

        if options.compression {
            parts.append("COMPRESSION")
        }

        if options.copyOnly {
            parts.append("COPY_ONLY")
        }

        parts.append("SKIP, NOREWIND, NOUNLOAD")
        parts.append("STATS = \(max(1, min(100, options.statsPercentage)))")

        return parts.joined(separator: ", ") + ";"
    }

    private func buildRestoreSQL(_ options: SQLServerRestoreOptions) -> String {
        let db = SQLServerAdministrationClient.escapeIdentifier(options.database)
        let path = options.diskPath.replacingOccurrences(of: "'", with: "''")

        var parts: [String] = []
        parts.append("RESTORE DATABASE \(db)")
        parts.append("FROM DISK = N'\(path)'")
        parts.append("WITH FILE = \(max(1, options.fileNumber))")

        if !options.withRecovery {
            parts.append("NORECOVERY")
        }

        for relocation in options.relocateFiles {
            let logical = relocation.logicalName.replacingOccurrences(of: "'", with: "''")
            let physical = relocation.physicalPath.replacingOccurrences(of: "'", with: "''")
            parts.append("MOVE N'\(logical)' TO N'\(physical)'")
        }

        parts.append("NOUNLOAD")
        parts.append("STATS = \(max(1, min(100, options.statsPercentage)))")

        return parts.joined(separator: ", ") + ";"
    }

    // MARK: - Execution

    @available(macOS 12.0, *)
    private func executeLongRunning(_ sql: String) async throws -> [SQLServerStreamMessage] {
        let result = try await client.execute(sql)
        return result.messages
    }
}
