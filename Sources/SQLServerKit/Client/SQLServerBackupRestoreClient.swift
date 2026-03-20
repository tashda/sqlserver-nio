import Foundation
import NIO

// MARK: - Types

/// The type of SQL Server backup operation.
public enum SQLServerBackupType: String, Sendable, CaseIterable {
    case full = "Full"
    case differential = "Differential"
    case log = "Log"
}

/// Encryption algorithm for SQL Server backup encryption.
public enum SQLServerBackupEncryptionAlgorithm: String, Sendable, CaseIterable {
    case aes128 = "AES_128"
    case aes192 = "AES_192"
    case aes256 = "AES_256"
    case tripleDES = "TRIPLE_DES_3KEY"
}

/// Encryption options for a SQL Server backup.
public struct SQLServerBackupEncryption: Sendable {
    public let algorithm: SQLServerBackupEncryptionAlgorithm
    public let serverCertificate: String?
    public let serverAsymmetricKey: String?

    public init(
        algorithm: SQLServerBackupEncryptionAlgorithm,
        serverCertificate: String? = nil,
        serverAsymmetricKey: String? = nil
    ) {
        self.algorithm = algorithm
        self.serverCertificate = serverCertificate
        self.serverAsymmetricKey = serverAsymmetricKey
    }
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
    public let checksum: Bool
    public let continueAfterError: Bool
    public let initMedia: Bool
    public let formatMedia: Bool
    public let mediaName: String?
    public let verifyAfterBackup: Bool
    public let expireDate: Date?
    public let encryption: SQLServerBackupEncryption?
    public let statsPercentage: Int

    public init(
        database: String,
        diskPath: String,
        backupType: SQLServerBackupType = .full,
        backupName: String? = nil,
        description: String? = nil,
        compression: Bool = false,
        copyOnly: Bool = false,
        checksum: Bool = false,
        continueAfterError: Bool = false,
        initMedia: Bool = false,
        formatMedia: Bool = false,
        mediaName: String? = nil,
        verifyAfterBackup: Bool = false,
        expireDate: Date? = nil,
        encryption: SQLServerBackupEncryption? = nil,
        statsPercentage: Int = 10
    ) {
        self.database = database
        self.diskPath = diskPath
        self.backupType = backupType
        self.backupName = backupName
        self.description = description
        self.compression = compression
        self.copyOnly = copyOnly
        self.checksum = checksum
        self.continueAfterError = continueAfterError
        self.initMedia = initMedia
        self.formatMedia = formatMedia
        self.mediaName = mediaName
        self.verifyAfterBackup = verifyAfterBackup
        self.expireDate = expireDate
        self.encryption = encryption
        self.statsPercentage = statsPercentage
    }
}

/// Recovery mode for a SQL Server RESTORE operation.
public enum SQLServerRestoreRecoveryMode: String, Sendable, CaseIterable {
    case recovery = "RECOVERY"
    case noRecovery = "NORECOVERY"
    case standby = "STANDBY"
}

/// Options for a SQL Server RESTORE operation.
public struct SQLServerRestoreOptions: Sendable {
    public let database: String
    public let diskPath: String
    public let fileNumber: Int
    public let recoveryMode: SQLServerRestoreRecoveryMode
    public let replace: Bool
    public let closeExistingConnections: Bool
    public let keepReplication: Bool
    public let restrictedUser: Bool
    public let checksum: Bool
    public let continueAfterError: Bool
    public let statsPercentage: Int
    public let relocateFiles: [FileRelocation]
    public let stopAt: Date?
    public let standbyFile: String?

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
        recoveryMode: SQLServerRestoreRecoveryMode = .recovery,
        replace: Bool = false,
        closeExistingConnections: Bool = false,
        keepReplication: Bool = false,
        restrictedUser: Bool = false,
        checksum: Bool = false,
        continueAfterError: Bool = false,
        statsPercentage: Int = 5,
        relocateFiles: [FileRelocation] = [],
        stopAt: Date? = nil,
        standbyFile: String? = nil
    ) {
        self.database = database
        self.diskPath = diskPath
        self.fileNumber = fileNumber
        self.recoveryMode = recoveryMode
        self.replace = replace
        self.closeExistingConnections = closeExistingConnections
        self.keepReplication = keepReplication
        self.restrictedUser = restrictedUser
        self.checksum = checksum
        self.continueAfterError = continueAfterError
        self.statsPercentage = statsPercentage
        self.relocateFiles = relocateFiles
        self.stopAt = stopAt
        self.standbyFile = standbyFile
    }

    /// Backward-compatible initializer using `withRecovery` boolean.
    public init(
        database: String,
        diskPath: String,
        fileNumber: Int = 1,
        withRecovery: Bool = true,
        replace: Bool = false,
        checksum: Bool = false,
        continueAfterError: Bool = false,
        statsPercentage: Int = 5,
        relocateFiles: [FileRelocation] = [],
        stopAt: Date? = nil
    ) {
        self.init(
            database: database,
            diskPath: diskPath,
            fileNumber: fileNumber,
            recoveryMode: withRecovery ? .recovery : .noRecovery,
            replace: replace,
            checksum: checksum,
            continueAfterError: continueAfterError,
            statsPercentage: statsPercentage,
            relocateFiles: relocateFiles,
            stopAt: stopAt
        )
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

    /// Verifies a backup file without restoring it (RESTORE VERIFYONLY).
    @available(macOS 12.0, *)
    public func verifyBackup(diskPath: String, fileNumber: Int = 1) async throws -> [SQLServerStreamMessage] {
        let escaped = diskPath.replacingOccurrences(of: "'", with: "''")
        let sql = "RESTORE VERIFYONLY FROM DISK = N'\(escaped)' WITH FILE = \(max(1, fileNumber));"
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

    // MARK: - Connection Management

    /// Sets a database to SINGLE_USER mode, disconnecting all other sessions.
    /// Call this before restoring over an active database.
    @available(macOS 12.0, *)
    public func closeConnections(database: String) async throws {
        let db = SQLServerAdministrationClient.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(db) SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
        _ = try await client.execute(sql)
    }

    /// Restores a database to MULTI_USER mode after a restore operation.
    @available(macOS 12.0, *)
    public func restoreMultiUser(database: String) async throws {
        let db = SQLServerAdministrationClient.escapeIdentifier(database)
        let sql = "ALTER DATABASE \(db) SET MULTI_USER;"
        _ = try await client.execute(sql)
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
        parts.append(options.formatMedia ? "WITH FORMAT" : "WITH NOFORMAT")
        parts.append(options.initMedia ? "INIT" : "NOINIT")

        if let mediaName = options.mediaName, !mediaName.isEmpty {
            let escaped = mediaName.replacingOccurrences(of: "'", with: "''")
            parts.append("MEDIANAME = N'\(escaped)'")
        }

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

        if options.checksum {
            parts.append("CHECKSUM")
        }

        if options.continueAfterError {
            parts.append("CONTINUE_AFTER_ERROR")
        }

        if let expireDate = options.expireDate {
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
            formatter.timeZone = TimeZone(identifier: "UTC")
            parts.append("EXPIREDATE = N'\(formatter.string(from: expireDate))'")
        }

        if let encryption = options.encryption {
            var encParts = "ENCRYPTION(ALGORITHM = \(encryption.algorithm.rawValue)"
            if let cert = encryption.serverCertificate, !cert.isEmpty {
                let escaped = cert.replacingOccurrences(of: "'", with: "''")
                encParts += ", SERVER CERTIFICATE = [\(escaped)]"
            } else if let key = encryption.serverAsymmetricKey, !key.isEmpty {
                let escaped = key.replacingOccurrences(of: "'", with: "''")
                encParts += ", SERVER ASYMMETRIC KEY = [\(escaped)]"
            }
            encParts += ")"
            parts.append(encParts)
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

        switch options.recoveryMode {
        case .recovery:
            break
        case .noRecovery:
            parts.append("NORECOVERY")
        case .standby:
            if let standbyFile = options.standbyFile, !standbyFile.isEmpty {
                let escaped = standbyFile.replacingOccurrences(of: "'", with: "''")
                parts.append("STANDBY = N'\(escaped)'")
            } else {
                parts.append("NORECOVERY")
            }
        }

        if options.replace {
            parts.append("REPLACE")
        }

        if options.keepReplication {
            parts.append("KEEP_REPLICATION")
        }

        if options.restrictedUser {
            parts.append("RESTRICTED_USER")
        }

        if options.checksum {
            parts.append("CHECKSUM")
        }

        if options.continueAfterError {
            parts.append("CONTINUE_AFTER_ERROR")
        }

        for relocation in options.relocateFiles {
            let logical = relocation.logicalName.replacingOccurrences(of: "'", with: "''")
            let physical = relocation.physicalPath.replacingOccurrences(of: "'", with: "''")
            parts.append("MOVE N'\(logical)' TO N'\(physical)'")
        }

        if let stopAt = options.stopAt {
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
            formatter.timeZone = TimeZone(identifier: "UTC")
            let formatted = formatter.string(from: stopAt)
            parts.append("STOPAT = N'\(formatted)'")
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

    /// Retrieves recent backup history for a database from msdb.
    @available(macOS 12.0, *)
    public func getBackupHistory(database: String, limit: Int = 100) async throws -> [SQLServerBackupHistoryEntry] {
        let escaped = database.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT TOP (\(limit))
            bs.backup_set_id,
            bs.name AS backup_name,
            bs.description,
            bs.backup_start_date,
            bs.backup_finish_date,
            bs.type,
            bs.backup_size,
            bs.compressed_backup_size,
            bmf.physical_device_name,
            bs.server_name,
            bs.recovery_model
        FROM msdb.dbo.backupset bs
        JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
        WHERE bs.database_name = N'\(escaped)'
        ORDER BY bs.backup_finish_date DESC;
        """

        let rows = try await client.query(sql).get()
        return rows.map { row in
            SQLServerBackupHistoryEntry(
                id: row.column("backup_set_id")?.int ?? 0,
                name: row.column("backup_name")?.string,
                description: row.column("description")?.string,
                startDate: row.column("backup_start_date")?.date,
                finishDate: row.column("backup_finish_date")?.date,
                type: row.column("type")?.string ?? "D",
                size: row.column("backup_size")?.int64 ?? 0,
                compressedSize: row.column("compressed_backup_size")?.int64,
                physicalPath: row.column("physical_device_name")?.string ?? "",
                serverName: row.column("server_name")?.string ?? "",
                recoveryModel: row.column("recovery_model")?.string ?? ""
            )
        }
    }
}

/// Represents an entry in the SQL Server backup history.
public struct SQLServerBackupHistoryEntry: Sendable, Identifiable {
    public let id: Int
    public let name: String?
    public let description: String?
    public let startDate: Date?
    public let finishDate: Date?
    public let type: String
    public let size: Int64
    public let compressedSize: Int64?
    public let physicalPath: String
    public let serverName: String
    public let recoveryModel: String

    public var typeDescription: String {
        switch type {
        case "D": return "Full"
        case "I": return "Differential"
        case "L": return "Log"
        case "F": return "File or Filegroup"
        case "G": return "Differential File"
        case "P": return "Partial"
        case "Q": return "Differential Partial"
        default: return "Unknown (\(type))"
        }
    }
}
