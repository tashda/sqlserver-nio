import Foundation
import SQLServerTDS

// MARK: - Create Database Options

/// Options for creating a SQL Server database, including collation, containment, and file settings.
public struct SQLServerCreateDatabaseOptions: Sendable {
    public var collation: String?
    public var containment: String?
    public var dataFileName: String?
    public var dataFileSize: Int?
    public var dataFileMaxSize: Int?
    public var dataFileGrowth: Int?
    public var logFileName: String?
    public var logFileSize: Int?
    public var logFileMaxSize: Int?
    public var logFileGrowth: Int?

    public init(
        collation: String? = nil,
        containment: String? = nil,
        dataFileName: String? = nil,
        dataFileSize: Int? = nil,
        dataFileMaxSize: Int? = nil,
        dataFileGrowth: Int? = nil,
        logFileName: String? = nil,
        logFileSize: Int? = nil,
        logFileMaxSize: Int? = nil,
        logFileGrowth: Int? = nil
    ) {
        self.collation = collation
        self.containment = containment
        self.dataFileName = dataFileName
        self.dataFileSize = dataFileSize
        self.dataFileMaxSize = dataFileMaxSize
        self.dataFileGrowth = dataFileGrowth
        self.logFileName = logFileName
        self.logFileSize = logFileSize
        self.logFileMaxSize = logFileMaxSize
        self.logFileGrowth = logFileGrowth
    }
}

extension SQLServerAdministrationClient {
    // MARK: - Database Management

    /// Create a database with optional configuration for collation, containment, and file settings.
    @available(macOS 12.0, *)
    @discardableResult
    public func createDatabase(
        name: String,
        options: SQLServerCreateDatabaseOptions = .init()
    ) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        var sql = "CREATE DATABASE \(escaped)"

        if let collation = options.collation {
            sql += "\n    COLLATE \(collation)"
        }

        if let containment = options.containment {
            sql += "\n    WITH CONTAINMENT = \(containment)"
        }

        // Primary data file
        if options.dataFileName != nil || options.dataFileSize != nil {
            let logicalName = options.dataFileName ?? name
            let escapedLogical = logicalName.replacingOccurrences(of: "'", with: "''")
            var fileParts = ["NAME = N'\(escapedLogical)'"]
            if let size = options.dataFileSize { fileParts.append("SIZE = \(size)MB") }
            if let maxSize = options.dataFileMaxSize {
                fileParts.append("MAXSIZE = \(maxSize)MB")
            }
            if let growth = options.dataFileGrowth { fileParts.append("FILEGROWTH = \(growth)MB") }
            sql += "\n    ON PRIMARY (\(fileParts.joined(separator: ", ")))"
        }

        // Log file
        if options.logFileName != nil || options.logFileSize != nil {
            let logicalName = options.logFileName ?? "\(name)_log"
            let escapedLogical = logicalName.replacingOccurrences(of: "'", with: "''")
            var fileParts = ["NAME = N'\(escapedLogical)'"]
            if let size = options.logFileSize { fileParts.append("SIZE = \(size)MB") }
            if let maxSize = options.logFileMaxSize {
                fileParts.append("MAXSIZE = \(maxSize)MB")
            }
            if let growth = options.logFileGrowth { fileParts.append("FILEGROWTH = \(growth)MB") }
            sql += "\n    LOG ON (\(fileParts.joined(separator: ", ")))"
        }

        let result = try await client.execute(sql)
        return result.messages
    }

    /// Create a database with optional configuration for collation, containment, and file settings.
    @available(macOS 12.0, *)
    @available(*, deprecated, message: "Use createDatabase(name:options:) instead")
    @discardableResult
    public func createDatabase(
        name: String,
        collation: String? = nil,
        containment: String? = nil,
        dataFileName: String? = nil,
        dataFileSize: Int? = nil,
        dataFileMaxSize: Int? = nil,
        dataFileGrowth: Int? = nil,
        logFileName: String? = nil,
        logFileSize: Int? = nil,
        logFileMaxSize: Int? = nil,
        logFileGrowth: Int? = nil
    ) async throws -> [SQLServerStreamMessage] {
        try await createDatabase(
            name: name,
            options: SQLServerCreateDatabaseOptions(
                collation: collation,
                containment: containment,
                dataFileName: dataFileName,
                dataFileSize: dataFileSize,
                dataFileMaxSize: dataFileMaxSize,
                dataFileGrowth: dataFileGrowth,
                logFileName: logFileName,
                logFileSize: logFileSize,
                logFileMaxSize: logFileMaxSize,
                logFileGrowth: logFileGrowth
            )
        )
    }

    /// List available collations from sys.fn_helpcollations().
    @available(macOS 12.0, *)
    public func listCollations() async throws -> [String] {
        let rows = try await client.query("SELECT name FROM sys.fn_helpcollations() ORDER BY name")
        return rows.compactMap { $0.column("name")?.string }
    }

    /// List standard recovery models.
    @available(macOS 12.0, *)
    public func listRecoveryModels() -> [String] {
        ["FULL", "BULK_LOGGED", "SIMPLE"]
    }

    /// Take a database offline with rollback of active transactions.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func takeDatabaseOffline(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let result = try await client.execute("ALTER DATABASE \(escaped) SET OFFLINE WITH ROLLBACK IMMEDIATE")
        return result.messages
    }

    /// Bring an offline database back online.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func bringDatabaseOnline(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let result = try await client.execute("ALTER DATABASE \(escaped) SET ONLINE")
        return result.messages
    }

    /// Shrink a database to reclaim unused space.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func shrinkDatabase(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let result = try await client.execute("DBCC SHRINKDATABASE(\(escaped))")
        return result.messages
    }

    /// Drop a database.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func dropDatabase(name: String, forceSingleUser: Bool = false) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let sql: String
        if forceSingleUser {
            sql = """
            IF DB_ID(N'\(name.replacingOccurrences(of: "'", with: "''"))') IS NOT NULL
            BEGIN
                ALTER DATABASE \(escaped) SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE \(escaped);
            END
            """
        } else {
            sql = "DROP DATABASE \(escaped)"
        }
        let result = try await client.execute(sql)
        return result.messages
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func setSnapshotIsolation(database name: String, enabled: Bool) async throws -> [SQLServerStreamMessage] {
        let state = enabled ? "ON" : "OFF"
        let result = try await client.execute("ALTER DATABASE \(SQLServerSQL.escapeIdentifier(name)) SET ALLOW_SNAPSHOT_ISOLATION \(state)")
        return result.messages
    }

    /// Get comprehensive properties for a database from sys.databases and related system views.
    @available(macOS 12.0, *)
    public func getDatabaseProperties(name: String) async throws -> SQLServerDatabaseProperties {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            d.name,
            COALESCE(SUSER_SNAME(d.owner_sid), '') AS owner,
            d.state_desc AS state_description,
            d.recovery_model_desc AS recovery_model,
            d.compatibility_level,
            COALESCE(d.collation_name, '') AS collation_name,
            d.is_read_only,
            d.user_access_desc AS user_access_description,
            d.page_verify_option_desc AS page_verify_option,
            d.is_auto_close_on,
            d.is_auto_shrink_on,
            d.is_auto_create_stats_on,
            d.is_auto_update_stats_on,
            d.is_auto_update_stats_async_on,
            CONVERT(VARCHAR(23), d.create_date, 121) AS create_date,
            CONVERT(VARCHAR(64), COALESCE((
                SELECT CAST(SUM(CAST(mf.size AS BIGINT)) * 8.0 / 1024 AS FLOAT)
                FROM sys.master_files mf
                WHERE mf.database_id = d.database_id
            ), 0)) AS size_mb,
            COALESCE((
                SELECT COUNT(*)
                FROM sys.dm_exec_sessions s
                WHERE s.database_id = d.database_id
            ), 0) AS active_sessions,
            COALESCE(d.delayed_durability_desc, 'DISABLED') AS delayed_durability,
            d.snapshot_isolation_state_desc AS snapshot_isolation_state,
            d.is_read_committed_snapshot_on,
            d.is_encrypted,
            d.is_broker_enabled,
            d.is_trustworthy_on,
            d.is_parameterization_forced,
            d.is_ansi_null_default_on,
            d.is_ansi_nulls_on,
            d.is_ansi_padding_on,
            d.is_ansi_warnings_on,
            d.is_arithabort_on,
            d.is_concat_null_yields_null_on,
            d.is_quoted_identifier_on,
            d.is_recursive_triggers_on,
            d.is_numeric_roundabort_on,
            d.is_date_correlation_on,
            COALESCE((
                SELECT TOP 1 CONVERT(VARCHAR(23), bs.backup_finish_date, 121)
                FROM msdb.dbo.backupset bs
                WHERE bs.database_name = d.name AND bs.type = 'D'
                ORDER BY bs.backup_finish_date DESC
            ), '') AS last_backup_date,
            COALESCE((
                SELECT TOP 1 CONVERT(VARCHAR(23), bs.backup_finish_date, 121)
                FROM msdb.dbo.backupset bs
                WHERE bs.database_name = d.name AND bs.type = 'L'
                ORDER BY bs.backup_finish_date DESC
            ), '') AS last_log_backup_date
        FROM sys.databases d
        WHERE d.name = N'\(escapedName)'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "Database '\(name)' not found in sys.databases")
        }
        let targetRecoveryTimeQuery = "SELECT CONVERT(VARCHAR(12), ISNULL((SELECT target_recovery_time_in_seconds FROM sys.databases WHERE name = N'\(escapedName)'), 0)) AS target_recovery_time_seconds;"
        @Sendable
        func queryTargetRecoveryTime(using connection: SQLServerConnection) async throws -> Int {
            let rows = try await connection.query(
                targetRecoveryTimeQuery
            ).get()
            return Int(rows.first?.column("target_recovery_time_seconds")?.string ?? "0") ?? 0
        }

        var targetRecoveryTimeSeconds = try await client.withConnection { connection in
            connection.eventLoop.makeFutureWithTask {
                try await queryTargetRecoveryTime(using: connection)
            }
        }.get()
        if targetRecoveryTimeSeconds == 0 {
            let fallbackConnection = try await SQLServerConnection.connect(
                configuration: client.configuration.connection,
                eventLoopGroupProvider: .shared(client.eventLoopGroup),
                logger: client.logger
            ).get()
            defer { _ = fallbackConnection.close() }
            targetRecoveryTimeSeconds = try await queryTargetRecoveryTime(using: fallbackConnection)
        }
        func boolCol(_ name: String) -> Bool { (row.column(name)?.int ?? 0) != 0 }

        return SQLServerDatabaseProperties(
            name: row.column("name")?.string ?? name,
            owner: row.column("owner")?.string ?? "",
            stateDescription: row.column("state_description")?.string ?? "UNKNOWN",
            createDate: row.column("create_date")?.string ?? "",
            sizeMB: Double(row.column("size_mb")?.string ?? "0") ?? 0.0,
            activeSessions: row.column("active_sessions")?.int ?? 0,
            collationName: row.column("collation_name")?.string ?? "",
            recoveryModel: row.column("recovery_model")?.string ?? "UNKNOWN",
            compatibilityLevel: row.column("compatibility_level")?.int ?? 0,
            isReadOnly: boolCol("is_read_only"),
            userAccessDescription: row.column("user_access_description")?.string ?? "MULTI_USER",
            pageVerifyOption: row.column("page_verify_option")?.string ?? "NONE",
            targetRecoveryTimeSeconds: targetRecoveryTimeSeconds,
            delayedDurability: row.column("delayed_durability")?.string ?? "DISABLED",
            snapshotIsolationState: row.column("snapshot_isolation_state")?.string ?? "OFF",
            isReadCommittedSnapshotOn: boolCol("is_read_committed_snapshot_on"),
            isEncrypted: boolCol("is_encrypted"),
            isBrokerEnabled: boolCol("is_broker_enabled"),
            isTrustworthy: boolCol("is_trustworthy_on"),
            isParameterizationForced: boolCol("is_parameterization_forced"),
            isAutoCloseOn: boolCol("is_auto_close_on"),
            isAutoShrinkOn: boolCol("is_auto_shrink_on"),
            isAutoCreateStatsOn: boolCol("is_auto_create_stats_on"),
            isAutoUpdateStatsOn: boolCol("is_auto_update_stats_on"),
            isAutoUpdateStatsAsyncOn: boolCol("is_auto_update_stats_async_on"),
            isAnsiNullDefaultOn: boolCol("is_ansi_null_default_on"),
            isAnsiNullsOn: boolCol("is_ansi_nulls_on"),
            isAnsiPaddingOn: boolCol("is_ansi_padding_on"),
            isAnsiWarningsOn: boolCol("is_ansi_warnings_on"),
            isArithAbortOn: boolCol("is_arithabort_on"),
            isConcatNullYieldsNullOn: boolCol("is_concat_null_yields_null_on"),
            isQuotedIdentifierOn: boolCol("is_quoted_identifier_on"),
            isRecursiveTriggersOn: boolCol("is_recursive_triggers_on"),
            isNumericRoundAbortOn: boolCol("is_numeric_roundabort_on"),
            isDateCorrelationOn: boolCol("is_date_correlation_on"),
            lastBackupDate: { let v = row.column("last_backup_date")?.string ?? ""; return v.isEmpty ? nil : v }(),
            lastLogBackupDate: { let v = row.column("last_log_backup_date")?.string ?? ""; return v.isEmpty ? nil : v }()
        )
    }

    @available(*, deprecated, renamed: "getDatabaseProperties(name:)")
    @available(macOS 12.0, *)
    public func fetchDatabaseProperties(name: String) async throws -> SQLServerDatabaseProperties {
        try await getDatabaseProperties(name: name)
    }

    /// Get database files from sys.master_files.
    @available(macOS 12.0, *)
    public func getDatabaseFiles(name: String) async throws -> [SQLServerDatabaseFile] {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            mf.name,
            mf.type,
            mf.type_desc,
            mf.physical_name,
            mf.size,
            CAST(mf.size AS BIGINT) * 8.0 / 1024 AS size_mb,
            mf.max_size,
            CASE
                WHEN mf.max_size = -1 THEN 'Unlimited'
                WHEN mf.max_size = 0 THEN 'No Growth'
                ELSE CAST(CAST(mf.max_size AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
            END AS max_size_desc,
            mf.growth,
            mf.is_percent_growth,
            CASE
                WHEN mf.is_percent_growth = 1 THEN CAST(mf.growth AS VARCHAR) + '%'
                WHEN mf.growth = 0 THEN 'None'
                ELSE CAST(mf.growth * 8 / 1024 AS VARCHAR) + ' MB'
            END AS growth_desc,
            COALESCE(fg.name, '') AS filegroup_name
        FROM sys.master_files mf
        LEFT JOIN sys.filegroups fg ON fg.data_space_id = mf.data_space_id
            AND mf.database_id = DB_ID()
        WHERE mf.database_id = DB_ID(N'\(escapedName)')
        ORDER BY mf.type, mf.file_id
        """

        let rows = try await client.query(sql)
        return rows.map { row in
            let name = row.column("name")?.string ?? ""
            let typeDesc = row.column("type_desc")?.string ?? ""
            let physicalName = row.column("physical_name")?.string ?? ""
            let sizeMB = row.column("size_mb")?.double ?? 0
            let maxSizeDesc = row.column("max_size_desc")?.string ?? ""
            let growthDesc = row.column("growth_desc")?.string ?? ""
            let fileGroupName = row.column("filegroup_name")?.string
            let sizePages = row.column("size")?.int ?? 0
            let maxSizeRaw = row.column("max_size")?.int ?? -1
            let growthRaw = row.column("growth")?.int ?? 0
            let isPercentGrowth = (row.column("is_percent_growth")?.int ?? 0) != 0
            let type = row.column("type")?.int ?? 0

            return SQLServerDatabaseFile(
                name: name,
                typeDescription: typeDesc,
                physicalName: physicalName,
                sizeMB: sizeMB,
                maxSizeDescription: maxSizeDesc,
                growthDescription: growthDesc,
                fileGroupName: fileGroupName?.isEmpty == false ? fileGroupName : nil,
                sizePages: sizePages,
                maxSizeRaw: maxSizeRaw,
                growthRaw: growthRaw,
                isPercentGrowth: isPercentGrowth,
                type: type
            )
        }
    }

    @available(*, deprecated, renamed: "getDatabaseFiles(name:)")
    @available(macOS 12.0, *)
    public func fetchDatabaseFiles(name: String) async throws -> [SQLServerDatabaseFile] {
        try await getDatabaseFiles(name: name)
    }

    /// Modify a database file property (size, max size, or growth).
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func modifyDatabaseFile(
        databaseName: String,
        logicalFileName: String,
        option: SQLServerDatabaseFileOption
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(databaseName)
        let escapedFile = logicalFileName.replacingOccurrences(of: "'", with: "''")

        let optionClause: String
        switch option {
        case .sizeMB(let mb):
            optionClause = "SIZE = \(mb)MB"
        case .maxSizeMB(let mb):
            optionClause = "MAXSIZE = \(mb)MB"
        case .maxSizeUnlimited:
            optionClause = "MAXSIZE = UNLIMITED"
        case .filegrowthMB(let mb):
            optionClause = "FILEGROWTH = \(mb)MB"
        case .filegrowthPercent(let pct):
            optionClause = "FILEGROWTH = \(pct)%"
        case .filegrowthNone:
            optionClause = "FILEGROWTH = 0"
        }

        let sql = "ALTER DATABASE \(escapedDb) MODIFY FILE (NAME = N'\(escapedFile)', \(optionClause))"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Add a new data file to a database.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func addDatabaseFile(
        databaseName: String,
        logicalName: String,
        fileName: String,
        sizeMB: Int = 8,
        maxSizeMB: Int? = nil,
        filegrowthMB: Int = 64,
        fileGroup: String? = nil
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(databaseName)
        let escapedLogical = logicalName.replacingOccurrences(of: "'", with: "''")
        let escapedPhysical = fileName.replacingOccurrences(of: "'", with: "''")

        var clauses = [
            "NAME = N'\(escapedLogical)'",
            "FILENAME = N'\(escapedPhysical)'",
            "SIZE = \(sizeMB)MB",
            "FILEGROWTH = \(filegrowthMB)MB"
        ]

        if let maxSize = maxSizeMB {
            clauses.append("MAXSIZE = \(maxSize)MB")
        } else {
            clauses.append("MAXSIZE = UNLIMITED")
        }

        let fileSpec = clauses.joined(separator: ", ")
        let toFileGroup: String
        if let fg = fileGroup {
            toFileGroup = " TO FILEGROUP \(SQLServerSQL.escapeIdentifier(fg))"
        } else {
            toFileGroup = ""
        }

        let sql = "ALTER DATABASE \(escapedDb) ADD FILE (\(fileSpec))\(toFileGroup)"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Add a new log file to a database.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func addDatabaseLogFile(
        databaseName: String,
        logicalName: String,
        fileName: String,
        sizeMB: Int = 8,
        maxSizeMB: Int? = nil,
        filegrowthMB: Int = 64
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(databaseName)
        let escapedLogical = logicalName.replacingOccurrences(of: "'", with: "''")
        let escapedPhysical = fileName.replacingOccurrences(of: "'", with: "''")

        var clauses = [
            "NAME = N'\(escapedLogical)'",
            "FILENAME = N'\(escapedPhysical)'",
            "SIZE = \(sizeMB)MB",
            "FILEGROWTH = \(filegrowthMB)MB"
        ]

        if let maxSize = maxSizeMB {
            clauses.append("MAXSIZE = \(maxSize)MB")
        } else {
            clauses.append("MAXSIZE = UNLIMITED")
        }

        let fileSpec = clauses.joined(separator: ", ")
        let sql = "ALTER DATABASE \(escapedDb) ADD LOG FILE (\(fileSpec))"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Remove a file from a database.
    /// The file must be empty before it can be removed. Use `shrinkDatabaseFile` first if needed.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func removeDatabaseFile(
        databaseName: String,
        logicalFileName: String
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = SQLServerSQL.escapeIdentifier(databaseName)
        let escapedFile = logicalFileName.replacingOccurrences(of: "'", with: "''")
        let sql = "ALTER DATABASE \(escapedDb) REMOVE FILE \(SQLServerSQL.escapeIdentifier(escapedFile))"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Shrink a specific database file to reclaim unused space.
    /// targetSizeMB: the target size in MB (pass 0 to shrink as much as possible).
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func shrinkDatabaseFile(
        databaseName: String,
        logicalFileName: String,
        targetSizeMB: Int = 0
    ) async throws -> [SQLServerStreamMessage] {
        // DBCC SHRINKFILE must run in the context of the target database.
        let escapedFile = logicalFileName.replacingOccurrences(of: "'", with: "''")
        let escapedDb = SQLServerSQL.escapeIdentifier(databaseName)
        let sql = """
        USE \(escapedDb);
        DBCC SHRINKFILE(N'\(escapedFile)', \(targetSizeMB));
        """
        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Detach / Attach

    /// Detach a database, releasing its files.
    @available(macOS 12.0, *)
    @discardableResult
    public func detachDatabase(
        name: String,
        skipChecks: Bool = false,
        keepFullTextIndexFiles: Bool = true
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let skipStr = skipChecks ? "true" : "false"
        let keepFTStr = keepFullTextIndexFiles ? "true" : "false"
        let sql = "EXEC sp_detach_db @dbname = N'\(escapedName)', @skipchecks = N'\(skipStr)', @keepfulltextindexfile = N'\(keepFTStr)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Attach a database from existing MDF/NDF/LDF files.
    @available(macOS 12.0, *)
    @discardableResult
    public func attachDatabase(
        name: String,
        files: [String]
    ) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let fileSpecs = files.map { path in
            let escapedPath = path.replacingOccurrences(of: "'", with: "''")
            return "    (FILENAME = N'\(escapedPath)')"
        }.joined(separator: ",\n")
        let sql = """
        CREATE DATABASE \(escaped)
        ON \(fileSpecs)
        FOR ATTACH
        """
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Attach a database and rebuild the transaction log if missing.
    @available(macOS 12.0, *)
    @discardableResult
    public func attachDatabaseRebuildLog(
        name: String,
        files: [String]
    ) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let fileSpecs = files.map { path in
            let escapedPath = path.replacingOccurrences(of: "'", with: "''")
            return "    (FILENAME = N'\(escapedPath)')"
        }.joined(separator: ",\n")
        let sql = """
        CREATE DATABASE \(escaped)
        ON \(fileSpecs)
        FOR ATTACH_REBUILD_LOG
        """
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Get active connections to a database.
    @available(macOS 12.0, *)
    public func getActiveConnections(database: String) async throws -> [SQLServerActiveConnection] {
        let escapedName = database.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            s.session_id,
            COALESCE(s.login_name, '') AS login_name,
            COALESCE(s.host_name, '') AS host_name,
            COALESCE(s.program_name, '') AS program_name,
            COALESCE(s.status, '') AS status
        FROM sys.dm_exec_sessions s
        WHERE s.database_id = DB_ID(N'\(escapedName)')
            AND s.session_id <> @@SPID
        ORDER BY s.session_id
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let sessionId = row.column("session_id")?.int else { return nil }
            return SQLServerActiveConnection(
                sessionId: sessionId,
                loginName: row.column("login_name")?.string ?? "",
                hostName: row.column("host_name")?.string ?? "",
                programName: row.column("program_name")?.string ?? "",
                status: row.column("status")?.string ?? ""
            )
        }
    }

    /// Set database to single-user mode.
    @available(macOS 12.0, *)
    @discardableResult
    public func setDatabaseSingleUser(name: String, rollbackImmediate: Bool = true) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let rollback = rollbackImmediate ? " WITH ROLLBACK IMMEDIATE" : ""
        let result = try await client.execute("ALTER DATABASE \(escaped) SET SINGLE_USER\(rollback)")
        return result.messages
    }

    /// Set database back to multi-user mode.
    @available(macOS 12.0, *)
    @discardableResult
    public func setDatabaseMultiUser(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = SQLServerSQL.escapeIdentifier(name)
        let result = try await client.execute("ALTER DATABASE \(escaped) SET MULTI_USER")
        return result.messages
    }

    /// List all online user databases.
    @available(macOS 12.0, *)
    public func listDatabases() async throws -> [String] {
        let rows = try await client.query("SELECT name FROM sys.databases WHERE state = 0 AND database_id > 0 ORDER BY name")
        return rows.compactMap { $0.column("name")?.string }
    }

}

/// An active connection to a database.
public struct SQLServerActiveConnection: Sendable, Identifiable {
    public var id: Int { sessionId }
    public let sessionId: Int
    public let loginName: String
    public let hostName: String
    public let programName: String
    public let status: String

    public init(sessionId: Int, loginName: String, hostName: String, programName: String, status: String) {
        self.sessionId = sessionId
        self.loginName = loginName
        self.hostName = hostName
        self.programName = programName
        self.status = status
    }
}
