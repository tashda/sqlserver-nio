import Foundation
import SQLServerTDS

extension SQLServerAdministrationClient {
    // MARK: - Database Management

    /// Create a database.
    @available(macOS 12.0, *)
    @discardableResult
    public func createDatabase(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let result = try await client.execute("CREATE DATABASE \(escaped)")
        return result.messages
    }

    /// Take a database offline with rollback of active transactions.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func takeDatabaseOffline(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let result = try await client.execute("ALTER DATABASE \(escaped) SET OFFLINE WITH ROLLBACK IMMEDIATE")
        return result.messages
    }

    /// Bring an offline database back online.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func bringDatabaseOnline(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let result = try await client.execute("ALTER DATABASE \(escaped) SET ONLINE")
        return result.messages
    }

    /// Shrink a database to reclaim unused space.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func shrinkDatabase(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let result = try await client.execute("DBCC SHRINKDATABASE(\(escaped))")
        return result.messages
    }

    /// Drop a database.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func dropDatabase(name: String, forceSingleUser: Bool = false) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
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
        let result = try await client.execute("ALTER DATABASE \(Self.escapeIdentifier(name)) SET ALLOW_SNAPSHOT_ISOLATION \(state)")
        return result.messages
    }

    /// Fetch comprehensive properties for a database from sys.databases and related system views.
    @available(macOS 12.0, *)
    public func fetchDatabaseProperties(name: String) async throws -> SQLServerDatabaseProperties {
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

    /// Fetch database files from sys.master_files.
    @available(macOS 12.0, *)
    public func fetchDatabaseFiles(name: String) async throws -> [SQLServerDatabaseFile] {
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

    /// Modify a database file property (size, max size, or growth).
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func modifyDatabaseFile(
        databaseName: String,
        logicalFileName: String,
        option: SQLServerDatabaseFileOption
    ) async throws -> [SQLServerStreamMessage] {
        let escapedDb = Self.escapeIdentifier(databaseName)
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
        let escapedDb = Self.escapeIdentifier(databaseName)
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
            toFileGroup = " TO FILEGROUP \(Self.escapeIdentifier(fg))"
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
        let escapedDb = Self.escapeIdentifier(databaseName)
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
        let escapedDb = Self.escapeIdentifier(databaseName)
        let escapedFile = logicalFileName.replacingOccurrences(of: "'", with: "''")
        let sql = "ALTER DATABASE \(escapedDb) REMOVE FILE \(Self.escapeIdentifier(escapedFile))"
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
        let escapedDb = Self.escapeIdentifier(databaseName)
        let sql = """
        USE \(escapedDb);
        DBCC SHRINKFILE(N'\(escapedFile)', \(targetSizeMB));
        """
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Alter a database option using ALTER DATABASE SET.
    /// Returns informational messages from SQL Server.
    @available(macOS 12.0, *)
    @discardableResult
    public func alterDatabaseOption(name: String, option: SQLServerDatabaseOption) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let onOff: (Bool) -> String = { $0 ? "ON" : "OFF" }
        let setClause: String

        switch option {
        case .recoveryModel(let model):
            setClause = "SET RECOVERY \(model.rawValue)"
        case .compatibilityLevel(let level):
            setClause = "SET COMPATIBILITY_LEVEL = \(level)"
        case .readOnly(let readOnly):
            setClause = readOnly ? "SET READ_ONLY" : "SET READ_WRITE"
        case .autoClose(let on):
            setClause = "SET AUTO_CLOSE \(onOff(on))"
        case .autoShrink(let on):
            setClause = "SET AUTO_SHRINK \(onOff(on))"
        case .autoCreateStatistics(let on):
            setClause = "SET AUTO_CREATE_STATISTICS \(onOff(on))"
        case .autoUpdateStatistics(let on):
            setClause = "SET AUTO_UPDATE_STATISTICS \(onOff(on))"
        case .autoUpdateStatisticsAsync(let on):
            setClause = "SET AUTO_UPDATE_STATISTICS_ASYNC \(onOff(on))"
        case .pageVerify(let option):
            setClause = "SET PAGE_VERIFY \(option.rawValue)"
        case .userAccess(let access):
            setClause = "SET \(access.rawValue)"
        case .targetRecoveryTime(let seconds):
            setClause = "SET TARGET_RECOVERY_TIME = \(seconds) SECONDS"
        case .delayedDurability(let option):
            setClause = "SET DELAYED_DURABILITY = \(option.rawValue)"
        case .allowSnapshotIsolation(let on):
            setClause = "SET ALLOW_SNAPSHOT_ISOLATION \(onOff(on))"
        case .readCommittedSnapshot(let on):
            setClause = "SET READ_COMMITTED_SNAPSHOT \(onOff(on))"
        case .encryption(let on):
            setClause = "SET ENCRYPTION \(onOff(on))"
        case .brokerEnabled(let on):
            setClause = on ? "SET ENABLE_BROKER" : "SET DISABLE_BROKER"
        case .trustworthy(let on):
            setClause = "SET TRUSTWORTHY \(onOff(on))"
        case .parameterization(let option):
            setClause = "SET PARAMETERIZATION \(option.rawValue)"
        case .ansiNullDefault(let on):
            setClause = "SET ANSI_NULL_DEFAULT \(onOff(on))"
        case .ansiNulls(let on):
            setClause = "SET ANSI_NULLS \(onOff(on))"
        case .ansiPadding(let on):
            setClause = "SET ANSI_PADDING \(onOff(on))"
        case .ansiWarnings(let on):
            setClause = "SET ANSI_WARNINGS \(onOff(on))"
        case .arithAbort(let on):
            setClause = "SET ARITHABORT \(onOff(on))"
        case .concatNullYieldsNull(let on):
            setClause = "SET CONCAT_NULL_YIELDS_NULL \(onOff(on))"
        case .quotedIdentifier(let on):
            setClause = "SET QUOTED_IDENTIFIER \(onOff(on))"
        case .recursiveTriggers(let on):
            setClause = "SET RECURSIVE_TRIGGERS \(onOff(on))"
        case .numericRoundAbort(let on):
            setClause = "SET NUMERIC_ROUNDABORT \(onOff(on))"
        case .dateCorrelationOptimization(let on):
            setClause = "SET DATE_CORRELATION_OPTIMIZATION \(onOff(on))"
        }

        let result = try await client.execute("ALTER DATABASE \(escaped) \(setClause)")
        return result.messages
    }
}
