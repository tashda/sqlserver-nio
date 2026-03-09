import NIO
import SQLServerTDS

public struct SQLServerLoginInfo: Sendable {
    public let name: String
    public let type: String
    public let isDisabled: Bool
}

public struct SQLServerRoleInfo: Sendable {
    public let name: String
    public let isFixedRole: Bool
}

// MARK: - Database Properties

/// Comprehensive database properties fetched from sys.databases and related system views.
public struct SQLServerDatabaseProperties: Sendable {
    // General
    public let name: String
    public let owner: String
    public let stateDescription: String
    public let createDate: String
    public let sizeMB: Double
    public let activeSessions: Int
    public let collationName: String

    // Options
    public let recoveryModel: String
    public let compatibilityLevel: Int
    public let isReadOnly: Bool
    public let userAccessDescription: String
    public let pageVerifyOption: String
    public let targetRecoveryTimeSeconds: Int
    public let delayedDurability: String
    public let snapshotIsolationState: String
    public let isReadCommittedSnapshotOn: Bool
    public let isEncrypted: Bool
    public let isBrokerEnabled: Bool
    public let isTrustworthy: Bool
    public let isParameterizationForced: Bool

    // Automatic
    public let isAutoCloseOn: Bool
    public let isAutoShrinkOn: Bool
    public let isAutoCreateStatsOn: Bool
    public let isAutoUpdateStatsOn: Bool
    public let isAutoUpdateStatsAsyncOn: Bool

    // ANSI / Miscellaneous
    public let isAnsiNullDefaultOn: Bool
    public let isAnsiNullsOn: Bool
    public let isAnsiPaddingOn: Bool
    public let isAnsiWarningsOn: Bool
    public let isArithAbortOn: Bool
    public let isConcatNullYieldsNullOn: Bool
    public let isQuotedIdentifierOn: Bool
    public let isRecursiveTriggersOn: Bool
    public let isNumericRoundAbortOn: Bool
    public let isDateCorrelationOn: Bool

    // Backup
    public let lastBackupDate: String?
    public let lastLogBackupDate: String?
}

/// Database file information from sys.master_files.
public struct SQLServerDatabaseFile: Sendable {
    public let name: String
    public let typeDescription: String
    public let physicalName: String
    public let sizeMB: Double
    public let maxSizeDescription: String
    public let growthDescription: String
    public let fileGroupName: String?
}

/// Options that can be set on a database via ALTER DATABASE SET.
public enum SQLServerDatabaseOption: Sendable {
    case recoveryModel(RecoveryModel)
    case compatibilityLevel(Int)
    case readOnly(Bool)
    case autoClose(Bool)
    case autoShrink(Bool)
    case autoCreateStatistics(Bool)
    case autoUpdateStatistics(Bool)
    case autoUpdateStatisticsAsync(Bool)
    case pageVerify(PageVerifyOption)
    case userAccess(UserAccessOption)
    case targetRecoveryTime(Int)
    case delayedDurability(DelayedDurabilityOption)
    case allowSnapshotIsolation(Bool)
    case readCommittedSnapshot(Bool)
    case encryption(Bool)
    case brokerEnabled(Bool)
    case trustworthy(Bool)
    case parameterization(ParameterizationOption)
    case ansiNullDefault(Bool)
    case ansiNulls(Bool)
    case ansiPadding(Bool)
    case ansiWarnings(Bool)
    case arithAbort(Bool)
    case concatNullYieldsNull(Bool)
    case quotedIdentifier(Bool)
    case recursiveTriggers(Bool)
    case numericRoundAbort(Bool)
    case dateCorrelationOptimization(Bool)

    public enum RecoveryModel: String, Sendable, CaseIterable {
        case simple = "SIMPLE"
        case bulkLogged = "BULK_LOGGED"
        case full = "FULL"
    }

    public enum PageVerifyOption: String, Sendable, CaseIterable {
        case checksum = "CHECKSUM"
        case tornPageDetection = "TORN_PAGE_DETECTION"
        case none = "NONE"
    }

    public enum UserAccessOption: String, Sendable, CaseIterable {
        case multiUser = "MULTI_USER"
        case singleUser = "SINGLE_USER"
        case restrictedUser = "RESTRICTED_USER"
    }

    public enum DelayedDurabilityOption: String, Sendable, CaseIterable {
        case disabled = "DISABLED"
        case allowed = "ALLOWED"
        case forced = "FORCED"
    }

    public enum ParameterizationOption: String, Sendable, CaseIterable {
        case simple = "SIMPLE"
        case forced = "FORCED"
    }
}

public final class SQLServerAdministrationClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    public func listServerLogins(includeDisabled: Bool = true) -> EventLoopFuture<[SQLServerLoginInfo]> {
        let filter = includeDisabled ? "" : "WHERE sp.is_disabled = 0"
        let sql = """
        SELECT sp.name, sp.type_desc, sp.is_disabled
        FROM sys.server_principals AS sp
        WHERE sp.type IN ('S','U','G','E','X')
        \(filter.isEmpty ? "" : filter)
        ORDER BY sp.name;
        """

        return client.query(sql).map {
            rows in
            rows.compactMap {
                row in
                guard
                    let name = row.column("name")?.string,
                    let type = row.column("type_desc")?.string,
                    let disabled = row.column("is_disabled")?.int
                else {
                    return nil
                }
                return SQLServerLoginInfo(name: name, type: type, isDisabled: disabled != 0)
            }
        }
    }

    public func listServerRoles() -> EventLoopFuture<[SQLServerRoleInfo]> {
        let sql = """
        SELECT name, is_fixed_role = CAST(ISNULL(is_fixed_role, 0) AS INT)
        FROM sys.server_principals
        WHERE type = 'R'
        ORDER BY name;
        """

        return client.query(sql).map {
            rows in
            rows.compactMap {
                row in
                guard let name = row.column("name")?.string else { return nil }
                let isFixed = row.column("is_fixed_role")?.int ?? 0
                return SQLServerRoleInfo(name: name, isFixedRole: isFixed != 0)
            }
        }
    }

    @available(macOS 12.0, *)
    public func listServerLogins(includeDisabled: Bool = true) async throws -> [SQLServerLoginInfo] {
        try await listServerLogins(includeDisabled: includeDisabled).get()
    }

    @available(macOS 12.0, *)
    public func listServerRoles() async throws -> [SQLServerRoleInfo] {
        try await listServerRoles().get()
    }

    public func createTable(name: String, columns: [SQLServerColumnDefinition]) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createTable(name: name, columns: columns)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    public func createTable(name: String, columns: [SQLServerColumnDefinition]) async throws {
        var primaryKeyColumns = [String]()
        var columnStrings = [String]()

        for col in columns {
            columnStrings.append(try sqlString(for: col))
            if case .standard(let standardColumn) = col.definition, standardColumn.isPrimaryKey {
                primaryKeyColumns.append(col.name)
            }
        }

        let primaryKeyConstraint: String
        if !primaryKeyColumns.isEmpty {
            let keylist = primaryKeyColumns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
            primaryKeyConstraint = ", CONSTRAINT [PK_\(name)] PRIMARY KEY (\(keylist))"
        } else {
            primaryKeyConstraint = ""
        }

        let columnsAndConstraints = columnStrings.joined(separator: ", ") + primaryKeyConstraint
        let createTableSql = "CREATE TABLE [dbo].\(Self.escapeIdentifier(name)) (\n  \(columnsAndConstraints)\n)"

        // Collect column comments
        var commentStatements: [String] = []
        for col in columns {
            if case .standard(let standardColumn) = col.definition, let comment = standardColumn.comment {
                let commentSql = try commentSqlString(forColumn: col.name, inTable: name, comment: comment)
                commentStatements.append(commentSql)
            }
        }
        
        // Execute using separate SQL batches with proper transaction control
        try await executeWithTransactionControl(createTableSql: createTableSql, commentStatements: commentStatements)
    }
    /// Executes table creation with comments using separate SQL batches with proper transaction control
    @available(macOS 12.0, *)
    private func executeWithTransactionControl(createTableSql: String, commentStatements: [String]) async throws {
        try await client.withConnection { connection in
            try await connection.withTransaction { conn in
                _ = try await conn.execute(createTableSql)
                for commentSql in commentStatements {
                    _ = try await conn.execute(commentSql)
                }
                return ()
            }
        }
    }
    
    /// Adds a comment to a table
    @available(macOS 12.0, *)
    public func addTableComment(tableName: String, comment: String) async throws {
        let sql = """
        EXEC sp_addextendedproperty
            @name = N'MS_Description',
            @value = N'\(comment.replacingOccurrences(of: "'", with: "''"))',
            @level0type = N'SCHEMA', @level0name = N'dbo',
            @level1type = N'TABLE', @level1name = N'\(tableName.replacingOccurrences(of: "'", with: "''"))';
        """
        _ = try await client.execute(sql)
    }
    
    /// Adds a comment to a column
    @available(macOS 12.0, *)
    public func addColumnComment(tableName: String, columnName: String, comment: String) async throws {
        let sql = try commentSqlString(forColumn: columnName, inTable: tableName, comment: comment)
        _ = try await client.execute(sql)
    }
    
    /// Updates an existing column comment
    @available(macOS 12.0, *)
    public func updateColumnComment(tableName: String, columnName: String, comment: String) async throws {
        let sql = try updateCommentSqlString(forColumn: columnName, inTable: tableName, comment: comment)
        _ = try await client.execute(sql)
    }
    
    /// Updates an existing table comment
    @available(macOS 12.0, *)
    public func updateTableComment(tableName: String, comment: String) async throws {
        let sql = try updateCommentSqlString(forTable: tableName, comment: comment)
        _ = try await client.execute(sql)
    }
    
    /// Removes a column comment
    @available(macOS 12.0, *)
    public func removeColumnComment(tableName: String, columnName: String) async throws {
        let sql = try removeCommentSqlString(forColumn: columnName, inTable: tableName)
        _ = try await client.execute(sql)
    }
    
    /// Removes a table comment
    @available(macOS 12.0, *)
    public func removeTableComment(tableName: String) async throws {
        let sql = try removeCommentSqlString(forTable: tableName)
        _ = try await client.execute(sql)
    }
    


    // MARK: - Database Management

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
    public func dropDatabase(name: String) async throws -> [SQLServerStreamMessage] {
        let escaped = Self.escapeIdentifier(name)
        let result = try await client.execute("DROP DATABASE \(escaped)")
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
            COALESCE((
                SELECT CAST(SUM(CAST(mf.size AS BIGINT)) * 8.0 / 1024 AS FLOAT)
                FROM sys.master_files mf
                WHERE mf.database_id = d.database_id
            ), 0) AS size_mb,
            COALESCE((
                SELECT COUNT(*)
                FROM sys.dm_exec_sessions s
                WHERE s.database_id = d.database_id
            ), 0) AS active_sessions,
            d.target_recovery_time_in_seconds,
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

        func boolCol(_ name: String) -> Bool { (row.column(name)?.int ?? 0) != 0 }

        return SQLServerDatabaseProperties(
            name: row.column("name")?.string ?? name,
            owner: row.column("owner")?.string ?? "",
            stateDescription: row.column("state_description")?.string ?? "UNKNOWN",
            createDate: row.column("create_date")?.string ?? "",
            sizeMB: row.column("size_mb")?.double ?? 0.0,
            activeSessions: row.column("active_sessions")?.int ?? 0,
            collationName: row.column("collation_name")?.string ?? "",
            recoveryModel: row.column("recovery_model")?.string ?? "UNKNOWN",
            compatibilityLevel: row.column("compatibility_level")?.int ?? 0,
            isReadOnly: boolCol("is_read_only"),
            userAccessDescription: row.column("user_access_description")?.string ?? "MULTI_USER",
            pageVerifyOption: row.column("page_verify_option")?.string ?? "NONE",
            targetRecoveryTimeSeconds: row.column("target_recovery_time_in_seconds")?.int ?? 0,
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
            mf.type_desc,
            mf.physical_name,
            CAST(mf.size AS BIGINT) * 8.0 / 1024 AS size_mb,
            CASE
                WHEN mf.max_size = -1 THEN 'Unlimited'
                WHEN mf.max_size = 0 THEN 'No Growth'
                ELSE CAST(CAST(mf.max_size AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
            END AS max_size_desc,
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
            SQLServerDatabaseFile(
                name: row.column("name")?.string ?? "",
                typeDescription: row.column("type_desc")?.string ?? "",
                physicalName: row.column("physical_name")?.string ?? "",
                sizeMB: row.column("size_mb")?.double ?? 0,
                maxSizeDescription: row.column("max_size_desc")?.string ?? "",
                growthDescription: row.column("growth_desc")?.string ?? "",
                fileGroupName: { let v = row.column("filegroup_name")?.string ?? ""; return v.isEmpty ? nil : v }()
            )
        }
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

    // MARK: - Table Management

    public func dropTable(name: String) -> EventLoopFuture<Void> {
        let sql = "DROP TABLE \(Self.escapeIdentifier(name))"
        return client.execute(sql).flatMapThrowing { result in
            if let errorMessage = result.messages.first(where: { $0.kind == .error }) {
                throw SQLServerError.sqlExecutionError(message: errorMessage.message)
            }
            return ()
        }
    }

    @available(macOS 12.0, *)
    public func dropTable(name: String) async throws {
        let sql = "DROP TABLE \(Self.escapeIdentifier(name))"
        _ = try await client.execute(sql)
    }



    @available(macOS 12.0, *)
    private func commentSqlString(forColumn column: String, inTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        
        // Use the simplest possible syntax that should work
        let sql = """
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
        return sql
    }
    
    private func commentSqlString(forTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)'
        """
        return sql
    }
    
    private func updateCommentSqlString(forColumn column: String, inTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_updateextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
        return sql
    }
    
    private func updateCommentSqlString(forTable table: String, comment: String) throws -> String {
        let escapedComment = comment.replacingOccurrences(of: "'", with: "''")
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_updateextendedproperty 
            N'MS_Description',
            N'\(escapedComment)',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)'
        """
        return sql
    }
    
    private func removeCommentSqlString(forColumn column: String, inTable table: String) throws -> String {
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let escapedColumn = column.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_dropextendedproperty 
            N'MS_Description',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)',
            N'COLUMN',
            N'\(escapedColumn)'
        """
        return sql
    }
    
    private func removeCommentSqlString(forTable table: String) throws -> String {
        let escapedTable = table.replacingOccurrences(of: "'", with: "''")
        let sql = """
        EXEC sp_dropextendedproperty 
            N'MS_Description',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(escapedTable)'
        """
        return sql
    }

    private func sqlString(for column: SQLServerColumnDefinition) throws -> String {
        let name = Self.escapeIdentifier(column.name)
        switch column.definition {
        case .computed(let expression, let persisted):
            return "\(name) AS (\(expression))\(persisted ? " PERSISTED" : "")"
        case .standard(let std):
            var parts = [String]()
            parts.append(name)
            parts.append(std.dataType.toSqlString())

            if let collate = std.collation, !collate.isEmpty {
                parts.append("COLLATE \(collate)")
            }

            if let identity = std.identity {
                parts.append("IDENTITY(\(identity.seed), \(identity.increment))")
            }

            if std.isSparse {
                parts.append("SPARSE")
            }

            parts.append(std.isNullable ? "NULL" : "NOT NULL")

            if std.isRowGuidCol {
                parts.append("ROWGUIDCOL")
            }

            if let defaultValue = std.defaultValue {
                parts.append("DEFAULT \(defaultValue)")
            }

            if std.isUnique {
                parts.append("UNIQUE")
            }

            return parts.joined(separator: " ")
        }
    }

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}
