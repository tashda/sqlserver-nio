import Foundation
import SQLServerTDS

/// Client for reading and modifying SQL Server instance-level configuration.
///
/// Wraps `SERVERPROPERTY()`, `sys.configurations` (`sp_configure`), `sys.dm_os_sys_info`,
/// and authentication/audit settings. Corresponds to the SSMS "Server Properties" dialog.
///
/// Usage:
/// ```swift
/// let info = try await client.serverConfig.fetchServerInfo()
/// let configs = try await client.serverConfig.listConfigurations()
/// try await client.serverConfig.setConfiguration(name: "max server memory (MB)", value: 8192)
/// ```
public final class SQLServerServerConfigurationClient: @unchecked Sendable {
    internal let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Server Properties (General Page)

    /// Fetches server-level properties from SERVERPROPERTY().
    /// All fields are read-only and correspond to the SSMS General page.
    @available(macOS 12.0, *)
    public func fetchServerInfo() async throws -> SQLServerServerInfo {
        let sql = """
        SELECT
            CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(256)) AS server_name,
            CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256)) AS edition,
            CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS product_version,
            CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(128)) AS product_level,
            CAST(SERVERPROPERTY('Collation') AS NVARCHAR(128)) AS collation,
            CAST(ISNULL(SERVERPROPERTY('IsClustered'), 0) AS INT) AS is_clustered,
            CAST(ISNULL(SERVERPROPERTY('IsHadrEnabled'), 0) AS INT) AS is_hadr_enabled,
            CAST(ISNULL(SERVERPROPERTY('IsIntegratedSecurityOnly'), 0) AS INT) AS is_integrated_security_only,
            CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(256)) AS machine_name,
            CAST(ISNULL(SERVERPROPERTY('InstanceDefaultDataPath'), '') AS NVARCHAR(512)) AS default_data_path,
            CAST(ISNULL(SERVERPROPERTY('InstanceDefaultLogPath'), '') AS NVARCHAR(512)) AS default_log_path,
            CAST(ISNULL(SERVERPROPERTY('InstanceDefaultBackupPath'), '') AS NVARCHAR(512)) AS default_backup_path,
            CAST(ISNULL(SERVERPROPERTY('FilestreamConfiguredLevel'), 0) AS INT) AS filestream_configured_level,
            CAST(ISNULL(SERVERPROPERTY('FilestreamEffectiveLevel'), 0) AS INT) AS filestream_effective_level,
            CAST(ISNULL(SERVERPROPERTY('FilestreamShareName'), '') AS NVARCHAR(256)) AS filestream_share_name,
            CAST(SERVERPROPERTY('EngineEdition') AS INT) AS engine_edition,
            CAST(SERVERPROPERTY('ProcessID') AS INT) AS process_id
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "Failed to fetch server properties")
        }

        return SQLServerServerInfo(
            serverName: row.column("server_name")?.string ?? "",
            product: "Microsoft SQL Server",
            edition: row.column("edition")?.string ?? "",
            productVersion: row.column("product_version")?.string ?? "",
            productLevel: row.column("product_level")?.string ?? "",
            collation: row.column("collation")?.string ?? "",
            isClustered: (row.column("is_clustered")?.int ?? 0) != 0,
            isHadrEnabled: (row.column("is_hadr_enabled")?.int ?? 0) != 0,
            isIntegratedSecurityOnly: (row.column("is_integrated_security_only")?.int ?? 0) != 0,
            machineName: row.column("machine_name")?.string ?? "",
            instanceDefaultDataPath: row.column("default_data_path")?.string ?? "",
            instanceDefaultLogPath: row.column("default_log_path")?.string ?? "",
            instanceDefaultBackupPath: row.column("default_backup_path")?.string ?? "",
            filestreamConfiguredLevel: row.column("filestream_configured_level")?.int ?? 0,
            filestreamEffectiveLevel: row.column("filestream_effective_level")?.int ?? 0,
            filestreamShareName: row.column("filestream_share_name")?.string ?? "",
            engineEdition: row.column("engine_edition")?.int ?? 0,
            processID: row.column("process_id")?.int ?? 0
        )
    }

    // MARK: - System Info (General Page hardware details)

    /// Fetches hardware and OS information from sys.dm_os_sys_info.
    /// Requires VIEW SERVER STATE permission.
    @available(macOS 12.0, *)
    public func fetchSystemInfo() async throws -> SQLServerSystemInfo {
        let sql = """
        SELECT
            cpu_count,
            socket_count,
            cores_per_socket,
            numa_node_count,
            CAST(physical_memory_kb / 1024 AS INT) AS physical_memory_mb,
            committed_kb,
            committed_target_kb,
            max_workers_count,
            CONVERT(VARCHAR(23), sqlserver_start_time, 121) AS sqlserver_start_time,
            affinity_type_desc AS affinity_type
        FROM sys.dm_os_sys_info
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "Failed to fetch system info from sys.dm_os_sys_info")
        }

        return SQLServerSystemInfo(
            cpuCount: row.column("cpu_count")?.int ?? 0,
            socketCount: row.column("socket_count")?.int ?? 0,
            coresPerSocket: row.column("cores_per_socket")?.int ?? 0,
            numaNodeCount: row.column("numa_node_count")?.int ?? 0,
            physicalMemoryMB: row.column("physical_memory_mb")?.int ?? 0,
            committedKB: row.column("committed_kb")?.int ?? 0,
            committedTargetKB: row.column("committed_target_kb")?.int ?? 0,
            maxWorkersCount: row.column("max_workers_count")?.int ?? 0,
            sqlServerStartTime: row.column("sqlserver_start_time")?.string ?? "",
            affinityType: row.column("affinity_type")?.string ?? ""
        )
    }

    // MARK: - Configuration Options (sp_configure / sys.configurations)

    /// Lists all server configuration options from sys.configurations.
    ///
    /// By default only shows non-advanced options. Set `showAdvanced` to true to include
    /// advanced options (this temporarily enables "show advanced options" if needed, then restores it).
    @available(macOS 12.0, *)
    public func listConfigurations(showAdvanced: Bool = true) async throws -> [SQLServerConfigurationOption] {
        if showAdvanced {
            // Ensure advanced options are visible
            let checkSQL = "SELECT CAST(value_in_use AS INT) AS val FROM sys.configurations WHERE name = 'show advanced options'"
            let checkRows = try await client.query(checkSQL)
            let advancedEnabled = (checkRows.first?.column("val")?.int ?? 0) != 0

            if !advancedEnabled {
                _ = try await client.execute("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;")
            }

            let options = try await fetchAllConfigurations()

            if !advancedEnabled {
                _ = try await client.execute("EXEC sp_configure 'show advanced options', 0; RECONFIGURE;")
            }

            return options
        } else {
            return try await fetchAllConfigurations()
        }
    }

    /// Fetches a single configuration option by name.
    @available(macOS 12.0, *)
    public func getConfiguration(name: String) async throws -> SQLServerConfigurationOption {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT
            configuration_id,
            name,
            CAST(minimum AS BIGINT) AS minimum,
            CAST(maximum AS BIGINT) AS maximum,
            CAST(value AS BIGINT) AS configured_value,
            CAST(value_in_use AS BIGINT) AS running_value,
            CAST(description AS NVARCHAR(512)) AS description,
            is_dynamic,
            is_advanced
        FROM sys.configurations
        WHERE name = N'\(escapedName)'
        """

        let rows = try await client.query(sql)
        guard let row = rows.first else {
            throw SQLServerError.sqlExecutionError(message: "Configuration option '\(name)' not found")
        }

        return parseConfigurationRow(row)
    }

    /// Sets a server configuration option via sp_configure and RECONFIGURE.
    ///
    /// For advanced options, this automatically enables "show advanced options" first.
    /// For non-dynamic options, the change will not take effect until SQL Server is restarted.
    ///
    /// - Parameters:
    ///   - name: The sp_configure option name (e.g. "max server memory (MB)").
    ///   - value: The new value to set.
    ///   - withOverride: Use RECONFIGURE WITH OVERRIDE instead of RECONFIGURE.
    ///     Required for some options like "recovery interval".
    @available(macOS 12.0, *)
    @discardableResult
    public func setConfiguration(
        name: String,
        value: Int64,
        withOverride: Bool = false
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let reconfigure = withOverride ? "RECONFIGURE WITH OVERRIDE" : "RECONFIGURE"

        // Check if this is an advanced option
        let option = try await getConfiguration(name: name)
        var sql = ""

        if option.isAdvanced {
            // Ensure advanced options are enabled
            let checkSQL = "SELECT CAST(value_in_use AS INT) AS val FROM sys.configurations WHERE name = 'show advanced options'"
            let checkRows = try await client.query(checkSQL)
            let advancedEnabled = (checkRows.first?.column("val")?.int ?? 0) != 0

            if !advancedEnabled {
                sql += "EXEC sp_configure 'show advanced options', 1; RECONFIGURE;\n"
            }

            sql += "EXEC sp_configure '\(escapedName)', \(value); \(reconfigure);"

            if !advancedEnabled {
                sql += "\nEXEC sp_configure 'show advanced options', 0; RECONFIGURE;"
            }
        } else {
            sql = "EXEC sp_configure '\(escapedName)', \(value); \(reconfigure);"
        }

        let result = try await client.execute(sql)
        return result.messages
    }

    /// Sets multiple configuration options in a single batch.
    /// All options are set and then RECONFIGURE is called once at the end.
    @available(macOS 12.0, *)
    @discardableResult
    public func setConfigurations(
        _ options: [(name: String, value: Int64)],
        withOverride: Bool = false
    ) async throws -> [SQLServerStreamMessage] {
        guard !options.isEmpty else { return [] }

        let reconfigure = withOverride ? "RECONFIGURE WITH OVERRIDE" : "RECONFIGURE"
        var sql = "EXEC sp_configure 'show advanced options', 1; RECONFIGURE;\n"

        for option in options {
            let escapedName = option.name.replacingOccurrences(of: "'", with: "''")
            sql += "EXEC sp_configure '\(escapedName)', \(option.value);\n"
        }

        sql += reconfigure + ";"
        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Security Settings

    /// Fetches server authentication mode and login audit level.
    /// Corresponds to the SSMS Server Properties > Security page.
    @available(macOS 12.0, *)
    public func fetchSecuritySettings() async throws -> SQLServerSecuritySettings {
        // Authentication mode from SERVERPROPERTY
        let authSQL = "SELECT CAST(ISNULL(SERVERPROPERTY('IsIntegratedSecurityOnly'), 0) AS INT) AS is_windows_only"
        let authRows = try await client.query(authSQL)
        let isWindowsOnly = (authRows.first?.column("is_windows_only")?.int ?? 0) != 0

        // Login audit level from xp_loginconfig
        let auditSQL = "EXEC xp_loginconfig 'audit level'"
        let auditRows = try await client.query(auditSQL)
        let auditLevelString = auditRows.first?.column("config_value")?.string ?? "none"

        let auditLevel: SQLServerLoginAuditLevel
        switch auditLevelString.lowercased() {
        case "none":
            auditLevel = .none
        case "failure":
            auditLevel = .failedLoginsOnly
        case "success":
            auditLevel = .successfulLoginsOnly
        case "all":
            auditLevel = .both
        default:
            auditLevel = .none
        }

        return SQLServerSecuritySettings(
            authenticationMode: isWindowsOnly ? .windowsOnly : .mixed,
            loginAuditLevel: auditLevel
        )
    }

    // MARK: - Default Paths

    /// Updates the default data file path for new databases.
    /// Requires sysadmin role. Change takes effect for new databases immediately.
    @available(macOS 12.0, *)
    @discardableResult
    public func setDefaultDataPath(_ path: String) async throws -> [SQLServerStreamMessage] {
        let escapedPath = path.replacingOccurrences(of: "'", with: "''")
        let sql = "EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', N'DefaultData', REG_SZ, N'\(escapedPath)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Updates the default log file path for new databases.
    /// Requires sysadmin role. Change takes effect for new databases immediately.
    @available(macOS 12.0, *)
    @discardableResult
    public func setDefaultLogPath(_ path: String) async throws -> [SQLServerStreamMessage] {
        let escapedPath = path.replacingOccurrences(of: "'", with: "''")
        let sql = "EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', N'DefaultLog', REG_SZ, N'\(escapedPath)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    /// Updates the default backup path.
    /// Requires sysadmin role.
    @available(macOS 12.0, *)
    @discardableResult
    public func setDefaultBackupPath(_ path: String) async throws -> [SQLServerStreamMessage] {
        let escapedPath = path.replacingOccurrences(of: "'", with: "''")
        let sql = "EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', N'BackupDirectory', REG_SZ, N'\(escapedPath)'"
        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Startup Parameters (Phase 7.4)

    /// Fetches the SQL Server startup parameters from the registry.
    /// Requires VIEW SERVER STATE and sysadmin permissions.
    @available(macOS 12.0, *)
    public func fetchStartupParameters() async throws -> [String] {
        let sql = "SELECT [value] FROM sys.dm_server_registry WHERE registry_key LIKE '%\\MSSQLServer\\Parameters'"
        let rows = try await client.query(sql)
        return rows.compactMap { $0.column("value")?.string }
    }

    // MARK: - Private

    @available(macOS 12.0, *)
    private func fetchAllConfigurations() async throws -> [SQLServerConfigurationOption] {
        let sql = """
        SELECT
            configuration_id,
            name,
            CAST(minimum AS BIGINT) AS minimum,
            CAST(maximum AS BIGINT) AS maximum,
            CAST(value AS BIGINT) AS configured_value,
            CAST(value_in_use AS BIGINT) AS running_value,
            CAST(description AS NVARCHAR(512)) AS description,
            is_dynamic,
            is_advanced
        FROM sys.configurations
        ORDER BY name
        """

        let rows = try await client.query(sql)
        return rows.map { parseConfigurationRow($0) }
    }

    private func parseConfigurationRow(_ row: SQLServerRow) -> SQLServerConfigurationOption {
        SQLServerConfigurationOption(
            configurationID: row.column("configuration_id")?.int ?? 0,
            name: row.column("name")?.string ?? "",
            minimum: Int64(row.column("minimum")?.string ?? "0") ?? 0,
            maximum: Int64(row.column("maximum")?.string ?? "0") ?? 0,
            configuredValue: Int64(row.column("configured_value")?.string ?? "0") ?? 0,
            runningValue: Int64(row.column("running_value")?.string ?? "0") ?? 0,
            description: row.column("description")?.string ?? "",
            isDynamic: (row.column("is_dynamic")?.int ?? 0) != 0,
            isAdvanced: (row.column("is_advanced")?.int ?? 0) != 0
        )
    }
}
