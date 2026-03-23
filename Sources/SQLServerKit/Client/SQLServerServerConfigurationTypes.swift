import Foundation

// MARK: - Server Properties (SERVERPROPERTY)

/// Server-level properties retrieved from SERVERPROPERTY() and related system views.
/// Corresponds to the SSMS Server Properties > General page.
public struct SQLServerServerInfo: Sendable {
    /// Server instance name (e.g. "MYSERVER\\SQLEXPRESS" or "MYSERVER").
    public let serverName: String
    /// Product name (e.g. "Microsoft SQL Server").
    public let product: String
    /// Product edition (e.g. "Enterprise Edition (64-bit)").
    public let edition: String
    /// Version string (e.g. "16.0.1000.6").
    public let productVersion: String
    /// Product level (e.g. "RTM", "SP1", "CTP2.1").
    public let productLevel: String
    /// Server collation name.
    public let collation: String
    /// Whether the server is part of a failover cluster.
    public let isClustered: Bool
    /// Whether Always On availability groups is enabled.
    public let isHadrEnabled: Bool
    /// Whether only Windows Authentication is enabled (true) or mixed mode (false).
    public let isIntegratedSecurityOnly: Bool
    /// Physical computer name.
    public let machineName: String
    /// Default data file path for new databases.
    public let instanceDefaultDataPath: String
    /// Default log file path for new databases.
    public let instanceDefaultLogPath: String
    /// Default backup path (SQL Server 2019+, empty on older versions).
    public let instanceDefaultBackupPath: String
    /// FILESTREAM configured level (0=disabled, 1=T-SQL, 2=T-SQL+streaming, 3=remote).
    public let filestreamConfiguredLevel: Int
    /// FILESTREAM effective level.
    public let filestreamEffectiveLevel: Int
    /// FILESTREAM share name (empty if not configured).
    public let filestreamShareName: String
    /// Engine edition code (1=Personal/Desktop, 2=Standard, 3=Enterprise, 4=Express, 5=Azure SQL, etc.).
    public let engineEdition: Int
    /// SQL Server process ID.
    public let processID: Int

    public init(
        serverName: String,
        product: String,
        edition: String,
        productVersion: String,
        productLevel: String,
        collation: String,
        isClustered: Bool,
        isHadrEnabled: Bool,
        isIntegratedSecurityOnly: Bool,
        machineName: String,
        instanceDefaultDataPath: String,
        instanceDefaultLogPath: String,
        instanceDefaultBackupPath: String,
        filestreamConfiguredLevel: Int,
        filestreamEffectiveLevel: Int,
        filestreamShareName: String,
        engineEdition: Int,
        processID: Int
    ) {
        self.serverName = serverName
        self.product = product
        self.edition = edition
        self.productVersion = productVersion
        self.productLevel = productLevel
        self.collation = collation
        self.isClustered = isClustered
        self.isHadrEnabled = isHadrEnabled
        self.isIntegratedSecurityOnly = isIntegratedSecurityOnly
        self.machineName = machineName
        self.instanceDefaultDataPath = instanceDefaultDataPath
        self.instanceDefaultLogPath = instanceDefaultLogPath
        self.instanceDefaultBackupPath = instanceDefaultBackupPath
        self.filestreamConfiguredLevel = filestreamConfiguredLevel
        self.filestreamEffectiveLevel = filestreamEffectiveLevel
        self.filestreamShareName = filestreamShareName
        self.engineEdition = engineEdition
        self.processID = processID
    }
}

// MARK: - System Info (sys.dm_os_sys_info)

/// Hardware and OS information from sys.dm_os_sys_info.
public struct SQLServerSystemInfo: Sendable {
    /// Number of logical CPUs available to SQL Server.
    public let cpuCount: Int
    /// Number of physical CPU sockets (hyperthreaded cores are counted separately in cpuCount).
    public let socketCount: Int
    /// Number of cores per socket.
    public let coresPerSocket: Int
    /// Number of NUMA nodes.
    public let numaNodeCount: Int
    /// Physical memory on the machine in MB.
    public let physicalMemoryMB: Int
    /// Virtual memory committed by SQL Server in KB.
    public let committedKB: Int
    /// Virtual memory target for SQL Server in KB.
    public let committedTargetKB: Int
    /// Maximum number of worker threads configured.
    public let maxWorkersCount: Int
    /// SQL Server start time.
    public let sqlServerStartTime: String
    /// Processor affinity mask.
    public let affinityType: String

    public init(
        cpuCount: Int,
        socketCount: Int,
        coresPerSocket: Int,
        numaNodeCount: Int,
        physicalMemoryMB: Int,
        committedKB: Int,
        committedTargetKB: Int,
        maxWorkersCount: Int,
        sqlServerStartTime: String,
        affinityType: String
    ) {
        self.cpuCount = cpuCount
        self.socketCount = socketCount
        self.coresPerSocket = coresPerSocket
        self.numaNodeCount = numaNodeCount
        self.physicalMemoryMB = physicalMemoryMB
        self.committedKB = committedKB
        self.committedTargetKB = committedTargetKB
        self.maxWorkersCount = maxWorkersCount
        self.sqlServerStartTime = sqlServerStartTime
        self.affinityType = affinityType
    }
}

// MARK: - Configuration Option (sys.configurations / sp_configure)

/// A single server configuration option from sys.configurations.
/// Each option has a configured value and a running (in-use) value.
public struct SQLServerConfigurationOption: Sendable, Identifiable {
    /// Unique configuration ID.
    public let configurationID: Int
    /// Option name as it appears in sp_configure (e.g. "max server memory (MB)").
    public let name: String
    /// Minimum allowed value.
    public let minimum: Int64
    /// Maximum allowed value.
    public let maximum: Int64
    /// Configured value (set by sp_configure but may not yet be active).
    public let configuredValue: Int64
    /// Currently active running value.
    public let runningValue: Int64
    /// Description of what this option controls.
    public let description: String
    /// Whether the option takes effect immediately with RECONFIGURE (true) or requires a restart (false).
    public let isDynamic: Bool
    /// Whether this is an advanced option (only visible when "show advanced options" = 1).
    public let isAdvanced: Bool

    public var id: Int { configurationID }

    /// Whether the configured value differs from the running value (pending restart).
    public var isPendingRestart: Bool { configuredValue != runningValue }

    public init(
        configurationID: Int,
        name: String,
        minimum: Int64,
        maximum: Int64,
        configuredValue: Int64,
        runningValue: Int64,
        description: String,
        isDynamic: Bool,
        isAdvanced: Bool
    ) {
        self.configurationID = configurationID
        self.name = name
        self.minimum = minimum
        self.maximum = maximum
        self.configuredValue = configuredValue
        self.runningValue = runningValue
        self.description = description
        self.isDynamic = isDynamic
        self.isAdvanced = isAdvanced
    }
}

// MARK: - Authentication & Audit

/// Server authentication mode.
public enum SQLServerAuthenticationMode: String, Sendable {
    /// Windows Authentication only.
    case windowsOnly = "Windows Authentication"
    /// SQL Server and Windows Authentication (mixed mode).
    case mixed = "SQL Server and Windows Authentication"
}

/// Login audit level setting.
public enum SQLServerLoginAuditLevel: Int, Sendable {
    /// No login auditing.
    case none = 0
    /// Audit failed logins only.
    case failedLoginsOnly = 1
    /// Audit successful logins only.
    case successfulLoginsOnly = 2
    /// Audit both failed and successful logins.
    case both = 3
}

/// Combined server security settings from the SSMS Security page.
public struct SQLServerSecuritySettings: Sendable {
    /// Authentication mode (Windows-only or mixed).
    public let authenticationMode: SQLServerAuthenticationMode
    /// Login audit level.
    public let loginAuditLevel: SQLServerLoginAuditLevel

    public init(
        authenticationMode: SQLServerAuthenticationMode,
        loginAuditLevel: SQLServerLoginAuditLevel
    ) {
        self.authenticationMode = authenticationMode
        self.loginAuditLevel = loginAuditLevel
    }
}

// MARK: - Well-Known Configuration Names

/// Well-known sp_configure option names matching SSMS Server Properties pages.
public enum SQLServerConfigurationName {
    // Memory page
    public static let minServerMemory = "min server memory (MB)"
    public static let maxServerMemory = "max server memory (MB)"
    public static let indexCreateMemory = "index create memory (KB)"
    public static let minMemoryPerQuery = "min memory per query (KB)"

    // Processors page
    public static let affinityMask = "affinity mask"
    public static let affinity64Mask = "affinity64 mask"
    public static let affinityIOMask = "affinity I/O mask"
    public static let affinity64IOMask = "affinity64 I/O mask"
    public static let maxWorkerThreads = "max worker threads"
    public static let priorityBoost = "priority boost"
    public static let lightweightPooling = "lightweight pooling"

    // Security page
    public static let c2AuditMode = "c2 audit mode"
    public static let crossDbOwnershipChaining = "cross db ownership chaining"
    public static let xpCmdshell = "xp_cmdshell"

    // Connections page
    public static let userConnections = "user connections"
    public static let userOptions = "user options"
    public static let remoteAccess = "remote access"
    public static let remoteQueryTimeout = "remote query timeout (s)"
    public static let remoteProcTrans = "remote proc trans"

    // Database Settings page
    public static let fillFactor = "fill factor (%)"
    public static let mediaRetention = "media retention"
    public static let backupCompressionDefault = "backup compression default"
    public static let backupChecksumDefault = "backup checksum default"
    public static let recoveryInterval = "recovery interval (min)"

    // Advanced page — Containment
    public static let containedDatabaseAuthentication = "contained database authentication"

    // Advanced page — FILESTREAM
    public static let filestreamAccessLevel = "filestream access level"

    // Advanced page — Miscellaneous
    public static let nestedTriggers = "nested triggers"
    public static let blockedProcessThreshold = "blocked process threshold (s)"
    public static let cursorThreshold = "cursor threshold"
    public static let defaultFullTextLanguage = "default full-text language"
    public static let defaultLanguage = "default language"
    public static let maxTextReplSize = "max text repl size (B)"
    public static let scanForStartupProcs = "scan for startup procs"
    public static let twoDigitYearCutoff = "two digit year cutoff"

    // Advanced page — Network
    public static let networkPacketSize = "network packet size (B)"
    public static let remoteLoginTimeout = "remote login timeout (s)"

    // Advanced page — Parallelism
    public static let costThresholdForParallelism = "cost threshold for parallelism"
    public static let locks = "locks"
    public static let maxDegreeOfParallelism = "max degree of parallelism"
    public static let queryWait = "query wait (s)"

    // Misc Server Settings page
    public static let queryGovernorCostLimit = "query governor cost limit"

    // Internal
    public static let showAdvancedOptions = "show advanced options"
}

// MARK: - User Options Bitmask

/// Bitmask values for the `user options` sp_configure setting.
/// These correspond to the checkboxes on the SSMS Connections page.
public struct SQLServerUserOptions: OptionSet, Sendable {
    public let rawValue: Int

    public init(rawValue: Int) {
        self.rawValue = rawValue
    }

    public static let disableDeferredConstraintChecking = SQLServerUserOptions(rawValue: 1)
    public static let implicitTransactions = SQLServerUserOptions(rawValue: 2)
    public static let cursorCloseOnCommit = SQLServerUserOptions(rawValue: 4)
    public static let ansiWarnings = SQLServerUserOptions(rawValue: 8)
    public static let ansiPadding = SQLServerUserOptions(rawValue: 16)
    public static let ansiNulls = SQLServerUserOptions(rawValue: 32)
    public static let arithmeticAbort = SQLServerUserOptions(rawValue: 64)
    public static let arithmeticIgnore = SQLServerUserOptions(rawValue: 128)
    public static let quotedIdentifier = SQLServerUserOptions(rawValue: 256)
    public static let noCount = SQLServerUserOptions(rawValue: 512)
    public static let ansiNullDefaultOn = SQLServerUserOptions(rawValue: 1024)
    public static let ansiNullDefaultOff = SQLServerUserOptions(rawValue: 2048)
    public static let concatNullYieldsNull = SQLServerUserOptions(rawValue: 4096)
    public static let numericRoundAbort = SQLServerUserOptions(rawValue: 8192)
    public static let xactAbort = SQLServerUserOptions(rawValue: 16384)
}
