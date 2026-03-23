import Foundation

// MARK: - Filegroups

/// A filegroup within a database, from sys.filegroups.
public struct SQLServerFilegroup: Sendable, Identifiable {
    public let dataSpaceID: Int
    public let name: String
    /// Type description: "ROWS_FILEGROUP", "FILESTREAM_DATA_FILEGROUP", or "MEMORY_OPTIMIZED_DATA_FILEGROUP".
    public let typeDescription: String
    /// Whether this is the default filegroup for new tables.
    public let isDefault: Bool
    /// Whether this filegroup is read-only.
    public let isReadOnly: Bool
    /// Whether this is a system filegroup (PRIMARY).
    public let isSystem: Bool
    /// Number of files in this filegroup.
    public let fileCount: Int

    public var id: Int { dataSpaceID }

    /// Whether this is the PRIMARY filegroup.
    public var isPrimary: Bool { name == "PRIMARY" }

    /// Whether this is a memory-optimized data filegroup.
    public var isMemoryOptimized: Bool { typeDescription == "MEMORY_OPTIMIZED_DATA_FILEGROUP" }

    /// Whether this is a FILESTREAM filegroup.
    public var isFilestream: Bool { typeDescription == "FILESTREAM_DATA_FILEGROUP" }

    public init(
        dataSpaceID: Int,
        name: String,
        typeDescription: String,
        isDefault: Bool,
        isReadOnly: Bool,
        isSystem: Bool,
        fileCount: Int
    ) {
        self.dataSpaceID = dataSpaceID
        self.name = name
        self.typeDescription = typeDescription
        self.isDefault = isDefault
        self.isReadOnly = isReadOnly
        self.isSystem = isSystem
        self.fileCount = fileCount
    }
}

// MARK: - Database Scoped Configurations

/// A database scoped configuration option from sys.database_scoped_configurations.
/// Available on SQL Server 2016+ (compatibility level 130+).
public struct SQLServerScopedConfiguration: Sendable, Identifiable {
    public let configurationID: Int
    /// Option name (e.g. "MAXDOP", "LEGACY_CARDINALITY_ESTIMATION", "PARAMETER_SNIFFING").
    public let name: String
    /// Current value for the primary database.
    public let value: String
    /// Current value for secondary replicas (nil if not set separately).
    public let valueForSecondary: String?

    public var id: Int { configurationID }

    public init(
        configurationID: Int,
        name: String,
        value: String,
        valueForSecondary: String?
    ) {
        self.configurationID = configurationID
        self.name = name
        self.value = value
        self.valueForSecondary = valueForSecondary
    }
}

// MARK: - Mirroring

/// Database mirroring status from sys.database_mirroring.
public struct SQLServerMirroringStatus: Sendable {
    /// Whether mirroring is configured for this database.
    public let isConfigured: Bool
    /// Mirroring state: "SYNCHRONIZED", "SYNCHRONIZING", "SUSPENDED", "PENDING_FAILOVER", "DISCONNECTED", or nil if not configured.
    public let stateDescription: String?
    /// Role: "PRINCIPAL" or "MIRROR", or nil.
    public let roleDescription: String?
    /// Safety level: "FULL" (synchronous) or "OFF" (asynchronous), or nil.
    public let safetyLevelDescription: String?
    /// Partner server endpoint address (TCP://host:port).
    public let partnerName: String
    /// Partner server instance name.
    public let partnerInstance: String
    /// Witness server endpoint address, or empty.
    public let witnessName: String
    /// Witness state: "CONNECTED", "DISCONNECTED", "UNKNOWN", or empty.
    public let witnessStateDescription: String
    /// Connection timeout in seconds.
    public let connectionTimeout: Int?
    /// Number of redo log records in the redo queue.
    public let redoQueue: Int?
    /// Redo queue type description.
    public let redoQueueType: String?

    public init(
        isConfigured: Bool,
        stateDescription: String?,
        roleDescription: String?,
        safetyLevelDescription: String?,
        partnerName: String,
        partnerInstance: String,
        witnessName: String,
        witnessStateDescription: String,
        connectionTimeout: Int?,
        redoQueue: Int?,
        redoQueueType: String?
    ) {
        self.isConfigured = isConfigured
        self.stateDescription = stateDescription
        self.roleDescription = roleDescription
        self.safetyLevelDescription = safetyLevelDescription
        self.partnerName = partnerName
        self.partnerInstance = partnerInstance
        self.witnessName = witnessName
        self.witnessStateDescription = witnessStateDescription
        self.connectionTimeout = connectionTimeout
        self.redoQueue = redoQueue
        self.redoQueueType = redoQueueType
    }
}

/// Mirroring safety level (operating mode).
public enum SQLServerMirroringSafetyLevel: String, Sendable {
    /// High safety (synchronous) — all committed transactions guaranteed on both copies.
    case full = "FULL"
    /// High performance (asynchronous) — mirror may lag behind principal.
    case off = "OFF"
}

// MARK: - Log Shipping

/// Log shipping primary configuration from msdb.dbo.log_shipping_primary_databases.
public struct SQLServerLogShippingConfig: Sendable {
    public let primaryID: String
    public let primaryDatabase: String
    public let backupDirectory: String
    public let backupShare: String
    public let backupRetentionPeriodMinutes: Int
    public let backupCompression: Bool
    public let monitorServer: String?
    /// 0 = Windows Authentication, 1 = SQL Server Authentication.
    public let monitorServerSecurityMode: Int
    public let lastBackupDate: String?
    public let lastBackupFile: String
    public let secondaries: [SQLServerLogShippingSecondary]

    public init(
        primaryID: String,
        primaryDatabase: String,
        backupDirectory: String,
        backupShare: String,
        backupRetentionPeriodMinutes: Int,
        backupCompression: Bool,
        monitorServer: String?,
        monitorServerSecurityMode: Int,
        lastBackupDate: String?,
        lastBackupFile: String,
        secondaries: [SQLServerLogShippingSecondary]
    ) {
        self.primaryID = primaryID
        self.primaryDatabase = primaryDatabase
        self.backupDirectory = backupDirectory
        self.backupShare = backupShare
        self.backupRetentionPeriodMinutes = backupRetentionPeriodMinutes
        self.backupCompression = backupCompression
        self.monitorServer = monitorServer
        self.monitorServerSecurityMode = monitorServerSecurityMode
        self.lastBackupDate = lastBackupDate
        self.lastBackupFile = lastBackupFile
        self.secondaries = secondaries
    }
}

/// A log shipping secondary server/database entry.
public struct SQLServerLogShippingSecondary: Sendable {
    public let secondaryServer: String
    public let secondaryDatabase: String
    public let lastCopiedDate: String?
    public let lastRestoredDate: String?

    public init(
        secondaryServer: String,
        secondaryDatabase: String,
        lastCopiedDate: String?,
        lastRestoredDate: String?
    ) {
        self.secondaryServer = secondaryServer
        self.secondaryDatabase = secondaryDatabase
        self.lastCopiedDate = lastCopiedDate
        self.lastRestoredDate = lastRestoredDate
    }
}

/// Log shipping secondary configuration from msdb.dbo.log_shipping_secondary_databases.
public struct SQLServerLogShippingSecondaryConfig: Sendable {
    public let secondaryID: String
    public let secondaryDatabase: String
    public let primaryServer: String
    public let primaryDatabase: String
    public let restoreDelayMinutes: Int
    /// 0 = NORECOVERY, 1 = STANDBY.
    public let restoreMode: Int
    public let disconnectUsers: Bool
    public let lastRestoredDate: String?
    public let lastRestoredFile: String

    public init(
        secondaryID: String,
        secondaryDatabase: String,
        primaryServer: String,
        primaryDatabase: String,
        restoreDelayMinutes: Int,
        restoreMode: Int,
        disconnectUsers: Bool,
        lastRestoredDate: String?,
        lastRestoredFile: String
    ) {
        self.secondaryID = secondaryID
        self.secondaryDatabase = secondaryDatabase
        self.primaryServer = primaryServer
        self.primaryDatabase = primaryDatabase
        self.restoreDelayMinutes = restoreDelayMinutes
        self.restoreMode = restoreMode
        self.disconnectUsers = disconnectUsers
        self.lastRestoredDate = lastRestoredDate
        self.lastRestoredFile = lastRestoredFile
    }
}

// MARK: - FILESTREAM Options

/// FILESTREAM options for a database from sys.database_filestream_options.
public struct SQLServerFilestreamOptions: Sendable {
    /// FILESTREAM directory name within the database.
    public let directoryName: String
    /// Non-transacted access level: 0=OFF, 1=READ_ONLY, 2=FULL.
    public let nonTransactedAccess: Int
    /// Non-transacted access description: "OFF", "READ_ONLY", or "FULL".
    public let nonTransactedAccessDescription: String

    public init(
        directoryName: String,
        nonTransactedAccess: Int,
        nonTransactedAccessDescription: String
    ) {
        self.directoryName = directoryName
        self.nonTransactedAccess = nonTransactedAccess
        self.nonTransactedAccessDescription = nonTransactedAccessDescription
    }
}

/// FILESTREAM non-transacted access level.
public enum SQLServerFilestreamAccessLevel: String, Sendable, CaseIterable {
    case off = "OFF"
    case readOnly = "READ_ONLY"
    case full = "FULL"
}

// MARK: - Containment Properties

/// Containment-specific properties for a contained database.
public struct SQLServerContainmentProperties: Sendable {
    /// "NONE" or "PARTIAL".
    public let containmentDescription: String
    public let defaultFulltextLanguageLCID: Int
    public let defaultFulltextLanguageName: String
    public let defaultLanguageLCID: Int
    public let defaultLanguageName: String
    public let isNestedTriggersOn: Bool
    public let isTransformNoiseWordsOn: Bool
    public let twoDigitYearCutoff: Int

    public init(
        containmentDescription: String,
        defaultFulltextLanguageLCID: Int,
        defaultFulltextLanguageName: String,
        defaultLanguageLCID: Int,
        defaultLanguageName: String,
        isNestedTriggersOn: Bool,
        isTransformNoiseWordsOn: Bool,
        twoDigitYearCutoff: Int
    ) {
        self.containmentDescription = containmentDescription
        self.defaultFulltextLanguageLCID = defaultFulltextLanguageLCID
        self.defaultFulltextLanguageName = defaultFulltextLanguageName
        self.defaultLanguageLCID = defaultLanguageLCID
        self.defaultLanguageName = defaultLanguageName
        self.isNestedTriggersOn = isNestedTriggersOn
        self.isTransformNoiseWordsOn = isTransformNoiseWordsOn
        self.twoDigitYearCutoff = twoDigitYearCutoff
    }
}

// MARK: - Cursor Defaults

/// Cursor-related database options.
public struct SQLServerCursorDefaults: Sendable {
    /// Whether cursors default to LOCAL scope (true) or GLOBAL scope (false).
    public let isLocalCursorDefault: Bool
    /// Whether open cursors are closed when a transaction commits.
    public let isCursorCloseOnCommitOn: Bool

    public init(isLocalCursorDefault: Bool, isCursorCloseOnCommitOn: Bool) {
        self.isLocalCursorDefault = isLocalCursorDefault
        self.isCursorCloseOnCommitOn = isCursorCloseOnCommitOn
    }
}

// MARK: - Service Broker Properties

/// Service Broker properties for a database.
public struct SQLServerServiceBrokerProperties: Sendable {
    public let isBrokerEnabled: Bool
    public let isHonorBrokerPriorityOn: Bool
    /// Unique identifier for this database's Service Broker.
    public let serviceBrokerGUID: String

    public init(
        isBrokerEnabled: Bool,
        isHonorBrokerPriorityOn: Bool,
        serviceBrokerGUID: String
    ) {
        self.isBrokerEnabled = isBrokerEnabled
        self.isHonorBrokerPriorityOn = isHonorBrokerPriorityOn
        self.serviceBrokerGUID = serviceBrokerGUID
    }
}
