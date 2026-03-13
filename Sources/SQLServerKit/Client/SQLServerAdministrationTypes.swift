import Foundation
import SQLServerTDS

public struct SQLServerLoginInfo: Sendable {
    public let name: String
    public let type: String
    public let isDisabled: Bool
    
    public init(name: String, type: String, isDisabled: Bool) {
        self.name = name
        self.type = type
        self.isDisabled = isDisabled
    }
}

public struct SQLServerRoleInfo: Sendable {
    public let name: String
    public let isFixedRole: Bool
    
    public init(name: String, isFixedRole: Bool) {
        self.name = name
        self.isFixedRole = isFixedRole
    }
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

    /// Raw size in 8KB pages (for modification operations).
    public let sizePages: Int
    /// Raw max_size value (-1 = unlimited, 0 = no growth, positive = pages).
    public let maxSizeRaw: Int
    /// Raw growth value (pages or percentage).
    public let growthRaw: Int
    /// Whether growth is expressed as a percentage.
    public let isPercentGrowth: Bool
    /// File type code (0 = ROWS, 1 = LOG, 2 = FILESTREAM, etc.)
    public let type: Int

    public init(
        name: String,
        typeDescription: String,
        physicalName: String,
        sizeMB: Double,
        maxSizeDescription: String,
        growthDescription: String,
        fileGroupName: String?,
        sizePages: Int = 0,
        maxSizeRaw: Int = -1,
        growthRaw: Int = 0,
        isPercentGrowth: Bool = false,
        type: Int = 0
    ) {
        self.name = name
        self.typeDescription = typeDescription
        self.physicalName = physicalName
        self.sizeMB = sizeMB
        self.maxSizeDescription = maxSizeDescription
        self.growthDescription = growthDescription
        self.fileGroupName = fileGroupName
        self.sizePages = sizePages
        self.maxSizeRaw = maxSizeRaw
        self.growthRaw = growthRaw
        self.isPercentGrowth = isPercentGrowth
        self.type = type
    }

    /// Computed growth in MB (returns nil if percent growth).
    public var growthMB: Int? {
        guard !isPercentGrowth else { return nil }
        return growthRaw * 8 / 1024
    }

    /// Computed growth percentage (returns nil if not percent growth).
    public var growthPercent: Int? {
        guard isPercentGrowth else { return nil }
        return growthRaw
    }

    /// Computed max size in MB (returns nil if unlimited or no growth).
    public var maxSizeMB: Int? {
        guard maxSizeRaw > 0 else { return nil }
        return maxSizeRaw * 8 / 1024
    }

    /// Whether the file has unlimited max size.
    public var isMaxSizeUnlimited: Bool { maxSizeRaw == -1 }

    /// Whether the file is set to no growth.
    public var isNoGrowth: Bool { maxSizeRaw == 0 && growthRaw == 0 }
}

/// Options for modifying a database file via ALTER DATABASE MODIFY FILE.
public enum SQLServerDatabaseFileOption: Sendable {
    /// Resize the file to the specified size in MB.
    case sizeMB(Int)
    /// Set the maximum size in MB. Use -1 for unlimited.
    case maxSizeMB(Int)
    /// Set max size to unlimited.
    case maxSizeUnlimited
    /// Set file growth by MB.
    case filegrowthMB(Int)
    /// Set file growth by percentage.
    case filegrowthPercent(Int)
    /// Disable file growth.
    case filegrowthNone
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
