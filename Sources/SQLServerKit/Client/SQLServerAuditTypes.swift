import Foundation

// MARK: - Audit Types

/// The destination for a server audit.
public enum AuditDestination: String, Sendable, Hashable {
    case file = "FILE"
    case securityLog = "SECURITY_LOG"
    case applicationLog = "APPLICATION_LOG"
    case externalMonitor = "EXTERNAL_MONITOR"

    public var displayName: String {
        switch self {
        case .file: return "File"
        case .securityLog: return "Security Log"
        case .applicationLog: return "Application Log"
        case .externalMonitor: return "External Monitor"
        }
    }
}

/// The action to take when the audit target cannot be written.
public enum AuditOnFailure: String, Sendable, Hashable {
    case continueOperation = "CONTINUE"
    case shutdownServer = "SHUTDOWN"
    case failOperation = "FAIL_OPERATION"

    public var displayName: String {
        switch self {
        case .continueOperation: return "Continue"
        case .shutdownServer: return "Shutdown Server"
        case .failOperation: return "Fail Operation"
        }
    }
}

/// A server audit from `sys.server_audits`.
public struct ServerAuditInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let auditID: Int
    public let name: String
    public let isEnabled: Bool
    public let destination: AuditDestination
    public let filePath: String?
    public let maxFileSize: Int?
    public let maxRolloverFiles: Int?
    public let queueDelay: Int?
    public let onFailure: AuditOnFailure
    public let createDate: String?

    public init(auditID: Int, name: String, isEnabled: Bool, destination: AuditDestination, filePath: String? = nil, maxFileSize: Int? = nil, maxRolloverFiles: Int? = nil, queueDelay: Int? = nil, onFailure: AuditOnFailure = .continueOperation, createDate: String? = nil) {
        self.auditID = auditID
        self.name = name
        self.isEnabled = isEnabled
        self.destination = destination
        self.filePath = filePath
        self.maxFileSize = maxFileSize
        self.maxRolloverFiles = maxRolloverFiles
        self.queueDelay = queueDelay
        self.onFailure = onFailure
        self.createDate = createDate
    }
}

/// Options for creating or altering a server audit.
public struct ServerAuditOptions: Sendable, Hashable {
    public var queueDelay: Int?
    public var onFailure: AuditOnFailure?
    public var filePath: String?
    public var maxFileSize: Int?
    public var maxRolloverFiles: Int?
    public var reserveDiskSpace: Bool?

    public init(queueDelay: Int? = nil, onFailure: AuditOnFailure? = nil, filePath: String? = nil, maxFileSize: Int? = nil, maxRolloverFiles: Int? = nil, reserveDiskSpace: Bool? = nil) {
        self.queueDelay = queueDelay
        self.onFailure = onFailure
        self.filePath = filePath
        self.maxFileSize = maxFileSize
        self.maxRolloverFiles = maxRolloverFiles
        self.reserveDiskSpace = reserveDiskSpace
    }
}

/// A server or database audit specification from `sys.server_audit_specifications` or `sys.database_audit_specifications`.
public struct AuditSpecificationInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let name: String
    public let auditName: String
    public let isEnabled: Bool
    public let createDate: String?

    public init(name: String, auditName: String, isEnabled: Bool, createDate: String? = nil) {
        self.name = name
        self.auditName = auditName
        self.isEnabled = isEnabled
        self.createDate = createDate
    }
}

/// A detail line within an audit specification.
public struct AuditSpecificationDetail: Sendable, Hashable {
    public let actionName: String
    public let classDesc: String
    public let securableSchemaName: String?
    public let securableObjectName: String?
    public let principalName: String?

    public init(actionName: String, classDesc: String, securableSchemaName: String? = nil, securableObjectName: String? = nil, principalName: String? = nil) {
        self.actionName = actionName
        self.classDesc = classDesc
        self.securableSchemaName = securableSchemaName
        self.securableObjectName = securableObjectName
        self.principalName = principalName
    }
}

/// An entry from the audit log (fn_get_audit_file).
public struct AuditLogEntry: Sendable, Hashable, Identifiable {
    public let id: UUID
    public let eventTime: String?
    public let actionID: String
    public let actionName: String?
    public let serverPrincipalName: String?
    public let databaseName: String?
    public let schemaName: String?
    public let objectName: String?
    public let statement: String?
    public let succeeded: Bool

    public init(eventTime: String? = nil, actionID: String, actionName: String? = nil, serverPrincipalName: String? = nil, databaseName: String? = nil, schemaName: String? = nil, objectName: String? = nil, statement: String? = nil, succeeded: Bool = false) {
        self.id = UUID()
        self.eventTime = eventTime
        self.actionID = actionID
        self.actionName = actionName
        self.serverPrincipalName = serverPrincipalName
        self.databaseName = databaseName
        self.schemaName = schemaName
        self.objectName = objectName
        self.statement = statement
        self.succeeded = succeeded
    }
}

/// An audit action from `sys.dm_audit_actions`.
public struct AuditActionInfo: Sendable, Hashable {
    public let actionID: String
    public let name: String
    public let classDesc: String
    public let coveringActionName: String?

    public init(actionID: String, name: String, classDesc: String, coveringActionName: String? = nil) {
        self.actionID = actionID
        self.name = name
        self.classDesc = classDesc
        self.coveringActionName = coveringActionName
    }
}
