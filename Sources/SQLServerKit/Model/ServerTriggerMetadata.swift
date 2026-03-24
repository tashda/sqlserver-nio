import Foundation

/// Metadata for a server-level trigger (DDL or logon).
public struct ServerTriggerMetadata: Sendable, Equatable, Identifiable {
    public var id: String { name }

    /// Trigger name.
    public let name: String
    /// Whether the trigger is currently disabled.
    public let isDisabled: Bool
    /// Trigger type description (e.g. "SERVER", "LOGON").
    public let typeDescription: String
    /// Creation date as ISO string.
    public let createDate: String?
    /// Last modification date as ISO string.
    public let modifyDate: String?
    /// T-SQL definition from sys.sql_modules (nil if encrypted).
    public let definition: String?
    /// DDL events that fire this trigger (e.g. ["CREATE_TABLE", "ALTER_TABLE"]).
    public let events: [String]

    public init(
        name: String,
        isDisabled: Bool,
        typeDescription: String,
        createDate: String? = nil,
        modifyDate: String? = nil,
        definition: String? = nil,
        events: [String] = []
    ) {
        self.name = name
        self.isDisabled = isDisabled
        self.typeDescription = typeDescription
        self.createDate = createDate
        self.modifyDate = modifyDate
        self.definition = definition
        self.events = events
    }

    /// True if this is a logon trigger.
    public var isLogonTrigger: Bool {
        typeDescription.uppercased().contains("LOGON")
    }
}

/// Metadata for a database-level DDL trigger.
public struct DatabaseDDLTriggerMetadata: Sendable, Equatable, Identifiable {
    public var id: String { name }

    /// Trigger name.
    public let name: String
    /// Whether the trigger is currently disabled.
    public let isDisabled: Bool
    /// Creation date as ISO string.
    public let createDate: String?
    /// Last modification date as ISO string.
    public let modifyDate: String?
    /// T-SQL definition from sys.sql_modules (nil if encrypted).
    public let definition: String?
    /// DDL events that fire this trigger (e.g. ["CREATE_TABLE", "DROP_TABLE"]).
    public let events: [String]

    public init(
        name: String,
        isDisabled: Bool,
        createDate: String? = nil,
        modifyDate: String? = nil,
        definition: String? = nil,
        events: [String] = []
    ) {
        self.name = name
        self.isDisabled = isDisabled
        self.createDate = createDate
        self.modifyDate = modifyDate
        self.definition = definition
        self.events = events
    }
}
