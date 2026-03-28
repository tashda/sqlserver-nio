import Foundation

public struct SQLServerAgentAlertInfo: Sendable {
    public let name: String
    public let severity: Int?
    public let messageId: Int?
    public let databaseName: String?
    public let eventDescriptionKeyword: String?
    public let enabled: Bool

    public init(name: String, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, eventDescriptionKeyword: String? = nil, enabled: Bool) {
        self.name = name
        self.severity = severity
        self.messageId = messageId
        self.databaseName = databaseName
        self.eventDescriptionKeyword = eventDescriptionKeyword
        self.enabled = enabled
    }
}
