import Foundation

public struct SQLServerAgentAlertInfo: Sendable {
    public let name: String
    public let severity: Int?
    public let messageId: Int?
    public let enabled: Bool

    public init(name: String, severity: Int? = nil, messageId: Int? = nil, enabled: Bool) {
        self.name = name
        self.severity = severity
        self.messageId = messageId
        self.enabled = enabled
    }
}
