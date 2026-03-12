import Foundation

public struct SQLServerAgentOperatorInfo: Sendable {
    public let name: String
    public let emailAddress: String?
    public let enabled: Bool

    public init(name: String, emailAddress: String? = nil, enabled: Bool) {
        self.name = name
        self.emailAddress = emailAddress
        self.enabled = enabled
    }
}
