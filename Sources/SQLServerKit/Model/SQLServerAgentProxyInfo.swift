import Foundation

public struct SQLServerAgentProxyInfo: Sendable {
    public let name: String
    public let credentialName: String?
    public let enabled: Bool

    public init(name: String, credentialName: String? = nil, enabled: Bool) {
        self.name = name
        self.credentialName = credentialName
        self.enabled = enabled
    }
}
