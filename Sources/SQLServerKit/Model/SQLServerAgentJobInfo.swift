import Foundation

public struct SQLServerAgentJobInfo: Sendable {
    public let name: String
    public let enabled: Bool
    public let lastRunOutcome: String?

    public init(name: String, enabled: Bool, lastRunOutcome: String? = nil) {
        self.name = name
        self.enabled = enabled
        self.lastRunOutcome = lastRunOutcome
    }
}
