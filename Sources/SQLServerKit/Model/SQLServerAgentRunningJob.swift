import Foundation

public struct SQLServerAgentRunningJob: Sendable {
    public let name: String
    public let sessionId: Int?
    public let startExecutionDate: Date?

    public init(name: String, sessionId: Int? = nil, startExecutionDate: Date? = nil) {
        self.name = name
        self.sessionId = sessionId
        self.startExecutionDate = startExecutionDate
    }
}
