import Foundation

public struct SQLServerAgentJobHistoryEntry: Sendable {
    public let runStatus: Int
    public let stepId: Int
    public let message: String
    public let runDate: Int?
    public let runTime: Int?

    public init(runStatus: Int, stepId: Int, message: String, runDate: Int? = nil, runTime: Int? = nil) {
        self.runStatus = runStatus
        self.stepId = stepId
        self.message = message
        self.runDate = runDate
        self.runTime = runTime
    }
}
