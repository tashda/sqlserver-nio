import Foundation

/// Information about the currently executing step of a running SQL Agent job.
public struct SQLServerAgentActiveStep: Sendable {
    public let jobName: String
    public let lastExecutedStepId: Int
    public let stepName: String?
    public let startExecutionDate: Date?

    public init(jobName: String, lastExecutedStepId: Int, stepName: String? = nil, startExecutionDate: Date? = nil) {
        self.jobName = jobName
        self.lastExecutedStepId = lastExecutedStepId
        self.stepName = stepName
        self.startExecutionDate = startExecutionDate
    }
}
