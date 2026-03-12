import Foundation

/// Enhanced job history information with proper date formatting
public struct SQLServerAgentJobHistoryDetail: Sendable {
    public let instanceId: Int
    public let jobName: String
    public let stepId: Int
    public let stepName: String?
    public let runStatus: Int
    public let runStatusDescription: String
    public let message: String
    public let runDateTime: Date?
    public let runDurationSeconds: Int?

    public init(
        instanceId: Int,
        jobName: String,
        stepId: Int,
        stepName: String? = nil,
        runStatus: Int,
        runStatusDescription: String,
        message: String,
        runDateTime: Date? = nil,
        runDurationSeconds: Int? = nil
    ) {
        self.instanceId = instanceId
        self.jobName = jobName
        self.stepId = stepId
        self.stepName = stepName
        self.runStatus = runStatus
        self.runStatusDescription = runStatusDescription
        self.message = message
        self.runDateTime = runDateTime
        self.runDurationSeconds = runDurationSeconds
    }
}
