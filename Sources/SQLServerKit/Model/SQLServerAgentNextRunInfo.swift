import Foundation

public struct SQLServerAgentNextRunInfo: Sendable {
    public let jobName: String
    public let nextRunDate: Date?

    public init(jobName: String, nextRunDate: Date? = nil) {
        self.jobName = jobName
        self.nextRunDate = nextRunDate
    }
}
