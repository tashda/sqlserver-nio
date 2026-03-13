import Foundation

/// Comprehensive job information including all details needed for job management
public struct SQLServerAgentJobDetail: Sendable {
    public let jobId: String
    public let name: String
    public let description: String?
    public let enabled: Bool
    public let ownerLoginName: String?
    public let categoryName: String?
    public let startStepId: Int?
    public let lastRunOutcome: String?
    public let lastRunDate: Date?
    public let nextRunDate: Date?
    public let hasSchedule: Bool

    public init(
        jobId: String,
        name: String,
        description: String? = nil,
        enabled: Bool,
        ownerLoginName: String? = nil,
        categoryName: String? = nil,
        startStepId: Int? = nil,
        lastRunOutcome: String? = nil,
        lastRunDate: Date? = nil,
        nextRunDate: Date? = nil,
        hasSchedule: Bool = false
    ) {
        self.jobId = jobId
        self.name = name
        self.description = description
        self.enabled = enabled
        self.ownerLoginName = ownerLoginName
        self.categoryName = categoryName
        self.startStepId = startStepId
        self.lastRunOutcome = lastRunOutcome
        self.lastRunDate = lastRunDate
        self.nextRunDate = nextRunDate
        self.hasSchedule = hasSchedule
    }
}
