import Foundation

/// Enhanced job schedule information
public struct SQLServerAgentJobScheduleDetail: Sendable {
    public let scheduleId: String
    public let name: String
    public let enabled: Bool
    public let freqType: Int
    public let freqInterval: Int?
    public let freqSubdayType: Int?
    public let freqSubdayInterval: Int?
    public let activeStartDate: Int?
    public let activeStartTime: Int?
    public let activeEndDate: Int?
    public let activeEndTime: Int?
    public let nextRunDate: Date?
    /// Weeks or months between runs for weekly and monthly schedules.
    public let freqRecurrenceFactor: Int?
    /// For monthly-relative schedules: first, second, third, fourth or last.
    public let freqRelativeInterval: Int?

    public init(
        scheduleId: String,
        name: String,
        enabled: Bool,
        freqType: Int,
        freqInterval: Int? = nil,
        freqSubdayType: Int? = nil,
        freqSubdayInterval: Int? = nil,
        activeStartDate: Int? = nil,
        activeStartTime: Int? = nil,
        activeEndDate: Int? = nil,
        activeEndTime: Int? = nil,
        nextRunDate: Date? = nil,
        freqRecurrenceFactor: Int? = nil,
        freqRelativeInterval: Int? = nil
    ) {
        self.scheduleId = scheduleId
        self.name = name
        self.enabled = enabled
        self.freqType = freqType
        self.freqInterval = freqInterval
        self.freqSubdayType = freqSubdayType
        self.freqSubdayInterval = freqSubdayInterval
        self.activeStartDate = activeStartDate
        self.activeStartTime = activeStartTime
        self.activeEndDate = activeEndDate
        self.activeEndTime = activeEndTime
        self.nextRunDate = nextRunDate
        self.freqRecurrenceFactor = freqRecurrenceFactor
        self.freqRelativeInterval = freqRelativeInterval
    }
}
