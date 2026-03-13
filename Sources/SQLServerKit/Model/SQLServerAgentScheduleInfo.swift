import Foundation

public struct SQLServerAgentScheduleInfo: Sendable {
    public let name: String
    public let enabled: Bool
    public let freqType: Int

    public init(name: String, enabled: Bool, freqType: Int) {
        self.name = name
        self.enabled = enabled
        self.freqType = freqType
    }
}
