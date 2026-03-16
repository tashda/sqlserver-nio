import Foundation

public struct SQLServerAgentErrorLog: Sendable, Hashable {
    public let archiveNumber: Int
    public let date: String
    public let size: String?

    public init(archiveNumber: Int, date: String, size: String?) {
        self.archiveNumber = archiveNumber
        self.date = date
        self.size = size
    }
}
