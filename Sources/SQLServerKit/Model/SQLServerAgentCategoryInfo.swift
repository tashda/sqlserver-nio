import Foundation

public struct SQLServerAgentCategoryInfo: Sendable {
    public let name: String
    public let classId: Int

    public init(name: String, classId: Int) {
        self.name = name
        self.classId = classId
    }
}
