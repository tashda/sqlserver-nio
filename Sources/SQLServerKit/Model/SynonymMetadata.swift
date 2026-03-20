import Foundation

public struct SynonymMetadata: Sendable {
    public let schema: String
    public let name: String
    public let baseObjectName: String
    public let comment: String?

    public init(schema: String, name: String, baseObjectName: String, comment: String? = nil) {
        self.schema = schema
        self.name = name
        self.baseObjectName = baseObjectName
        self.comment = comment
    }
}
