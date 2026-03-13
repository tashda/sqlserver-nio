import Foundation

public struct MetadataSearchScope: OptionSet, Sendable {
    public let rawValue: Int

    public init(rawValue: Int) {
        self.rawValue = rawValue
    }

    public static let objectNames = MetadataSearchScope(rawValue: 1 << 0)
    public static let definitions = MetadataSearchScope(rawValue: 1 << 1)
    public static let columns = MetadataSearchScope(rawValue: 1 << 2)
    public static let indexes = MetadataSearchScope(rawValue: 1 << 3)
    public static let constraints = MetadataSearchScope(rawValue: 1 << 4)

    public static let `default`: MetadataSearchScope = [.objectNames, .definitions]
    public static let all: MetadataSearchScope = [.objectNames, .definitions, .columns, .indexes, .constraints]
}

public struct MetadataSearchHit: Sendable {
    public enum MatchKind: String, Sendable {
        case name
        case definition
        case column
        case index
        case constraint
    }

    public let schema: String
    public let name: String
    public let type: ObjectDefinition.ObjectType
    public let matchKind: MatchKind
    public let detail: String?

    public init(schema: String, name: String, type: ObjectDefinition.ObjectType, matchKind: MatchKind, detail: String? = nil) {
        self.schema = schema
        self.name = name
        self.type = type
        self.matchKind = matchKind
        self.detail = detail
    }
}
