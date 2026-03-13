import Foundation

public struct SQLServerMetadataObjectIdentifier: Sendable {
    public enum Kind: String, Sendable {
        case view
        case procedure
        case function
        case trigger
        case table
        case other
    }

    public let database: String?
    public let schema: String
    public let name: String
    public let kind: Kind

    public init(database: String? = nil, schema: String, name: String, kind: Kind) {
        self.database = database
        self.schema = schema
        self.name = name
        self.kind = kind
    }
}
