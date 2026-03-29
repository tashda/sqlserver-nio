/// Metadata for a SQL Server user-defined type.
public struct UserTypeMetadata: Sendable {
    public let name: String
    public let schema: String
    public let kind: Kind
    public let baseType: String?
    public let maxLength: Int?
    public let precision: UInt8?
    public let scale: UInt8?
    public let isNullable: Bool
    public let isAssemblyType: Bool
    public let comment: String?

    public enum Kind: String, Sendable {
        case tableType = "TABLE_TYPE"
        case alias = "ALIAS"
        case clr = "CLR"
    }

    public init(
        name: String,
        schema: String,
        kind: Kind,
        baseType: String? = nil,
        maxLength: Int? = nil,
        precision: UInt8? = nil,
        scale: UInt8? = nil,
        isNullable: Bool = true,
        isAssemblyType: Bool = false,
        comment: String? = nil
    ) {
        self.name = name
        self.schema = schema
        self.kind = kind
        self.baseType = baseType
        self.maxLength = maxLength
        self.precision = precision
        self.scale = scale
        self.isNullable = isNullable
        self.isAssemblyType = isAssemblyType
        self.comment = comment
    }
}
