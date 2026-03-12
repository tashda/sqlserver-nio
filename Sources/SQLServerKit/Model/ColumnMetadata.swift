import Foundation

public struct ColumnMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let typeName: String
    public let systemTypeName: String?
    public let maxLength: Int?
    public let precision: Int?
    public let scale: Int?
    public let collationName: String?
    public let isNullable: Bool
    public let isIdentity: Bool
    public let isComputed: Bool
    public let hasDefaultValue: Bool
    public let defaultDefinition: String?
    public let computedDefinition: String?
    public let ordinalPosition: Int
    /// Identity seed value (if identity column)
    public let identitySeed: Int?
    /// Identity increment value (if identity column)
    public let identityIncrement: Int?
    /// Check constraint definition (if check constraint exists)
    public let checkDefinition: String?
    /// Optional MS_Description extended property for this column
    public let comment: String?

    public init(
        schema: String,
        table: String,
        name: String,
        typeName: String,
        systemTypeName: String?,
        maxLength: Int?,
        precision: Int?,
        scale: Int?,
        collationName: String?,
        isNullable: Bool,
        isIdentity: Bool,
        isComputed: Bool,
        hasDefaultValue: Bool,
        defaultDefinition: String?,
        computedDefinition: String?,
        ordinalPosition: Int,
        identitySeed: Int?,
        identityIncrement: Int?,
        checkDefinition: String?,
        comment: String?
    ) {
        self.schema = schema
        self.table = table
        self.name = name
        self.typeName = typeName
        self.systemTypeName = systemTypeName
        self.maxLength = maxLength
        self.precision = precision
        self.scale = scale
        self.collationName = collationName
        self.isNullable = isNullable
        self.isIdentity = isIdentity
        self.isComputed = isComputed
        self.hasDefaultValue = hasDefaultValue
        self.defaultDefinition = defaultDefinition
        self.computedDefinition = computedDefinition
        self.ordinalPosition = ordinalPosition
        self.identitySeed = identitySeed
        self.identityIncrement = identityIncrement
        self.checkDefinition = checkDefinition
        self.comment = comment
    }
}
