import Foundation

public struct ParameterMetadata: Sendable {
    public let schema: String
    public let object: String
    public let name: String
    public let ordinal: Int
    public let isReturnValue: Bool
    public let typeName: String
    public let systemTypeName: String?
    public let maxLength: Int?
    public let precision: Int?
    public let scale: Int?
    public let isOutput: Bool
    public let hasDefaultValue: Bool
    public let defaultValue: String?
    public let isReadOnly: Bool

    public init(
        schema: String,
        object: String,
        name: String,
        ordinal: Int,
        isReturnValue: Bool,
        typeName: String,
        systemTypeName: String?,
        maxLength: Int?,
        precision: Int?,
        scale: Int?,
        isOutput: Bool,
        hasDefaultValue: Bool,
        defaultValue: String?,
        isReadOnly: Bool
    ) {
        self.schema = schema
        self.object = object
        self.name = name
        self.ordinal = ordinal
        self.isReturnValue = isReturnValue
        self.typeName = typeName
        self.systemTypeName = systemTypeName
        self.maxLength = maxLength
        self.precision = precision
        self.scale = scale
        self.isOutput = isOutput
        self.hasDefaultValue = hasDefaultValue
        self.defaultValue = defaultValue
        self.isReadOnly = isReadOnly
    }
}
