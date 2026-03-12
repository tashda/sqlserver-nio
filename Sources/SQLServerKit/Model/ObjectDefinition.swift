import Foundation

public struct ObjectDefinition: Sendable {
    public enum ObjectType: String, Sendable {
        case view
        case table
        case procedure
        case scalarFunction
        case tableFunction
        case trigger
        case other

        public static func from(typeDesc: String, objectType: String? = nil) -> ObjectType {
            let normalized = typeDesc.uppercased()
            if normalized.contains("TABLE") && !normalized.contains("FUNCTION") {
                return .table
            }
            if normalized.contains("VIEW") {
                return .view
            }
            if normalized.contains("TRIGGER") {
                return .trigger
            }
            if normalized.contains("FUNCTION") {
                return normalized.contains("TABLE") ? .tableFunction : .scalarFunction
            }
            if normalized.contains("PROCEDURE") {
                return .procedure
            }
            return .other
        }
    }

    public let schema: String
    public let name: String
    public let type: ObjectType
    public let definition: String?
    public let isSystemObject: Bool
    public let createDate: Date?
    public let modifyDate: Date?

    public init(
        schema: String,
        name: String,
        type: ObjectType,
        definition: String? = nil,
        isSystemObject: Bool,
        createDate: Date? = nil,
        modifyDate: Date? = nil
    ) {
        self.schema = schema
        self.name = name
        self.type = type
        self.definition = definition
        self.isSystemObject = isSystemObject
        self.createDate = createDate
        self.modifyDate = modifyDate
    }
}
