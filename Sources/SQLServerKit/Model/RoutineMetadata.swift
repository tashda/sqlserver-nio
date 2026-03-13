import Foundation

public struct RoutineMetadata: Sendable {
    public enum RoutineType: String, Sendable {
        case procedure = "PROCEDURE"
        case scalarFunction = "SCALAR_FUNCTION"
        case tableFunction = "TABLE_FUNCTION"
    }

    public let schema: String
    public let name: String
    public let type: RoutineType
    public let definition: String?
    public let isSystemObject: Bool
    /// Optional MS_Description extended property for this routine (procedure/function)
    public let comment: String?

    public init(
        schema: String,
        name: String,
        type: RoutineType,
        definition: String? = nil,
        isSystemObject: Bool,
        comment: String? = nil
    ) {
        self.schema = schema
        self.name = name
        self.type = type
        self.definition = definition
        self.isSystemObject = isSystemObject
        self.comment = comment
    }
}
