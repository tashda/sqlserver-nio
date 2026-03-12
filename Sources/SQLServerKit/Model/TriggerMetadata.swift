import Foundation

public struct TriggerMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let isInsteadOf: Bool
    public let isDisabled: Bool
    public let definition: String?
    /// Optional MS_Description extended property for this trigger
    public let comment: String?

    public init(
        schema: String,
        table: String,
        name: String,
        isInsteadOf: Bool,
        isDisabled: Bool,
        definition: String? = nil,
        comment: String? = nil
    ) {
        self.schema = schema
        self.table = table
        self.name = name
        self.isInsteadOf = isInsteadOf
        self.isDisabled = isDisabled
        self.definition = definition
        self.comment = comment
    }
}
