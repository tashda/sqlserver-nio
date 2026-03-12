import Foundation

public struct DependencyMetadata: Sendable {
    public let referencingSchema: String
    public let referencingObject: String
    public let referencingType: String
    public let isSchemaBound: Bool

    public init(referencingSchema: String, referencingObject: String, referencingType: String, isSchemaBound: Bool) {
        self.referencingSchema = referencingSchema
        self.referencingObject = referencingObject
        self.referencingType = referencingType
        self.isSchemaBound = isSchemaBound
    }
}
