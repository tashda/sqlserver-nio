import Foundation

/// Represents a dependency relationship between two database objects.
/// Captures both the referencing (dependent) and referenced objects.
public struct SQLServerObjectDependency: Sendable {
    public let referencingName: String
    public let referencingType: String
    public let referencedName: String
    public let referencedType: String

    public init(
        referencingName: String,
        referencingType: String,
        referencedName: String,
        referencedType: String
    ) {
        self.referencingName = referencingName
        self.referencingType = referencingType
        self.referencedName = referencedName
        self.referencedType = referencedType
    }
}
