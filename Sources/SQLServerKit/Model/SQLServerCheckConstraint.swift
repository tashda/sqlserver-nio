/// A check constraint on a table.
public struct SQLServerCheckConstraint: Sendable {
    /// The constraint name.
    public let name: String
    /// The check expression (e.g. `([amount]>(0))`).
    public let definition: String

    public init(name: String, definition: String) {
        self.name = name
        self.definition = definition
    }
}
