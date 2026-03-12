import Foundation

public struct DatabaseMetadata: Sendable {
    public let name: String
    /// Database state descriptor (e.g. "ONLINE", "OFFLINE", "RESTORING").
    /// `nil` when the state was not fetched.
    public let stateDescription: String?

    public init(name: String, stateDescription: String? = nil) {
        self.name = name
        self.stateDescription = stateDescription
    }

    /// Whether the database is currently online.
    public var isOnline: Bool {
        guard let state = stateDescription else { return true }
        return state.uppercased() == "ONLINE"
    }
}
