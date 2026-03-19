import Foundation

public struct DatabaseMetadata: Sendable {
    public let name: String
    /// Database state descriptor (e.g. "ONLINE", "OFFLINE", "RESTORING").
    /// `nil` when the state was not fetched.
    public let stateDescription: String?
    /// Whether the current login has access to this database.
    /// `nil` when access was not checked (e.g. older code paths).
    public let hasAccess: Bool?

    public init(name: String, stateDescription: String? = nil, hasAccess: Bool? = nil) {
        self.name = name
        self.stateDescription = stateDescription
        self.hasAccess = hasAccess
    }

    /// Whether the database is currently online.
    public var isOnline: Bool {
        guard let state = stateDescription else { return true }
        return state.uppercased() == "ONLINE"
    }

    /// Whether the current login can access this database.
    /// Defaults to `true` when access was not checked.
    public var isAccessible: Bool {
        hasAccess ?? true
    }
}
