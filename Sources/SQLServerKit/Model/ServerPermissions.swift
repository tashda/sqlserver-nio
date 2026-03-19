import Foundation

/// Represents the current login's server-level permissions relevant to Echo features.
public struct ServerPermissions: Sendable {
    /// Whether the login has VIEW SERVER STATE (required for Activity Monitor DMVs).
    public let hasViewServerState: Bool
    /// Whether the login can access the `master` database (required for sys.master_files).
    public let hasMasterAccess: Bool
    /// Whether the login can access the `msdb` database (required for backup history).
    public let hasMsdbAccess: Bool

    public init(hasViewServerState: Bool, hasMasterAccess: Bool, hasMsdbAccess: Bool) {
        self.hasViewServerState = hasViewServerState
        self.hasMasterAccess = hasMasterAccess
        self.hasMsdbAccess = hasMsdbAccess
    }
}
