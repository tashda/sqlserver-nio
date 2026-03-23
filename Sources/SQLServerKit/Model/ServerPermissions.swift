import Foundation

/// Represents the current login's server-level permissions relevant to Echo features.
///
/// Fetched once at connection time via ``SQLServerMetadataNamespace/checkServerPermissions()``
/// and cached on the connection session. All SQL functions used (`IS_SRVROLEMEMBER`,
/// `HAS_PERMS_BY_NAME`, `HAS_DBACCESS`) work on SQL Server 2016+.
public struct ServerPermissions: Sendable, Equatable {

    // MARK: - Server Roles

    /// Member of the sysadmin fixed server role (full control).
    public let isSysadmin: Bool
    /// Member of the serveradmin role (server configuration).
    public let isServerAdmin: Bool
    /// Member of the securityadmin role (login management).
    public let isSecurityAdmin: Bool
    /// Member of the dbcreator role (create/alter databases).
    public let isDBCreator: Bool

    // MARK: - Server-Level Permissions

    /// `VIEW SERVER STATE` — required for Activity Monitor DMVs.
    public let hasViewServerState: Bool
    /// `ALTER ANY LOGIN` — required for login management.
    public let hasAlterAnyLogin: Bool
    /// `ALTER ANY DATABASE` — required for database-level admin.
    public let hasAlterAnyDatabase: Bool
    /// `ALTER ANY CREDENTIAL` — required for proxy/credential management.
    public let hasAlterAnyCredential: Bool

    // MARK: - Database Access

    /// Whether the login can access the `master` database.
    public let hasMasterAccess: Bool
    /// Whether the login can access the `msdb` database.
    public let hasMsdbAccess: Bool

    // MARK: - msdb Roles

    /// Roles the current principal holds in msdb (e.g. SQLAgentUserRole, DatabaseMailUserRole, db_owner).
    public let msdbRoles: Set<String>

    // MARK: - Init

    public init(
        isSysadmin: Bool,
        isServerAdmin: Bool,
        isSecurityAdmin: Bool,
        isDBCreator: Bool,
        hasViewServerState: Bool,
        hasAlterAnyLogin: Bool,
        hasAlterAnyDatabase: Bool,
        hasAlterAnyCredential: Bool,
        hasMasterAccess: Bool,
        hasMsdbAccess: Bool,
        msdbRoles: Set<String>
    ) {
        self.isSysadmin = isSysadmin
        self.isServerAdmin = isServerAdmin
        self.isSecurityAdmin = isSecurityAdmin
        self.isDBCreator = isDBCreator
        self.hasViewServerState = hasViewServerState
        self.hasAlterAnyLogin = hasAlterAnyLogin
        self.hasAlterAnyDatabase = hasAlterAnyDatabase
        self.hasAlterAnyCredential = hasAlterAnyCredential
        self.hasMasterAccess = hasMasterAccess
        self.hasMsdbAccess = hasMsdbAccess
        self.msdbRoles = msdbRoles
    }

    // MARK: - Convenience

    /// Can manage SQL Agent jobs (create, edit, delete, start, stop).
    public var canManageAgent: Bool {
        isSysadmin || msdbRoles.contains("SQLAgentOperatorRole") || msdbRoles.contains("db_owner")
    }

    /// Can view SQL Agent job definitions and history.
    public var canViewAgentJobs: Bool {
        isSysadmin || !msdbRoles.isDisjoint(with: [
            "SQLAgentUserRole", "SQLAgentReaderRole", "SQLAgentOperatorRole", "db_owner"
        ])
    }

    /// Can configure Database Mail (create/edit profiles, accounts, system parameters).
    public var canConfigureDatabaseMail: Bool {
        isSysadmin
    }

    /// Can use Database Mail (send email, view queue via an assigned profile).
    public var canUseDatabaseMail: Bool {
        isSysadmin || msdbRoles.contains("DatabaseMailUserRole") || msdbRoles.contains("db_owner")
    }

    /// Can create, alter, or drop logins.
    public var canManageLogins: Bool {
        isSysadmin || isSecurityAdmin || hasAlterAnyLogin
    }

    /// Can create new databases.
    public var canCreateDatabases: Bool {
        isSysadmin || isDBCreator || hasAlterAnyDatabase
    }

    /// Can view server state DMVs (activity monitor, wait stats, etc.).
    public var canViewServerState: Bool {
        isSysadmin || hasViewServerState
    }

    /// Can perform server-level backup and restore operations.
    public var canBackupRestore: Bool {
        isSysadmin || isServerAdmin
    }

    /// Can manage linked servers.
    public var canManageLinkedServers: Bool {
        isSysadmin || isServerAdmin
    }

    /// Can manage Agent proxies and credentials.
    public var canManageProxies: Bool {
        isSysadmin || (hasAlterAnyCredential && msdbRoles.contains("SQLAgentOperatorRole"))
    }
}
