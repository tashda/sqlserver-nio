import Foundation

// MARK: - Principals

public enum ServerLoginType: String, Sendable {
    case sql
    case windowsUser
    case windowsGroup
    case certificate
    case asymmetricKey
    case external

    public var tSqlTypeDesc: String {
        switch self {
        case .sql: return "SQL_LOGIN"
        case .windowsUser: return "WINDOWS_LOGIN"
        case .windowsGroup: return "WINDOWS_GROUP"
        case .certificate: return "CERTIFICATE_MAPPED_LOGIN"
        case .asymmetricKey: return "ASYMMETRIC_KEY_MAPPED_LOGIN"
        case .external: return "EXTERNAL_LOGIN"
        }
    }
}

public struct ServerLoginInfo: Sendable {
    public let name: String
    public let type: ServerLoginType
    public let isDisabled: Bool
    public let defaultDatabase: String?
    public let defaultLanguage: String?
    public let isPolicyChecked: Bool?
    public let isExpirationChecked: Bool?
}

public enum DatabasePrincipalType: String, Sendable {
    case sqlUser
    case windowsUser
    case windowsGroup
    case external
    case certificate
    case asymmetricKey
    case applicationRole
    case databaseRole
}

// MARK: - Roles

public struct ServerRoleInfo: Sendable {
    public let name: String
    public let isFixed: Bool
}

public struct DatabaseRoleInfo: Sendable {
    public let name: String
    public let isFixed: Bool
    public let owner: String?
}

// MARK: - Securables & Permissions

public enum PermissionScope: Sendable {
    case server
    case database
    case schema
    case object
    case column
}

public struct ObjectIdentifier: Sendable {
    public let database: String?
    public let schema: String
    public let name: String
    public let kind: ObjectKind
    public init(database: String? = nil, schema: String, name: String, kind: ObjectKind) {
        self.database = database
        self.schema = schema
        self.name = name
        self.kind = kind
    }
}

public enum ObjectKind: String, Sendable {
    case table = "U"
    case view = "V"
    case procedure = "P"
    case function = "FN"
    case inlineTableFunction = "IF"
    case scalarFunction = "FS"
    case unknown
}

public enum Securable: Sendable {
    case server
    case database(String?)
    case schema(String)
    case object(ObjectIdentifier)
    case column(ObjectIdentifier, [String])
}

public enum ServerPermissionName: String, Sendable, CaseIterable, Hashable {
    case administerBulkOperations = "ADMINISTER BULK OPERATIONS"
    case alterAnyAvailabilityGroup = "ALTER ANY AVAILABILITY GROUP"
    case alterAnyConnection = "ALTER ANY CONNECTION"
    case alterAnyCredential = "ALTER ANY CREDENTIAL"
    case alterAnyDatabase = "ALTER ANY DATABASE"
    case alterAnyEndpoint = "ALTER ANY ENDPOINT"
    case alterAnyEventNotification = "ALTER ANY EVENT NOTIFICATION"
    case alterAnyEventSession = "ALTER ANY EVENT SESSION"
    case alterAnyLinkedServer = "ALTER ANY LINKED SERVER"
    case alterAnyLogin = "ALTER ANY LOGIN"
    case alterAnyServerAudit = "ALTER ANY SERVER AUDIT"
    case alterAnyServerRole = "ALTER ANY SERVER ROLE"
    case alterResources = "ALTER RESOURCES"
    case alterServerState = "ALTER SERVER STATE"
    case alterSettings = "ALTER SETTINGS"
    case alterTrace = "ALTER TRACE"
    case authenticateServer = "AUTHENTICATE SERVER"
    case connectAnyDatabase = "CONNECT ANY DATABASE"
    case connectSql = "CONNECT SQL"
    case controlServer = "CONTROL SERVER"
    case createAnyDatabase = "CREATE ANY DATABASE"
    case createAvailabilityGroup = "CREATE AVAILABILITY GROUP"
    case createDdlEventNotification = "CREATE DDL EVENT NOTIFICATION"
    case createEndpoint = "CREATE ENDPOINT"
    case createServerRole = "CREATE SERVER ROLE"
    case createTraceEventNotification = "CREATE TRACE EVENT NOTIFICATION"
    case externalAccessAssembly = "EXTERNAL ACCESS ASSEMBLY"
    case impersonateAnyLogin = "IMPERSONATE ANY LOGIN"
    case selectAllUserSecurables = "SELECT ALL USER SECURABLES"
    case shutdown = "SHUTDOWN"
    case unsafeAssembly = "UNSAFE ASSEMBLY"
    case viewAnyDatabase = "VIEW ANY DATABASE"
    case viewAnyDefinition = "VIEW ANY DEFINITION"
    case viewServerState = "VIEW SERVER STATE"
}

public enum DatabasePermissionName: String, Sendable {
    case connect = "CONNECT"
    case control = "CONTROL"
    case viewDefinition = "VIEW DEFINITION"
    case alterAnySchema = "ALTER ANY SCHEMA"
    case alterAnyRole = "ALTER ANY ROLE"
    case alterAnyUser = "ALTER ANY USER"
    case createTable = "CREATE TABLE"
    case createView = "CREATE VIEW"
    case createProcedure = "CREATE PROCEDURE"
    case createFunction = "CREATE FUNCTION"
}

public enum ObjectPermissionName: String, Sendable {
    case select = "SELECT"
    case insert = "INSERT"
    case update = "UPDATE"
    case delete = "DELETE"
    case execute = "EXECUTE"
    case references = "REFERENCES"
}

// MARK: - Database User Types

/// The type of database user to create, determining authentication and login mapping.
public enum DatabaseUserType: Sendable, Hashable {
    /// Standard SQL user mapped to a server login.
    case mappedToLogin(String)
    /// Contained database user with a password (requires database containment).
    case withPassword(String)
    /// User with no login mapping (for application roles, schema ownership, etc.).
    case withoutLogin
    /// Windows/AD user mapped to a Windows login.
    case windowsUser(String)
    /// User mapped to a database certificate.
    case mappedToCertificate(String)
    /// User mapped to a database asymmetric key.
    case mappedToAsymmetricKey(String)
}

/// The authentication type of an existing database user, as reported by SQL Server.
public enum DatabaseUserAuthenticationType: String, Sendable {
    case instance = "INSTANCE"
    case database = "DATABASE"
    case windows = "WINDOWS"
    case external = "EXTERNAL"
    case none = "NONE"
}

// MARK: - Catalog Info Types

/// Information about a database certificate from `sys.certificates`.
public struct CertificateInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let name: String
    public let subject: String?
    public let expiryDate: String?

    public init(name: String, subject: String? = nil, expiryDate: String? = nil) {
        self.name = name
        self.subject = subject
        self.expiryDate = expiryDate
    }
}

/// Information about a database asymmetric key from `sys.asymmetric_keys`.
public struct AsymmetricKeyInfo: Sendable, Hashable, Identifiable {
    public var id: String { name }
    public let name: String
    public let algorithm: String?

    public init(name: String, algorithm: String? = nil) {
        self.name = name
        self.algorithm = algorithm
    }
}

/// Information about a server language from `sys.syslanguages`.
public struct LanguageInfo: Sendable, Hashable {
    public let name: String
    public let alias: String?
    public let lcid: Int

    public init(name: String, alias: String? = nil, lcid: Int) {
        self.name = name
        self.alias = alias
        self.lcid = lcid
    }
}

// MARK: - Directory Resolution

public struct PrincipalResolution: Sendable {
    public let input: String
    public let exists: Bool
    public let principalType: ServerLoginType?
    public let sid: Data?
}

/// Represents a login's user mapping to a specific database.
public struct LoginDatabaseMapping: Sendable {
    public let databaseName: String
    public let userName: String
    public let defaultSchema: String?

    public init(databaseName: String, userName: String, defaultSchema: String? = nil) {
        self.databaseName = databaseName
        self.userName = userName
        self.defaultSchema = defaultSchema
    }
}

// MARK: - Effective Permissions

/// A single effective permission entry returned by `fn_my_permissions()`.
public struct EffectivePermissionInfo: Sendable, Hashable {
    public let entityName: String?
    public let subentityName: String?
    public let permissionName: String

    public init(entityName: String? = nil, subentityName: String? = nil, permissionName: String) {
        self.entityName = entityName
        self.subentityName = subentityName
        self.permissionName = permissionName
    }
}

/// Represents a database role and whether a user is a member.
public struct DatabaseUserRoleMembership: Sendable {
    public let roleName: String
    public let isMember: Bool

    public init(roleName: String, isMember: Bool) {
        self.roleName = roleName
        self.isMember = isMember
    }
}

