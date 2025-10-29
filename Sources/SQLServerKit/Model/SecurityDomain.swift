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

public enum ServerPermissionName: String, Sendable {
    case viewServerState = "VIEW SERVER STATE"
    case alterAnyLogin = "ALTER ANY LOGIN"
    case controlServer = "CONTROL SERVER"
    case alterAnyCredential = "ALTER ANY CREDENTIAL"
    case createAnyDatabase = "CREATE ANY DATABASE"
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

// MARK: - Directory Resolution

public struct PrincipalResolution: Sendable {
    public let input: String
    public let exists: Bool
    public let principalType: ServerLoginType?
    public let sid: Data?
}

