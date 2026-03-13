import SQLServerTDS

// MARK: - Security Types

public struct UserOptions: Sendable {
    public let defaultSchema: String?
    public let defaultLanguage: String?
    public let allowEncryptedValueModifications: Bool
    
    public init(
        defaultSchema: String? = nil,
        defaultLanguage: String? = nil,
        allowEncryptedValueModifications: Bool = false
    ) {
        self.defaultSchema = defaultSchema
        self.defaultLanguage = defaultLanguage
        self.allowEncryptedValueModifications = allowEncryptedValueModifications
    }
}

public struct RoleOptions: Sendable {
    public let owner: String?
    
    public init(owner: String? = nil) {
        self.owner = owner
    }
}

public enum Permission: String, Sendable {
    case select = "SELECT"
    case insert = "INSERT"
    case update = "UPDATE"
    case delete = "DELETE"
    case execute = "EXECUTE"
    case references = "REFERENCES"
    case alter = "ALTER"
    case control = "CONTROL"
    case takeOwnership = "TAKE OWNERSHIP"
    case viewDefinition = "VIEW DEFINITION"
    case viewChangeTracking = "VIEW CHANGE TRACKING"
    case createTable = "CREATE TABLE"
    case createView = "CREATE VIEW"
    case createProcedure = "CREATE PROCEDURE"
    case createFunction = "CREATE FUNCTION"
    case createSchema = "CREATE SCHEMA"
    case createRole = "CREATE ROLE"
    case createUser = "CREATE USER"
    case alterAnySchema = "ALTER ANY SCHEMA"
    case alterAnyRole = "ALTER ANY ROLE"
    case alterAnyUser = "ALTER ANY USER"
    case backup = "BACKUP DATABASE"
    case restore = "RESTORE"
    case bulkAdmin = "ADMINISTER BULK OPERATIONS"
    case dbOwner = "db_owner"
    case dbDataReader = "db_datareader"
    case dbDataWriter = "db_datawriter"
    case dbDdlAdmin = "db_ddladmin"
    case dbSecurityAdmin = "db_securityadmin"
    case dbAccessAdmin = "db_accessadmin"
    case dbBackupOperator = "db_backupoperator"
    case dbDenyDataReader = "db_denydatareader"
    case dbDenyDataWriter = "db_denydatawriter"
}

public struct UserInfo: Sendable {
    public let name: String
    public let principalId: Int
    public let type: String
    public let defaultSchema: String?
    public let createDate: String?
    public let modifyDate: String?
    public let isDisabled: Bool
    
    public init(
        name: String,
        principalId: Int,
        type: String,
        defaultSchema: String? = nil,
        createDate: String? = nil,
        modifyDate: String? = nil,
        isDisabled: Bool = false
    ) {
        self.name = name
        self.principalId = principalId
        self.type = type
        self.defaultSchema = defaultSchema
        self.createDate = createDate
        self.modifyDate = modifyDate
        self.isDisabled = isDisabled
    }
}

public struct RoleInfo: Sendable {
    public let name: String
    public let principalId: Int
    public let type: String
    public let ownerPrincipalId: Int?
    public let isFixedRole: Bool
    public let createDate: String?
    public let modifyDate: String?
    
    public init(
        name: String,
        principalId: Int,
        type: String,
        ownerPrincipalId: Int? = nil,
        isFixedRole: Bool = false,
        createDate: String? = nil,
        modifyDate: String? = nil
    ) {
        self.name = name
        self.principalId = principalId
        self.type = type
        self.ownerPrincipalId = ownerPrincipalId
        self.isFixedRole = isFixedRole
        self.createDate = createDate
        self.modifyDate = modifyDate
    }
}

public struct PermissionInfo: Sendable {
    public let permission: String
    public let state: String // GRANT, DENY, REVOKE
    public let objectName: String?
    public let principalName: String
    public let grantor: String?
    
    public init(
        permission: String,
        state: String,
        objectName: String? = nil,
        principalName: String,
        grantor: String? = nil
    ) {
        self.permission = permission
        self.state = state
        self.objectName = objectName
        self.principalName = principalName
        self.grantor = grantor
    }
}

public struct DetailedPermissionInfo: Sendable {
    public let permission: String
    public let state: String
    public let classDesc: String
    public let schemaName: String?
    public let objectName: String?
    public let columnName: String?
    public let principalName: String
    public let grantor: String?
    
    public init(
        permission: String,
        state: String,
        classDesc: String,
        schemaName: String? = nil,
        objectName: String? = nil,
        columnName: String? = nil,
        principalName: String,
        grantor: String? = nil
    ) {
        self.permission = permission
        self.state = state
        self.classDesc = classDesc
        self.schemaName = schemaName
        self.objectName = objectName
        self.columnName = columnName
        self.principalName = principalName
        self.grantor = grantor
    }
}

public struct ApplicationRoleInfo: Sendable {
    public let name: String
    public let defaultSchema: String?
    public let createDate: String?
    public let modifyDate: String?
    
    public init(
        name: String,
        defaultSchema: String? = nil,
        createDate: String? = nil,
        modifyDate: String? = nil
    ) {
        self.name = name
        self.defaultSchema = defaultSchema
        self.createDate = createDate
        self.modifyDate = modifyDate
    }
}

public struct SchemaInfo: Sendable {
    public let name: String
    public let owner: String?
    
    public init(name: String, owner: String? = nil) {
        self.name = name
        self.owner = owner
    }
}
