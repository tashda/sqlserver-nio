import NIO
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
}

public struct RoleInfo: Sendable {
    public let name: String
    public let principalId: Int
    public let type: String
    public let ownerPrincipalId: Int?
    public let isFixedRole: Bool
    public let createDate: String?
    public let modifyDate: String?
}

public struct PermissionInfo: Sendable {
    public let permission: String
    public let state: String // GRANT, DENY, REVOKE
    public let objectName: String?
    public let principalName: String
    public let grantor: String?
}

// MARK: - SQLServerSecurityClient

public final class SQLServerSecurityClient {
    private let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
    // MARK: - User Management
    
    public func createUser(
        name: String,
        login: String? = nil,
        options: UserOptions = UserOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createUser(name: name, login: login, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createUser(
        name: String,
        login: String? = nil,
        options: UserOptions = UserOptions()
    ) async throws {
        let escapedUserName = Self.escapeIdentifier(name)
        
        var sql = "CREATE USER \(escapedUserName)"
        
        if let login = login {
            let escapedLogin = Self.escapeIdentifier(login)
            sql += " FOR LOGIN \(escapedLogin)"
        } else {
            sql += " WITHOUT LOGIN"
        }
        
        if let defaultSchema = options.defaultSchema {
            let escapedSchema = Self.escapeIdentifier(defaultSchema)
            sql += " WITH DEFAULT_SCHEMA = \(escapedSchema)"
        }
        
        if let defaultLanguage = options.defaultLanguage {
            sql += ", DEFAULT_LANGUAGE = '\(defaultLanguage.replacingOccurrences(of: "'", with: "''"))'"
        }
        
        if options.allowEncryptedValueModifications {
            sql += ", ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = ON"
        }
        
        _ = try await client.execute(sql)
    }
    
    public func dropUser(name: String) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropUser(name: name)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropUser(name: String) async throws {
        let escapedUserName = Self.escapeIdentifier(name)
        let sql = "DROP USER \(escapedUserName)"
        _ = try await client.execute(sql)
    }
    
    public func alterUser(
        name: String,
        newName: String? = nil,
        defaultSchema: String? = nil,
        login: String? = nil
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterUser(name: name, newName: newName, defaultSchema: defaultSchema, login: login)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func alterUser(
        name: String,
        newName: String? = nil,
        defaultSchema: String? = nil,
        login: String? = nil
    ) async throws {
        let escapedUserName = Self.escapeIdentifier(name)
        var sql = "ALTER USER \(escapedUserName)"
        
        var withClauses: [String] = []
        
        if let newName = newName {
            let escapedNewName = Self.escapeIdentifier(newName)
            sql += " WITH NAME = \(escapedNewName)"
        }
        
        if let defaultSchema = defaultSchema {
            let escapedSchema = Self.escapeIdentifier(defaultSchema)
            withClauses.append("DEFAULT_SCHEMA = \(escapedSchema)")
        }
        
        if let login = login {
            let escapedLogin = Self.escapeIdentifier(login)
            withClauses.append("LOGIN = \(escapedLogin)")
        }
        
        if !withClauses.isEmpty {
            if newName == nil {
                sql += " WITH "
            } else {
                sql += ", "
            }
            sql += withClauses.joined(separator: ", ")
        }
        
        _ = try await client.execute(sql)
    }
    
    // MARK: - Role Management
    
    public func createRole(name: String, options: RoleOptions = RoleOptions()) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createRole(name: name, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createRole(name: String, options: RoleOptions = RoleOptions()) async throws {
        let escapedRoleName = Self.escapeIdentifier(name)
        var sql = "CREATE ROLE \(escapedRoleName)"
        
        if let owner = options.owner {
            let escapedOwner = Self.escapeIdentifier(owner)
            sql += " AUTHORIZATION \(escapedOwner)"
        }
        
        _ = try await client.execute(sql)
    }
    
    public func dropRole(name: String) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropRole(name: name)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropRole(name: String) async throws {
        let escapedRoleName = Self.escapeIdentifier(name)
        let sql = "DROP ROLE \(escapedRoleName)"
        _ = try await client.execute(sql)
    }
    
    public func alterRole(name: String, newName: String) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterRole(name: name, newName: newName)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func alterRole(name: String, newName: String) async throws {
        let escapedRoleName = Self.escapeIdentifier(name)
        let escapedNewName = Self.escapeIdentifier(newName)
        let sql = "ALTER ROLE \(escapedRoleName) WITH NAME = \(escapedNewName)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Role Membership
    
    public func addUserToRole(user: String, role: String) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addUserToRole(user: user, role: role)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func addUserToRole(user: String, role: String) async throws {
        let escapedRoleName = Self.escapeIdentifier(role)
        let escapedUserName = Self.escapeIdentifier(user)
        let sql = "ALTER ROLE \(escapedRoleName) ADD MEMBER \(escapedUserName)"
        _ = try await client.execute(sql)
    }
    
    public func removeUserFromRole(user: String, role: String) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.removeUserFromRole(user: user, role: role)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func removeUserFromRole(user: String, role: String) async throws {
        let escapedRoleName = Self.escapeIdentifier(role)
        let escapedUserName = Self.escapeIdentifier(user)
        let sql = "ALTER ROLE \(escapedRoleName) DROP MEMBER \(escapedUserName)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Permission Management
    
    public func grantPermission(
        permission: Permission,
        on object: String,
        to principal: String,
        withGrantOption: Bool = false
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.grantPermission(permission: permission, on: object, to: principal, withGrantOption: withGrantOption)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func grantPermission(
        permission: Permission,
        on object: String,
        to principal: String,
        withGrantOption: Bool = false
    ) async throws {
        let escapedObject = Self.escapeIdentifier(object)
        let escapedPrincipal = Self.escapeIdentifier(principal)
        
        var sql = "GRANT \(permission.rawValue) ON \(escapedObject) TO \(escapedPrincipal)"
        
        if withGrantOption {
            sql += " WITH GRANT OPTION"
        }
        
        _ = try await client.execute(sql)
    }
    
    public func revokePermission(
        permission: Permission,
        on object: String,
        from principal: String,
        cascadeOption: Bool = false
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.revokePermission(permission: permission, on: object, from: principal, cascadeOption: cascadeOption)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func revokePermission(
        permission: Permission,
        on object: String,
        from principal: String,
        cascadeOption: Bool = false
    ) async throws {
        let escapedObject = Self.escapeIdentifier(object)
        let escapedPrincipal = Self.escapeIdentifier(principal)
        
        var sql = "REVOKE \(permission.rawValue) ON \(escapedObject) FROM \(escapedPrincipal)"
        
        if cascadeOption {
            sql += " CASCADE"
        }
        
        _ = try await client.execute(sql)
    }
    
    public func denyPermission(
        permission: Permission,
        on object: String,
        to principal: String
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.denyPermission(permission: permission, on: object, to: principal)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func denyPermission(
        permission: Permission,
        on object: String,
        to principal: String
    ) async throws {
        let escapedObject = Self.escapeIdentifier(object)
        let escapedPrincipal = Self.escapeIdentifier(principal)
        
        let sql = "DENY \(permission.rawValue) ON \(escapedObject) TO \(escapedPrincipal)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Database Role Membership
    
    public func addUserToDatabaseRole(user: String, role: Permission) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.addUserToDatabaseRole(user: user, role: role)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func addUserToDatabaseRole(user: String, role: Permission) async throws {
        let escapedUser = Self.escapeIdentifier(user)
        let sql = "ALTER ROLE \(role.rawValue) ADD MEMBER \(escapedUser)"
        _ = try await client.execute(sql)
    }
    
    public func removeUserFromDatabaseRole(user: String, role: Permission) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.removeUserFromDatabaseRole(user: user, role: role)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func removeUserFromDatabaseRole(user: String, role: Permission) async throws {
        let escapedUser = Self.escapeIdentifier(user)
        let sql = "ALTER ROLE \(role.rawValue) DROP MEMBER \(escapedUser)"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Information Queries
    
    @available(macOS 12.0, *)
    public func userExists(name: String) async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.database_principals
        WHERE name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND type IN ('S', 'U', 'G')
        """
        
        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }
    
    @available(macOS 12.0, *)
    public func roleExists(name: String) async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.database_principals
        WHERE name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND type = 'R'
        """
        
        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }
    
    @available(macOS 12.0, *)
    public func listUsers() async throws -> [UserInfo] {
        let sql = """
        SELECT 
            name,
            principal_id,
            type_desc as type,
            default_schema_name,
            create_date,
            modify_date,
            0 as is_disabled
        FROM sys.database_principals
        WHERE type IN ('S', 'U', 'G')
        AND name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')
        ORDER BY name
        """
        
        let rows = try await client.query(sql)
        return rows.map { row in
            UserInfo(
                name: row.column("name")?.string ?? "",
                principalId: row.column("principal_id")?.int ?? 0,
                type: row.column("type")?.string ?? "",
                defaultSchema: row.column("default_schema_name")?.string,
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string,
                isDisabled: (row.column("is_disabled")?.int ?? 0) != 0
            )
        }
    }
    
    @available(macOS 12.0, *)
    public func listRoles() async throws -> [RoleInfo] {
        let sql = """
        SELECT 
            name,
            principal_id,
            type_desc as type,
            owning_principal_id,
            is_fixed_role,
            create_date,
            modify_date
        FROM sys.database_principals
        WHERE type = 'R'
        ORDER BY name
        """
        
        let rows = try await client.query(sql)
        return rows.map { row in
            RoleInfo(
                name: row.column("name")?.string ?? "",
                principalId: row.column("principal_id")?.int ?? 0,
                type: row.column("type")?.string ?? "",
                ownerPrincipalId: row.column("owning_principal_id")?.int,
                isFixedRole: (row.column("is_fixed_role")?.int ?? 0) != 0,
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string
            )
        }
    }
    
    @available(macOS 12.0, *)
    public func listUserRoles(user: String) async throws -> [String] {
        let sql = """
        SELECT r.name as role_name
        FROM sys.database_role_members rm
        INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        INNER JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id
        WHERE u.name = '\(user.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY r.name
        """
        
        let rows = try await client.query(sql)
        return rows.compactMap { $0.column("role_name")?.string }
    }
    
    @available(macOS 12.0, *)
    public func listRoleMembers(role: String) async throws -> [String] {
        let sql = """
        SELECT u.name as user_name
        FROM sys.database_role_members rm
        INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        INNER JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id
        WHERE r.name = '\(role.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY u.name
        """
        
        let rows = try await client.query(sql)
        return rows.compactMap { $0.column("user_name")?.string }
    }
    
    @available(macOS 12.0, *)
    public func listPermissions(principal: String? = nil, object: String? = nil) async throws -> [PermissionInfo] {
        var sql = """
        SELECT 
            p.permission_name,
            p.state_desc,
            OBJECT_NAME(p.major_id) as object_name,
            pr.name as principal_name,
            grantor.name as grantor_name
        FROM sys.database_permissions p
        INNER JOIN sys.database_principals pr ON p.grantee_principal_id = pr.principal_id
        LEFT JOIN sys.database_principals grantor ON p.grantor_principal_id = grantor.principal_id
        WHERE p.major_id > 0
        """
        
        if let principal = principal {
            sql += " AND pr.name = '\(principal.replacingOccurrences(of: "'", with: "''"))'"
        }
        
        if let object = object {
            sql += " AND OBJECT_NAME(p.major_id) = '\(object.replacingOccurrences(of: "'", with: "''"))'"
        }
        
        sql += " ORDER BY pr.name, p.permission_name"
        
        let rows = try await client.query(sql)
        return rows.map { row in
            PermissionInfo(
                permission: row.column("permission_name")?.string ?? "",
                state: row.column("state_desc")?.string ?? "",
                objectName: row.column("object_name")?.string,
                principalName: row.column("principal_name")?.string ?? "",
                grantor: row.column("grantor_name")?.string
            )
        }
    }
    
    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}