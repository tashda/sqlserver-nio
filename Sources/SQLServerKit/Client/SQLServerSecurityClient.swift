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

public struct DetailedPermissionInfo: Sendable {
    public let permission: String
    public let state: String
    public let classDesc: String
    public let schemaName: String?
    public let objectName: String?
    public let columnName: String?
    public let principalName: String
    public let grantor: String?
}

// MARK: - SQLServerSecurityClient

public final class SQLServerSecurityClient {
    private enum Backing {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }
    private let backing: Backing

    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }

    private init(backing: Backing) {
        self.backing = backing
    }
    
    // MARK: - User Management
    
    public func createUser(
        name: String,
        login: String? = nil,
        options: UserOptions = UserOptions()
    ) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        
        _ = try await exec(sql)
    }
    
    public func dropUser(name: String) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    public func alterUser(
        name: String,
        newName: String? = nil,
        defaultSchema: String? = nil,
        login: String? = nil
    ) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        
        _ = try await exec(sql)
    }
    
    // MARK: - Role Management
    
    public func createRole(name: String, options: RoleOptions = RoleOptions()) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        
        _ = try await exec(sql)
    }
    
    public func dropRole(name: String) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    public func alterRole(name: String, newName: String) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    // MARK: - Role Membership
    
    public func addUserToRole(user: String, role: String) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    public func removeUserFromRole(user: String, role: String) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    // MARK: - Permission Management
    
    public func grantPermission(
        permission: Permission,
        on object: String,
        to principal: String,
        withGrantOption: Bool = false
    ) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        
        _ = try await exec(sql)
    }
    
    public func revokePermission(
        permission: Permission,
        on object: String,
        from principal: String,
        cascadeOption: Bool = false
    ) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        
        _ = try await exec(sql)
    }
    
    public func denyPermission(
        permission: Permission,
        on object: String,
        to principal: String
    ) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    // MARK: - Database Role Membership
    
    public func addUserToDatabaseRole(user: String, role: Permission) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
    }
    
    public func removeUserFromDatabaseRole(user: String, role: Permission) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing {
            case .client(let c): return c.eventLoopGroup.next()
            case .connection(let conn): return conn.eventLoop
            }
        }()
        let promise = loop.makePromise(of: Void.self)
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
        _ = try await exec(sql)
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
        let result = try await queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }

    // MARK: - Securable-aware permissions

    public func grant(permission: DatabasePermissionName, on securable: Securable, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "GRANT", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: withGrantOption, cascade: nil)
    }
    public func grant(permission: ObjectPermissionName, on securable: Securable, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "GRANT", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: withGrantOption, cascade: nil)
    }

    public func revoke(permission: DatabasePermissionName, on securable: Securable, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "REVOKE", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: cascade)
    }
    public func revoke(permission: ObjectPermissionName, on securable: Securable, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "REVOKE", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: cascade)
    }

    public func deny(permission: DatabasePermissionName, on securable: Securable, to principal: String) -> EventLoopFuture<Void> {
        emitPermission(kind: "DENY", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: nil)
    }
    public func deny(permission: ObjectPermissionName, on securable: Securable, to principal: String) -> EventLoopFuture<Void> {
        emitPermission(kind: "DENY", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: nil)
    }

    @available(macOS 12.0, *)
    public func grant(permission: DatabasePermissionName, on securable: Securable, to principal: String, withGrantOption: Bool = false) async throws { _ = try await grant(permission: permission, on: securable, to: principal, withGrantOption: withGrantOption).get() }
    @available(macOS 12.0, *)
    public func grant(permission: ObjectPermissionName, on securable: Securable, to principal: String, withGrantOption: Bool = false) async throws { _ = try await grant(permission: permission, on: securable, to: principal, withGrantOption: withGrantOption).get() }
    @available(macOS 12.0, *)
    public func revoke(permission: DatabasePermissionName, on securable: Securable, from principal: String, cascade: Bool = false) async throws { _ = try await revoke(permission: permission, on: securable, from: principal, cascade: cascade).get() }
    @available(macOS 12.0, *)
    public func revoke(permission: ObjectPermissionName, on securable: Securable, from principal: String, cascade: Bool = false) async throws { _ = try await revoke(permission: permission, on: securable, from: principal, cascade: cascade).get() }
    @available(macOS 12.0, *)
    public func deny(permission: DatabasePermissionName, on securable: Securable, to principal: String) async throws { _ = try await deny(permission: permission, on: securable, to: principal).get() }
    @available(macOS 12.0, *)
    public func deny(permission: ObjectPermissionName, on securable: Securable, to principal: String) async throws { _ = try await deny(permission: permission, on: securable, to: principal).get() }

    private func emitPermission(kind: String, permission: String, on securable: Securable, principal: String, withGrantOption: Bool?, cascade: Bool?) -> EventLoopFuture<Void> {
        let loop: EventLoop = {
            switch backing { case .client(let c): return c.eventLoopGroup.next(); case .connection(let conn): return conn.eventLoop }
        }()
        let promise = loop.makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                var sql = "\(kind) \(permission)"
                var targetClause: String? = nil
                switch securable {
                case .server:
                    throw SQLServerError.sqlExecutionError(message: "Server-level securable not supported by database client")
                case .database(let nameOptional):
                    // Historical overload: treat like DB-level current context; ignore name
                    _ = nameOptional
                    break
                case .schema(let schema):
                    targetClause = "SCHEMA::\(Self.escapeIdentifier(schema))"
                case .object(let oid):
                    targetClause = "OBJECT::\(Self.escapeIdentifier(oid.schema)).\(Self.escapeIdentifier(oid.name))"
                case .column(let oid, let columns):
                    // Column list goes after permission token
                    if !columns.isEmpty {
                        let cols = columns.map { Self.escapeIdentifier($0) }.joined(separator: ",")
                        sql += " (\(cols))"
                    }
                    targetClause = "OBJECT::\(Self.escapeIdentifier(oid.schema)).\(Self.escapeIdentifier(oid.name))"
                }
                if let target = targetClause { sql += " ON \(target)" }
                sql += " \(kind == "REVOKE" ? "FROM" : "TO") \(Self.escapeIdentifier(principal))"
                if kind == "GRANT", let wgo = withGrantOption, wgo { sql += " WITH GRANT OPTION" }
                if kind == "REVOKE", let c = cascade, c { sql += " CASCADE" }
                _ = try await self.exec(sql)
                return ()
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    /// Extended permission listing including database and schema scope and column-level entries.
    @available(macOS 12.0, *)
    public func listPermissionsDetailed(principal: String? = nil) async throws -> [DetailedPermissionInfo] {
        var sql = """
        SELECT
            p.permission_name,
            p.state_desc,
            p.class_desc,
            ISNULL(sch.name, '') AS schema_name,
            ISNULL(obj.name, '') AS object_name,
            ISNULL(col.name, '') AS column_name,
            grantee.name AS principal_name,
            ISNULL(grantor.name, '') AS grantor_name
        FROM sys.database_permissions AS p
        INNER JOIN sys.database_principals AS grantee ON grantee.principal_id = p.grantee_principal_id
        LEFT JOIN sys.database_principals AS grantor ON grantor.principal_id = p.grantor_principal_id
        LEFT JOIN sys.objects AS obj ON (p.class = 1 AND obj.object_id = p.major_id)
        LEFT JOIN sys.schemas AS sch ON (
            (p.class = 3 AND sch.schema_id = p.major_id) OR
            (p.class = 1 AND obj.schema_id = sch.schema_id)
        )
        LEFT JOIN sys.columns AS col ON (p.class = 1 AND p.minor_id > 0 AND col.object_id = p.major_id AND col.column_id = p.minor_id)
        """
        if let principal = principal {
            sql += " WHERE grantee.name = N'\(principal.replacingOccurrences(of: "'", with: "''"))'"
        }
        sql += " ORDER BY grantee.name, p.class, p.permission_name;"
        let rows = try await query(sql)
        return rows.map { row in
            DetailedPermissionInfo(
                permission: row.column("permission_name")?.string ?? "",
                state: row.column("state_desc")?.string ?? "",
                classDesc: row.column("class_desc")?.string ?? "",
                schemaName: row.column("schema_name")?.string,
                objectName: row.column("object_name")?.string,
                columnName: row.column("column_name")?.string,
                principalName: row.column("principal_name")?.string ?? "",
                grantor: row.column("grantor_name")?.string
            )
        }
    }

    // MARK: - Application roles

    public struct ApplicationRoleInfo: Sendable {
        public let name: String
        public let defaultSchema: String?
        public let createDate: String?
        public let modifyDate: String?
    }

    public func listApplicationRoles() -> EventLoopFuture<[ApplicationRoleInfo]> {
        let sql = """
        SELECT name, default_schema_name, create_date, modify_date
        FROM sys.database_principals
        WHERE type = 'A'
        ORDER BY name;
        """
        return run(sql).map { rows in
            rows.map { r in
                ApplicationRoleInfo(
                    name: r.column("name")?.string ?? "",
                    defaultSchema: r.column("default_schema_name")?.string,
                    createDate: r.column("create_date")?.string,
                    modifyDate: r.column("modify_date")?.string
                )
            }
        }
    }

    public func createApplicationRole(name: String, password: String, defaultSchema: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE APPLICATION ROLE \(Self.escapeIdentifier(name)) WITH PASSWORD = N'\(password.replacingOccurrences(of: "'", with: "''"))'"
        if let ds = defaultSchema { sql += ", DEFAULT_SCHEMA = \(Self.escapeIdentifier(ds))" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    public func alterApplicationRole(name: String, newName: String? = nil, password: String? = nil) -> EventLoopFuture<Void> {
        var parts: [String] = []
        if let newName { parts.append("NAME = \(Self.escapeIdentifier(newName))") }
        if let password { parts.append("PASSWORD = N'\(password.replacingOccurrences(of: "'", with: "''"))'") }
        guard !parts.isEmpty else { return run("SELECT 1").map { _ in () } }
        let sql = "ALTER APPLICATION ROLE \(Self.escapeIdentifier(name)) WITH \(parts.joined(separator: ", "));"
        return run(sql).map { _ in () }
    }

    public func dropApplicationRole(name: String) -> EventLoopFuture<Void> {
        let sql = "DROP APPLICATION ROLE \(Self.escapeIdentifier(name));"
        return run(sql).map { _ in () }
    }

    // MARK: - Schema helpers (security-flavored)

    public struct SchemaInfo: Sendable {
        public let name: String
        public let owner: String?
    }

    public func listSchemas() -> EventLoopFuture<[SchemaInfo]> {
        let sql = """
        SELECT s.name, dp.name AS owner
        FROM sys.schemas AS s
        LEFT JOIN sys.database_principals AS dp ON s.principal_id = dp.principal_id
        WHERE s.schema_id <> 4 -- exclude sys
        ORDER BY s.name;
        """
        return run(sql).map { rows in
            rows.map { r in SchemaInfo(name: r.column("name")?.string ?? "", owner: r.column("owner")?.string) }
        }
    }

    public func createSchema(name: String, authorization: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE SCHEMA \(Self.escapeIdentifier(name))"
        if let auth = authorization { sql += " AUTHORIZATION \(Self.escapeIdentifier(auth))" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    public func dropSchema(name: String) -> EventLoopFuture<Void> {
        let sql = "DROP SCHEMA \(Self.escapeIdentifier(name));"
        return run(sql).map { _ in () }
    }

    /// Drops a schema. When `cascade` is true, attempts to drop all objects in the schema first (views, functions, procedures, synonyms, foreign keys, tables, types), then drops the schema.
    /// Note: This operates within the current database context and requires appropriate privileges. It mirrors SSMS behavior for manual cascades.
    public func dropSchema(name: String, cascade: Bool) -> EventLoopFuture<Void> {
        guard cascade else { return dropSchema(name: name) }
        let schemaLit = name.replacingOccurrences(of: "'", with: "''")
        let script = """
        SET NOCOUNT ON;
        DECLARE @schema sysname = N'\(schemaLit)';
        BEGIN TRY
            BEGIN TRAN;

            -- 1) Drop foreign keys on tables in the schema
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + N'ALTER TABLE '
                + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(parent_object_id))
                + N' DROP CONSTRAINT ' + QUOTENAME(name) + N';\n'
            FROM sys.foreign_keys
            WHERE parent_object_id IN (
                SELECT o.object_id FROM sys.objects AS o WHERE o.type = 'U' AND o.schema_id = SCHEMA_ID(@schema)
            );
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 2) Drop views
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP VIEW ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'V';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 3) Drop functions (scalar + table-valued)
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP FUNCTION ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type IN ('FN','TF','IF','FS','FT');
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 4) Drop procedures
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP PROCEDURE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'P';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 5) Drop synonyms
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP SYNONYM ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'SN';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 6) Drop tables (no FKs remain here)
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';\n'
            FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.type = 'U';
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 7) Drop user-defined types in schema
            SET @sql = N'';
            SELECT @sql = @sql + N'DROP TYPE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';\n'
            FROM sys.types AS t JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.is_user_defined = 1;
            IF LEN(@sql) > 0 EXEC sp_executesql @sql;

            -- 8) Finally drop schema
            EXEC('DROP SCHEMA ' + QUOTENAME(@schema));
            COMMIT;
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0 ROLLBACK;
            THROW;
        END CATCH
        """
        return run(script).map { _ in () }
    }

    public func alterAuthorizationOnSchema(schema: String, principal: String) -> EventLoopFuture<Void> {
        let sql = "ALTER AUTHORIZATION ON SCHEMA::\(Self.escapeIdentifier(schema)) TO \(Self.escapeIdentifier(principal));"
        return run(sql).map { _ in () }
    }

    public func transferObjectToSchema(objectSchema: String, objectName: String, newSchema: String) -> EventLoopFuture<Void> {
        let sql = "ALTER SCHEMA \(Self.escapeIdentifier(newSchema)) TRANSFER OBJECT::\(Self.escapeIdentifier(objectSchema)).\(Self.escapeIdentifier(objectName));"
        return run(sql).map { _ in () }
    }
    
    @available(macOS 12.0, *)
    public func roleExists(name: String) async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.database_principals
        WHERE name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND type = 'R'
        """
        let result = try await queryScalar(sql, as: Int.self)
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
        
        let rows = try await query(sql)
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
        
        let rows = try await query(sql)
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
        
        let rows = try await query(sql)
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
        
        let rows = try await query(sql)
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
        
        let rows = try await query(sql)
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

    // MARK: - Backing execution helpers
    @available(macOS 12.0, *)
    private func exec(_ sql: String) async throws -> SQLServerExecutionResult {
        switch backing {
        case .client(let c):
            return try await c.execute(sql)
        case .connection(let conn):
            return try await conn.execute(sql).get()
        }
    }
    @available(macOS 12.0, *)
    private func query(_ sql: String) async throws -> [TDSRow] {
        switch backing {
        case .client(let c):
            return try await c.query(sql)
        case .connection(let conn):
            return try await conn.query(sql).get()
        }
    }
    @available(macOS 12.0, *)
    private func queryScalar<T: TDSDataConvertible>(_ sql: String, as: T.Type) async throws -> T? {
        switch backing {
        case .client(let c):
            return try await c.queryScalar(sql, as: T.self)
        case .connection(let conn):
            return try await conn.queryScalar(sql, as: T.self).get()
        }
    }

    // Non-async convenience used by some EventLoopFuture-returning helpers above
    private func run(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .client(let c):
            return c.query(sql)
        case .connection(let conn):
            return conn.query(sql)
        }
    }
}

// Back-compat: database-scoped client alias
public typealias SQLServerDatabaseSecurityClient = SQLServerSecurityClient
