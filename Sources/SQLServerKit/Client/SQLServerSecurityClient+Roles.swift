import NIO
import SQLServerTDS

extension SQLServerSecurityClient {
    // MARK: - Role Management
    
    internal func createRole(name: String, options: RoleOptions = RoleOptions()) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    internal func dropRole(name: String) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    internal func alterRole(name: String, newName: String) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    internal func addUserToRole(user: String, role: String) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    internal func removeUserFromRole(user: String, role: String) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    // MARK: - Database Role Membership
    
    internal func addUserToDatabaseRole(user: String, role: Permission) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    internal func removeUserFromDatabaseRole(user: String, role: Permission) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
    
    // MARK: - Application roles

    @available(macOS 12.0, *)
    public func listApplicationRoles() async throws -> [ApplicationRoleInfo] {
        let sql = """
        SELECT name, default_schema_name, create_date, modify_date
        FROM sys.database_principals
        WHERE type = 'A'
        ORDER BY name
        """
        let rows = try await query(sql)
        return rows.map { r in
            ApplicationRoleInfo(
                name: r.column("name")?.string ?? "",
                defaultSchema: r.column("default_schema_name")?.string,
                createDate: r.column("create_date")?.string,
                modifyDate: r.column("modify_date")?.string
            )
        }
    }

    internal func listApplicationRolesELF() -> EventLoopFuture<[ApplicationRoleInfo]> {
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

    internal func createApplicationRole(name: String, password: String, defaultSchema: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE APPLICATION ROLE \(Self.escapeIdentifier(name)) WITH PASSWORD = N'\(password.replacingOccurrences(of: "'", with: "''"))'"
        if let ds = defaultSchema { sql += ", DEFAULT_SCHEMA = \(Self.escapeIdentifier(ds))" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func alterApplicationRole(name: String, newName: String? = nil, password: String? = nil) -> EventLoopFuture<Void> {
        var parts: [String] = []
        if let newName { parts.append("NAME = \(Self.escapeIdentifier(newName))") }
        if let password { parts.append("PASSWORD = N'\(password.replacingOccurrences(of: "'", with: "''"))'") }
        guard !parts.isEmpty else { return run("SELECT 1").map { _ in () } }
        let sql = "ALTER APPLICATION ROLE \(Self.escapeIdentifier(name)) WITH \(parts.joined(separator: ", "));"
        return run(sql).map { _ in () }
    }

    internal func dropApplicationRole(name: String) -> EventLoopFuture<Void> {
        let sql = "DROP APPLICATION ROLE \(Self.escapeIdentifier(name));"
        return run(sql).map { _ in () }
    }
}
