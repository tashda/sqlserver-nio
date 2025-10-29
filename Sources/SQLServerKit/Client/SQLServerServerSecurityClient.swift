import Foundation
import NIO
import SQLServerTDS

// MARK: - Server-scoped Security Client

public final class SQLServerServerSecurityClient {
    private enum Backing {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }
    private let backing: Backing

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }
    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }
    private init(backing: Backing) { self.backing = backing }

    // MARK: - Logins
    public func listLogins(includeDisabled: Bool = true) -> EventLoopFuture<[ServerLoginInfo]> {
        run(sql: {
            var sql = """
            SELECT name,
                   type_desc,
                   is_disabled,
                   default_database_name,
                   default_language_name
            FROM sys.server_principals
            WHERE type IN ('S', 'U', 'G', 'C', 'K', 'E')
            """
            if !includeDisabled { sql += " AND is_disabled = 0" }
            sql += " ORDER BY name"
            return sql
        }()).map { rows in
            rows.compactMap { row -> ServerLoginInfo? in
                let name = row.column("name")?.string ?? ""
                let typeDesc = row.column("type_desc")?.string ?? ""
                let disabled = (row.column("is_disabled")?.int ?? 0) == 1
                let defDb = row.column("default_database_name")?.string
                let defLang = row.column("default_language_name")?.string
                let type: ServerLoginType
                switch typeDesc {
                case "SQL_LOGIN": type = .sql
                case "WINDOWS_LOGIN": type = .windowsUser
                case "WINDOWS_GROUP": type = .windowsGroup
                case "CERTIFICATE_MAPPED_LOGIN": type = .certificate
                case "ASYMMETRIC_KEY_MAPPED_LOGIN": type = .asymmetricKey
                case "EXTERNAL_LOGIN": type = .external
                default: type = .sql
                }
                return ServerLoginInfo(name: name, type: type, isDisabled: disabled, defaultDatabase: defDb, defaultLanguage: defLang)
            }
        }
    }

    public func createSqlLogin(name: String, password: String, options: LoginOptions = .init()) -> EventLoopFuture<Void> {
        exec(sql: buildCreateSqlLogin(name: name, password: password, options: options)).map { _ in () }
    }

    public func createWindowsLogin(name: String) -> EventLoopFuture<Void> {
        let sql = "CREATE LOGIN [\(escapeIdentifier(name))] FROM WINDOWS;"
        return exec(sql: sql).map { _ in () }
    }

    // MARK: - Additional login types
    public func createCertificateLogin(name: String, certificateName: String, defaultDatabase: String? = nil, defaultLanguage: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE LOGIN [\(escapeIdentifier(name))] FROM CERTIFICATE [\(escapeIdentifier(certificateName))]"
        var withs: [String] = []
        if let db = defaultDatabase { withs.append("DEFAULT_DATABASE = [\(escapeIdentifier(db))]") }
        if let lang = defaultLanguage { withs.append("DEFAULT_LANGUAGE = [\(escapeIdentifier(lang))]") }
        if !withs.isEmpty { sql += " WITH \(withs.joined(separator: ", "))" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    public func createAsymmetricKeyLogin(name: String, asymmetricKeyName: String, defaultDatabase: String? = nil, defaultLanguage: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE LOGIN [\(escapeIdentifier(name))] FROM ASYMMETRIC KEY [\(escapeIdentifier(asymmetricKeyName))]"
        var withs: [String] = []
        if let db = defaultDatabase { withs.append("DEFAULT_DATABASE = [\(escapeIdentifier(db))]") }
        if let lang = defaultLanguage { withs.append("DEFAULT_LANGUAGE = [\(escapeIdentifier(lang))]") }
        if !withs.isEmpty { sql += " WITH \(withs.joined(separator: ", "))" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    public func createExternalLogin(name: String) -> EventLoopFuture<Void> {
        // Azure AD / external provider login
        let sql = "CREATE LOGIN [\(escapeIdentifier(name))] FROM EXTERNAL PROVIDER;"
        return exec(sql: sql).map { _ in () }
    }

    public func enableLogin(name: String, enabled: Bool) -> EventLoopFuture<Void> {
        let sql = "ALTER LOGIN [\(escapeIdentifier(name))] \(enabled ? "ENABLE" : "DISABLE");"
        return exec(sql: sql).map { _ in () }
    }

    public func setLoginPassword(name: String, newPassword: String, oldPassword: String? = nil, mustChange: Bool? = nil) -> EventLoopFuture<Void> {
        // Per T-SQL grammar, OLD_PASSWORD and MUST_CHANGE are part of the PASSWORD clause
        // and are not separated by commas. Example:
        // ALTER LOGIN [name] WITH PASSWORD = N'new' OLD_PASSWORD = N'old' MUST_CHANGE;
        var sql = "ALTER LOGIN [\(escapeIdentifier(name))] WITH PASSWORD = N'\(escapeLiteral(newPassword))'"
        if let old = oldPassword { sql += " OLD_PASSWORD = N'\(escapeLiteral(old))'" }
        if let mc = mustChange, mc { sql += " MUST_CHANGE" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    public func alterLogin(name: String, options: LoginAlterOptions) -> EventLoopFuture<Void> {
        var parts: [String] = []
        if let db = options.defaultDatabase { parts.append("DEFAULT_DATABASE = [\(escapeIdentifier(db))]") }
        if let lang = options.defaultLanguage { parts.append("DEFAULT_LANGUAGE = [\(escapeIdentifier(lang))]") }
        if let cp = options.checkPolicy { parts.append("CHECK_POLICY = \(cp ? "ON" : "OFF")") }
        if let ce = options.checkExpiration { parts.append("CHECK_EXPIRATION = \(ce ? "ON" : "OFF")") }
        guard !parts.isEmpty else { return futureSucceeded(()) }
        let sql = "ALTER LOGIN [\(escapeIdentifier(name))] WITH \(parts.joined(separator: ", "));"
        return exec(sql: sql).map { _ in () }
    }

    public func dropLogin(name: String, dropMappedUsers: Bool = false) -> EventLoopFuture<Void> {
        // Best-effort: optionally drop mapped users across databases (requires high privileges)
        let loginName = escapeLiteral(name)
        let pre: String = dropMappedUsers ? """
        DECLARE @db sysname;
        DECLARE cur CURSOR FAST_FORWARD FOR
        SELECT name FROM sys.databases WHERE state = 0 AND database_id > 4;
        OPEN cur; FETCH NEXT FROM cur INTO @db;
        WHILE @@FETCH_STATUS = 0 BEGIN
            DECLARE @sql nvarchar(max) = N'USE ' + QUOTENAME(@db) + N';
                DECLARE @u sysname; DECLARE c CURSOR FAST_FORWARD FOR
                SELECT name FROM sys.database_principals WHERE sid = SUSER_SID(N'\(loginName)');
                OPEN c; FETCH NEXT FROM c INTO @u; WHILE @@FETCH_STATUS = 0 BEGIN
                    BEGIN TRY EXEC(N'DROP USER ' + QUOTENAME(@u)); END TRY BEGIN CATCH END CATCH;
                    FETCH NEXT FROM c INTO @u; END; CLOSE c; DEALLOCATE c;';
            EXEC sp_executesql @sql; FETCH NEXT FROM cur INTO @db; END; CLOSE cur; DEALLOCATE cur;
        """ : ""
        let sql = pre + "DROP LOGIN [\(escapeIdentifier(name))];"
        return exec(sql: sql).map { _ in () }
    }

    // Options
    public struct LoginOptions: Sendable {
        public var defaultDatabase: String?
        public var defaultLanguage: String?
        public var checkPolicy: Bool? = nil
        public var checkExpiration: Bool? = nil
        public var sid: Data? = nil
        public var mustChange: Bool? = nil
        public init(defaultDatabase: String? = nil, defaultLanguage: String? = nil, checkPolicy: Bool? = nil, checkExpiration: Bool? = nil, sid: Data? = nil, mustChange: Bool? = nil) {
            self.defaultDatabase = defaultDatabase
            self.defaultLanguage = defaultLanguage
            self.checkPolicy = checkPolicy
            self.checkExpiration = checkExpiration
            self.sid = sid
            self.mustChange = mustChange
        }
    }

    public struct LoginAlterOptions: Sendable {
        public var defaultDatabase: String?
        public var defaultLanguage: String?
        public var checkPolicy: Bool?
        public var checkExpiration: Bool?
        public init(defaultDatabase: String? = nil, defaultLanguage: String? = nil, checkPolicy: Bool? = nil, checkExpiration: Bool? = nil) {
            self.defaultDatabase = defaultDatabase
            self.defaultLanguage = defaultLanguage
            self.checkPolicy = checkPolicy
            self.checkExpiration = checkExpiration
        }
    }

    // MARK: - Server roles
    public func listServerRoles() -> EventLoopFuture<[ServerRoleInfo]> {
        run(sql: """
            SELECT name, is_fixed_role = CASE WHEN is_fixed_role = 1 THEN 1 ELSE 0 END
            FROM sys.server_principals WHERE type = 'R' ORDER BY name
        """).map { rows in
            rows.map { row in
                ServerRoleInfo(name: row.column("name")?.string ?? "", isFixed: (row.column("is_fixed_role")?.int ?? 0) == 1)
            }
        }
    }

    public func createServerRole(name: String) -> EventLoopFuture<Void> {
        exec(sql: "CREATE SERVER ROLE [\(escapeIdentifier(name))];").map { _ in () }
    }
    public func alterServerRole(name: String, newName: String?) -> EventLoopFuture<Void> {
        guard let nn = newName, !nn.isEmpty else { return futureSucceeded(()) }
        return exec(sql: "ALTER SERVER ROLE [\(escapeIdentifier(name))] WITH NAME = [\(escapeIdentifier(nn))];").map { _ in () }
    }
    public func dropServerRole(name: String) -> EventLoopFuture<Void> {
        exec(sql: "DROP SERVER ROLE [\(escapeIdentifier(name))];").map { _ in () }
    }
    public func addMemberToServerRole(role: String, principal: String) -> EventLoopFuture<Void> {
        exec(sql: "ALTER SERVER ROLE [\(escapeIdentifier(role))] ADD MEMBER [\(escapeIdentifier(principal))];").map { _ in () }
    }
    public func removeMemberFromServerRole(role: String, principal: String) -> EventLoopFuture<Void> {
        exec(sql: "ALTER SERVER ROLE [\(escapeIdentifier(role))] DROP MEMBER [\(escapeIdentifier(principal))];").map { _ in () }
    }
    public func listServerRoleMembers(role: String) -> EventLoopFuture<[String]> {
        run(sql: """
            SELECT spm.name AS member_name
            FROM sys.server_role_members m
            JOIN sys.server_principals spr ON spr.principal_id = m.role_principal_id
            JOIN sys.server_principals spm ON spm.principal_id = m.member_principal_id
            WHERE spr.name = N'\(escapeLiteral(role))'
            ORDER BY spm.name
        """).map { rows in rows.compactMap { $0.column("member_name")?.string } }
    }
    public func listServerRolesForPrincipal(principal: String) -> EventLoopFuture<[String]> {
        run(sql: """
            SELECT spr.name AS role_name
            FROM sys.server_role_members m
            JOIN sys.server_principals spr ON spr.principal_id = m.role_principal_id
            JOIN sys.server_principals spm ON spm.principal_id = m.member_principal_id
            WHERE spm.name = N'\(escapeLiteral(principal))'
            ORDER BY spr.name
        """).map { rows in rows.compactMap { $0.column("role_name")?.string } }
    }

    // MARK: - Server permissions
    public func grant(permission: ServerPermissionName, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        var sql = "GRANT \(permission.rawValue) TO [\(escapeIdentifier(principal))]"
        if withGrantOption { sql += " WITH GRANT OPTION" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }
    public func revoke(permission: ServerPermissionName, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        var sql = "REVOKE \(permission.rawValue) FROM [\(escapeIdentifier(principal))]"
        if cascade { sql += " CASCADE" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }
    public func deny(permission: ServerPermissionName, to principal: String) -> EventLoopFuture<Void> {
        let sql = "DENY \(permission.rawValue) TO [\(escapeIdentifier(principal))];"
        return exec(sql: sql).map { _ in () }
    }
    public struct ServerPermissionInfo: Sendable {
        public let permission: String
        public let state: String
        public let principalName: String
        public let grantor: String?
    }
    public func listPermissions(principal: String? = nil) -> EventLoopFuture<[ServerPermissionInfo]> {
        var sql = """
        SELECT p.permission_name, p.state_desc, grantee.name AS principal_name, grantor.name AS grantor_name
        FROM sys.server_permissions p
        JOIN sys.server_principals grantee ON grantee.principal_id = p.grantee_principal_id
        LEFT JOIN sys.server_principals grantor ON grantor.principal_id = p.grantor_principal_id
        WHERE 1=1
        """
        if let pr = principal { sql += " AND grantee.name = N'\(escapeLiteral(pr))'" }
        sql += " ORDER BY grantee.name, p.permission_name"
        return run(sql: sql).map { rows in
            rows.map { r in
                ServerPermissionInfo(
                    permission: r.column("permission_name")?.string ?? "",
                    state: r.column("state_desc")?.string ?? "",
                    principalName: r.column("principal_name")?.string ?? "",
                    grantor: r.column("grantor_name")?.string
                )
            }
        }
    }

    // MARK: - Credentials
    public struct CredentialInfo: Sendable {
        public let name: String
        public let identity: String?
    }
    public func listCredentials() -> EventLoopFuture<[CredentialInfo]> {
        run(sql: "SELECT name, credential_identity FROM sys.credentials ORDER BY name").map { rows in
            rows.map { r in CredentialInfo(name: r.column("name")?.string ?? "", identity: r.column("credential_identity")?.string) }
        }
    }
    public func createCredential(name: String, identity: String, secret: String) -> EventLoopFuture<Void> {
        let sql = "CREATE CREDENTIAL [\(escapeIdentifier(name))] WITH IDENTITY = N'\(escapeLiteral(identity))', SECRET = N'\(escapeLiteral(secret))';"
        return exec(sql: sql).map { _ in () }
    }
    public func alterCredential(name: String, identity: String?, secret: String?) -> EventLoopFuture<Void> {
        var parts: [String] = []
        if let id = identity { parts.append("IDENTITY = N'\(escapeLiteral(id))'") }
        if let s = secret { parts.append("SECRET = N'\(escapeLiteral(s))'") }
        guard !parts.isEmpty else { return futureSucceeded(()) }
        let sql = "ALTER CREDENTIAL [\(escapeIdentifier(name))] WITH \(parts.joined(separator: ", "));"
        return exec(sql: sql).map { _ in () }
    }
    public func dropCredential(name: String) -> EventLoopFuture<Void> {
        exec(sql: "DROP CREDENTIAL [\(escapeIdentifier(name))];").map { _ in () }
    }

    // MARK: - Async convenience
    @available(macOS 12.0, *)
    public func listLogins(includeDisabled: Bool = true) async throws -> [ServerLoginInfo] { try await listLogins(includeDisabled: includeDisabled).get() }
    @available(macOS 12.0, *)
    public func createSqlLogin(name: String, password: String, options: LoginOptions = .init()) async throws { _ = try await createSqlLogin(name: name, password: password, options: options).get() }
    @available(macOS 12.0, *)
    public func createWindowsLogin(name: String) async throws { _ = try await createWindowsLogin(name: name).get() }
    @available(macOS 12.0, *)
    public func enableLogin(name: String, enabled: Bool) async throws { _ = try await enableLogin(name: name, enabled: enabled).get() }
    @available(macOS 12.0, *)
    public func setLoginPassword(name: String, newPassword: String, oldPassword: String? = nil, mustChange: Bool? = nil) async throws { _ = try await setLoginPassword(name: name, newPassword: newPassword, oldPassword: oldPassword, mustChange: mustChange).get() }
    @available(macOS 12.0, *)
    public func alterLogin(name: String, options: LoginAlterOptions) async throws { _ = try await alterLogin(name: name, options: options).get() }
    @available(macOS 12.0, *)
    public func dropLogin(name: String, dropMappedUsers: Bool = false) async throws { _ = try await dropLogin(name: name, dropMappedUsers: dropMappedUsers).get() }
    @available(macOS 12.0, *)
    public func listServerRoles() async throws -> [ServerRoleInfo] { try await listServerRoles().get() }
    @available(macOS 12.0, *)
    public func createServerRole(name: String) async throws { _ = try await createServerRole(name: name).get() }
    @available(macOS 12.0, *)
    public func alterServerRole(name: String, newName: String?) async throws { _ = try await alterServerRole(name: name, newName: newName).get() }
    @available(macOS 12.0, *)
    public func dropServerRole(name: String) async throws { _ = try await dropServerRole(name: name).get() }
    @available(macOS 12.0, *)
    public func addMemberToServerRole(role: String, principal: String) async throws { _ = try await addMemberToServerRole(role: role, principal: principal).get() }
    @available(macOS 12.0, *)
    public func removeMemberFromServerRole(role: String, principal: String) async throws { _ = try await removeMemberFromServerRole(role: role, principal: principal).get() }
    @available(macOS 12.0, *)
    public func listServerRoleMembers(role: String) async throws -> [String] { try await listServerRoleMembers(role: role).get() }
    @available(macOS 12.0, *)
    public func listServerRolesForPrincipal(principal: String) async throws -> [String] { try await listServerRolesForPrincipal(principal: principal).get() }
    @available(macOS 12.0, *)
    public func grant(permission: ServerPermissionName, to principal: String, withGrantOption: Bool = false) async throws { _ = try await grant(permission: permission, to: principal, withGrantOption: withGrantOption).get() }
    @available(macOS 12.0, *)
    public func revoke(permission: ServerPermissionName, from principal: String, cascade: Bool = false) async throws { _ = try await revoke(permission: permission, from: principal, cascade: cascade).get() }
    @available(macOS 12.0, *)
    public func deny(permission: ServerPermissionName, to principal: String) async throws { _ = try await deny(permission: permission, to: principal).get() }
    @available(macOS 12.0, *)
    public func listPermissions(principal: String? = nil) async throws -> [ServerPermissionInfo] { try await listPermissions(principal: principal).get() }
    @available(macOS 12.0, *)
    public func listCredentials() async throws -> [CredentialInfo] { try await listCredentials().get() }
    @available(macOS 12.0, *)
    public func createCredential(name: String, identity: String, secret: String) async throws { _ = try await createCredential(name: name, identity: identity, secret: secret).get() }
    @available(macOS 12.0, *)
    public func alterCredential(name: String, identity: String?, secret: String?) async throws { _ = try await alterCredential(name: name, identity: identity, secret: secret).get() }
    @available(macOS 12.0, *)
    public func dropCredential(name: String) async throws { _ = try await dropCredential(name: name).get() }

    // MARK: - Helpers
    private func run(sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .client(let c): return c.query(sql)
        case .connection(let conn): return conn.query(sql)
        }
    }
    private func exec(sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        switch backing {
        case .client(let c): return c.execute(sql)
        case .connection(let conn): return conn.execute(sql)
        }
    }
    private func futureSucceeded<T>(_ value: T) -> EventLoopFuture<T> {
        switch backing {
        case .client(let c): return c.eventLoopGroup.next().makeSucceededFuture(value)
        case .connection(let conn): return conn.eventLoop.makeSucceededFuture(value)
        }
    }
    private func escapeIdentifier(_ identifier: String) -> String {
        identifier.replacingOccurrences(of: "]", with: "]]" )
    }
    private func escapeLiteral(_ literal: String) -> String {
        literal.replacingOccurrences(of: "'", with: "''")
    }
    private func buildCreateSqlLogin(name: String, password: String, options: LoginOptions) -> String {
        // MUST_CHANGE is a flag associated with the PASSWORD option and should not use = ON/OFF
        // nor be separated from PASSWORD by a comma.
        var sql = "CREATE LOGIN [\(escapeIdentifier(name))] WITH PASSWORD = N'\(escapeLiteral(password))'"
        if let mc = options.mustChange, mc { sql += " MUST_CHANGE" }

        var trailing: [String] = []
        if let defDb = options.defaultDatabase { trailing.append("DEFAULT_DATABASE = [\(escapeIdentifier(defDb))]") }
        if let defLang = options.defaultLanguage { trailing.append("DEFAULT_LANGUAGE = [\(escapeIdentifier(defLang))]") }
        if let cp = options.checkPolicy { trailing.append("CHECK_POLICY = \(cp ? "ON" : "OFF")") }
        if let ce = options.checkExpiration { trailing.append("CHECK_EXPIRATION = \(ce ? "ON" : "OFF")") }
        if let sid = options.sid { trailing.append("SID = 0x\(sid.map { String(format: "%02X", $0) }.joined())") }
        if !trailing.isEmpty { sql += ", \(trailing.joined(separator: ", "))" }
        sql += ";"
        return sql
    }
}
