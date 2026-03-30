import Foundation
import NIO
import SQLServerTDS

// MARK: - Server-scoped Security Client

public final class SQLServerServerSecurityClient: @unchecked Sendable {
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

    // MARK: - Login Editor Data

    @available(macOS 12.0, *)
    public func getServerLoginEditorData(name: String?) async throws -> ServerLoginEditorData {
        try await withThrowingTaskGroup(of: LoginEditorSubData.self) { group in
            // Always fetch independent sets
            group.addTask { .allRoles(try await self.listServerRoles()) }
            group.addTask { .allPermissions(try await self.listAllServerPermissions()) }
            
            // List databases
            group.addTask { .availableDatabases(try await self.listDatabases()) }

            if let name = name {
                group.addTask {
                    let logins = try await self.listLogins(includeSystemLogins: true)
                    return .loginInfo(logins.first(where: { $0.name.caseInsensitiveCompare(name) == .orderedSame }))
                }
                group.addTask { .memberOfRoles(try await self.listServerRolesForPrincipal(principal: name)) }
                group.addTask { .loginPermissions(try await self.listPermissions(principal: name)) }
                group.addTask { .mappings(try await self.listLoginDatabaseMappings(login: name)) }
            }

            var loginInfo: ServerLoginInfo?
            var allRoles: [ServerRoleInfo] = []
            var memberOf: [String] = []
            var allPerms: [String] = []
            var loginPerms: [ServerPermissionInfo] = []
            var mappings: [LoginDatabaseMapping] = []
            var dbs: [String] = []

            while let result = try await group.next() {
                switch result {
                case .loginInfo(let v): loginInfo = v
                case .allRoles(let v): allRoles = v
                case .memberOfRoles(let v): memberOf = v
                case .allPermissions(let v): allPerms = v
                case .loginPermissions(let v): loginPerms = v
                case .mappings(let v): mappings = v
                case .availableDatabases(let v): dbs = v
                }
            }

            return ServerLoginEditorData(
                loginInfo: loginInfo,
                allServerRoles: allRoles,
                memberOfRoles: memberOf,
                allServerPermissions: allPerms,
                loginPermissions: loginPerms,
                databaseMappings: mappings,
                availableDatabases: dbs
            )
        }
    }

    private enum LoginEditorSubData {
        case loginInfo(ServerLoginInfo?)
        case allRoles([ServerRoleInfo])
        case memberOfRoles([String])
        case allPermissions([String])
        case loginPermissions([ServerPermissionInfo])
        case mappings([LoginDatabaseMapping])
        case availableDatabases([String])
    }

    // MARK: - Databases

    internal func listDatabases() -> EventLoopFuture<[String]> {
        run(sql: "SELECT name FROM sys.databases WHERE state = 0 AND database_id > 0 ORDER BY name")
            .map { rows in rows.compactMap { $0.column("name")?.string } }
    }

    @available(macOS 12.0, *)
    public func listDatabases() async throws -> [String] {
        try await listDatabases().get()
    }

    // MARK: - Logins
    internal func listLogins(includeDisabled: Bool = true, includeSystemLogins: Bool = false) -> EventLoopFuture<[ServerLoginInfo]> {
        run(sql: {
            var sql = """
            SELECT sp.name,
                   sp.type_desc,
                   sp.is_disabled,
                   sp.default_database_name,
                   sp.default_language_name,
                   sl.is_policy_checked,
                   sl.is_expiration_checked
            FROM sys.server_principals sp
            LEFT JOIN sys.sql_logins sl ON sl.principal_id = sp.principal_id
            WHERE sp.type IN ('S', 'U', 'G', 'C', 'K', 'E')
            """
            if !includeDisabled { sql += " AND sp.is_disabled = 0" }
            if !includeSystemLogins {
                sql += " AND sp.name NOT LIKE '##%##'"
            }
            sql += " ORDER BY sp.name"
            return sql
        }()).map { rows in
            rows.compactMap { row -> ServerLoginInfo? in
                let name = row.column("name")?.string ?? ""
                let typeDesc = row.column("type_desc")?.string ?? ""
                let disabled = (row.column("is_disabled")?.int ?? 0) == 1
                let defDb = row.column("default_database_name")?.string
                let defLang = row.column("default_language_name")?.string
                let policyChecked = row.column("is_policy_checked")?.int.map { $0 == 1 }
                let expirationChecked = row.column("is_expiration_checked")?.int.map { $0 == 1 }
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
                return ServerLoginInfo(name: name, type: type, isDisabled: disabled, defaultDatabase: defDb, defaultLanguage: defLang, isPolicyChecked: policyChecked, isExpirationChecked: expirationChecked)
            }
        }
    }

    internal func currentLoginName() -> EventLoopFuture<String> {
        run(sql: "SELECT SUSER_SNAME() AS name").flatMapThrowing { rows in
            guard let name = rows.first?.column("name")?.string else {
                throw SQLServerError.sqlExecutionError(message: "Could not fetch current login name")
            }
            return name
        }
    }

    @available(macOS 12.0, *)
    public func currentLoginName() async throws -> String {
        try await currentLoginName().get()
    }

    /// Map a login to a database by creating a user in the target database.
    internal func mapLoginToDatabase(login: String, database: String, userName: String? = nil, defaultSchema: String? = nil) -> EventLoopFuture<Void> {
        let user = userName ?? login
        var sql = "USE \(SQLServerSQL.escapeIdentifier(database)); CREATE USER \(SQLServerSQL.escapeIdentifier(user)) FOR LOGIN \(SQLServerSQL.escapeIdentifier(login))"
        if let schema = defaultSchema {
            sql += " WITH DEFAULT_SCHEMA = \(SQLServerSQL.escapeIdentifier(schema))"
        }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    /// List databases a login is mapped to (has a user in).
    internal func listLoginDatabaseMappings(login: String) -> EventLoopFuture<[LoginDatabaseMapping]> {
        let loginLit = SQLServerSQL.escapeLiteral(login)
        // Use a table variable + cursor to iterate online databases.
        // The key fix: declare the cursor and table variable in a single batch,
        // and use SET NOCOUNT ON to prevent row-count messages from interfering
        // with the final SELECT result set.
        let sql = """
        SET NOCOUNT ON;
        DECLARE @results TABLE (db_name sysname, user_name sysname, default_schema sysname NULL);
        DECLARE @sid varbinary(85) = SUSER_SID(N'\(loginLit)');
        DECLARE @db sysname;
        DECLARE db_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT name FROM sys.databases WHERE state = 0 AND database_id > 0;
        OPEN db_cur;
        FETCH NEXT FROM db_cur INTO @db;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                DECLARE @sql nvarchar(max) = N'SELECT @oName = dp.name, @oSchema = dp.default_schema_name
                    FROM ' + QUOTENAME(@db) + N'.sys.database_principals dp
                    WHERE dp.sid = @pSid AND dp.type IN (''S'',''U'',''G'')';
                DECLARE @oName sysname, @oSchema sysname;
                SET @oName = NULL; SET @oSchema = NULL;
                EXEC sp_executesql @sql,
                    N'@pSid varbinary(85), @oName sysname OUTPUT, @oSchema sysname OUTPUT',
                    @pSid = @sid, @oName = @oName OUTPUT, @oSchema = @oSchema OUTPUT;
                IF @oName IS NOT NULL
                    INSERT INTO @results VALUES (@db, @oName, @oSchema);
            END TRY
            BEGIN CATCH
            END CATCH;
            FETCH NEXT FROM db_cur INTO @db;
        END;
        CLOSE db_cur;
        DEALLOCATE db_cur;
        SET NOCOUNT OFF;
        SELECT db_name, user_name, default_schema FROM @results ORDER BY db_name;
        """
        return run(sql: sql).map { rows in
            rows.compactMap { row -> LoginDatabaseMapping? in
                guard let dbName = row.column("db_name")?.string,
                      let userName = row.column("user_name")?.string else { return nil }
                return LoginDatabaseMapping(
                    databaseName: dbName,
                    userName: userName,
                    defaultSchema: row.column("default_schema")?.string
                )
            }
        }
    }

    /// Remove a login's user mapping from a database.
    internal func unmapLoginFromDatabase(login: String, database: String, userName: String? = nil) -> EventLoopFuture<Void> {
        let user = userName ?? login
        let sql = "USE \(SQLServerSQL.escapeIdentifier(database)); DROP USER \(SQLServerSQL.escapeIdentifier(user));"
        return exec(sql: sql).map { _ in () }
    }

    internal func createSqlLogin(name: String, password: String, options: LoginOptions = .init()) -> EventLoopFuture<Void> {
        exec(sql: buildCreateSqlLogin(name: name, password: password, options: options)).map { _ in () }
    }

    internal func createWindowsLogin(name: String) -> EventLoopFuture<Void> {
        let sql = "CREATE LOGIN \(SQLServerSQL.escapeIdentifier(name)) FROM WINDOWS;"
        return exec(sql: sql).map { _ in () }
    }

    // MARK: - Additional login types
    internal func createCertificateLogin(name: String, certificateName: String, defaultDatabase: String? = nil, defaultLanguage: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE LOGIN \(SQLServerSQL.escapeIdentifier(name)) FROM CERTIFICATE \(SQLServerSQL.escapeIdentifier(certificateName))"
        var withs: [String] = []
        if let db = defaultDatabase { withs.append("DEFAULT_DATABASE = \(SQLServerSQL.escapeIdentifier(db))") }
        if let lang = defaultLanguage { withs.append("DEFAULT_LANGUAGE = \(SQLServerSQL.escapeIdentifier(lang))") }
        if !withs.isEmpty { sql += " WITH \(withs.joined(separator: ", "))" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    internal func createAsymmetricKeyLogin(name: String, asymmetricKeyName: String, defaultDatabase: String? = nil, defaultLanguage: String? = nil) -> EventLoopFuture<Void> {
        var sql = "CREATE LOGIN \(SQLServerSQL.escapeIdentifier(name)) FROM ASYMMETRIC KEY \(SQLServerSQL.escapeIdentifier(asymmetricKeyName))"
        var withs: [String] = []
        if let db = defaultDatabase { withs.append("DEFAULT_DATABASE = \(SQLServerSQL.escapeIdentifier(db))") }
        if let lang = defaultLanguage { withs.append("DEFAULT_LANGUAGE = \(SQLServerSQL.escapeIdentifier(lang))") }
        if !withs.isEmpty { sql += " WITH \(withs.joined(separator: ", "))" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    internal func createExternalLogin(name: String) -> EventLoopFuture<Void> {
        // Azure AD / external provider login
        let sql = "CREATE LOGIN \(SQLServerSQL.escapeIdentifier(name)) FROM EXTERNAL PROVIDER;"
        return exec(sql: sql).map { _ in () }
    }

    internal func enableLogin(name: String, enabled: Bool) -> EventLoopFuture<Void> {
        let sql = "ALTER LOGIN \(SQLServerSQL.escapeIdentifier(name)) \(enabled ? "ENABLE" : "DISABLE");"
        return exec(sql: sql).map { _ in () }
    }

    internal func setLoginPassword(name: String, newPassword: String, oldPassword: String? = nil, mustChange: Bool? = nil) -> EventLoopFuture<Void> {
        // Per T-SQL grammar, OLD_PASSWORD and MUST_CHANGE are part of the PASSWORD clause
        // and are not separated by commas. Example:
        // ALTER LOGIN [name] WITH PASSWORD = N'new' OLD_PASSWORD = N'old' MUST_CHANGE;
        var sql = "ALTER LOGIN \(SQLServerSQL.escapeIdentifier(name)) WITH PASSWORD = N'\(SQLServerSQL.escapeLiteral(newPassword))'"
        if let old = oldPassword { sql += " OLD_PASSWORD = N'\(SQLServerSQL.escapeLiteral(old))'" }
        if let mc = mustChange, mc { sql += " MUST_CHANGE" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }

    internal func alterLogin(name: String, options: LoginAlterOptions) -> EventLoopFuture<Void> {
        var parts: [String] = []
        if let db = options.defaultDatabase { parts.append("DEFAULT_DATABASE = \(SQLServerSQL.escapeIdentifier(db))") }
        if let lang = options.defaultLanguage { parts.append("DEFAULT_LANGUAGE = \(SQLServerSQL.escapeIdentifier(lang))") }
        if let cp = options.checkPolicy { parts.append("CHECK_POLICY = \(cp ? "ON" : "OFF")") }
        if let ce = options.checkExpiration { parts.append("CHECK_EXPIRATION = \(ce ? "ON" : "OFF")") }
        guard !parts.isEmpty else { return futureSucceeded(()) }
        let sql = "ALTER LOGIN \(SQLServerSQL.escapeIdentifier(name)) WITH \(parts.joined(separator: ", "));"
        return exec(sql: sql).map { _ in () }
    }

    internal func dropLogin(name: String, dropMappedUsers: Bool = false) -> EventLoopFuture<Void> {
        // Best-effort: optionally drop mapped users across databases (requires high privileges)
        let loginName = SQLServerSQL.escapeLiteral(name)
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
        let sql = pre + "DROP LOGIN \(SQLServerSQL.escapeIdentifier(name));"
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
    internal func listServerRoles() -> EventLoopFuture<[ServerRoleInfo]> {
        run(sql: """
            SELECT name, is_fixed_role = CASE WHEN is_fixed_role = 1 THEN 1 ELSE 0 END
            FROM sys.server_principals WHERE type = 'R' ORDER BY name
        """).map { rows in
            rows.map { row in
                ServerRoleInfo(name: row.column("name")?.string ?? "", isFixed: (row.column("is_fixed_role")?.int ?? 0) == 1)
            }
        }
    }

    internal func createServerRole(name: String) -> EventLoopFuture<Void> {
        exec(sql: "CREATE SERVER ROLE \(SQLServerSQL.escapeIdentifier(name));").map { _ in () }
    }
    internal func alterServerRole(name: String, newName: String?) -> EventLoopFuture<Void> {
        guard let nn = newName, !nn.isEmpty else { return futureSucceeded(()) }
        return exec(sql: "ALTER SERVER ROLE \(SQLServerSQL.escapeIdentifier(name)) WITH NAME = \(SQLServerSQL.escapeIdentifier(nn));").map { _ in () }
    }
    internal func dropServerRole(name: String) -> EventLoopFuture<Void> {
        exec(sql: "DROP SERVER ROLE \(SQLServerSQL.escapeIdentifier(name));").map { _ in () }
    }
    internal func addMemberToServerRole(role: String, principal: String) -> EventLoopFuture<Void> {
        exec(sql: "ALTER SERVER ROLE \(SQLServerSQL.escapeIdentifier(role)) ADD MEMBER \(SQLServerSQL.escapeIdentifier(principal));").map { _ in () }
    }
    internal func removeMemberFromServerRole(role: String, principal: String) -> EventLoopFuture<Void> {
        exec(sql: "ALTER SERVER ROLE \(SQLServerSQL.escapeIdentifier(role)) DROP MEMBER \(SQLServerSQL.escapeIdentifier(principal));").map { _ in () }
    }
    internal func listServerRoleMembers(role: String) -> EventLoopFuture<[String]> {
        run(sql: """
            SELECT spm.name AS member_name
            FROM sys.server_role_members m
            JOIN sys.server_principals spr ON spr.principal_id = m.role_principal_id
            JOIN sys.server_principals spm ON spm.principal_id = m.member_principal_id
            WHERE spr.name = N'\(SQLServerSQL.escapeLiteral(role))'
            ORDER BY spm.name
        """).map { rows in rows.compactMap { $0.column("member_name")?.string } }
    }
    internal func listServerRolesForPrincipal(principal: String) -> EventLoopFuture<[String]> {
        run(sql: """
            SELECT spr.name AS role_name
            FROM sys.server_role_members m
            JOIN sys.server_principals spr ON spr.principal_id = m.role_principal_id
            JOIN sys.server_principals spm ON spm.principal_id = m.member_principal_id
            WHERE spm.name = N'\(SQLServerSQL.escapeLiteral(principal))'
            ORDER BY spr.name
        """).map { rows in rows.compactMap { $0.column("role_name")?.string } }
    }

    // MARK: - Server permissions

    /// Lists all available server-level permissions from the system catalog.
    internal func listAllServerPermissions() -> EventLoopFuture<[String]> {
        let sql = "SELECT permission_name FROM sys.fn_builtin_permissions('SERVER') ORDER BY permission_name"
        return run(sql: sql).map { rows in
            rows.compactMap { $0.column("permission_name")?.string }
        }
    }

    internal func grant(permission: ServerPermissionName, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        grantRaw(permission: permission.rawValue, to: principal, withGrantOption: withGrantOption)
    }
    internal func revoke(permission: ServerPermissionName, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        revokeRaw(permission: permission.rawValue, from: principal, cascade: cascade)
    }
    internal func deny(permission: ServerPermissionName, to principal: String) -> EventLoopFuture<Void> {
        denyRaw(permission: permission.rawValue, to: principal)
    }

    internal func grantRaw(permission: String, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        var sql = "GRANT \(permission) TO \(SQLServerSQL.escapeIdentifier(principal))"
        if withGrantOption { sql += " WITH GRANT OPTION" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }
    internal func revokeRaw(permission: String, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        var sql = "REVOKE \(permission) FROM \(SQLServerSQL.escapeIdentifier(principal))"
        if cascade { sql += " CASCADE" }
        sql += ";"
        return exec(sql: sql).map { _ in () }
    }
    internal func denyRaw(permission: String, to principal: String) -> EventLoopFuture<Void> {
        let sql = "DENY \(permission) TO \(SQLServerSQL.escapeIdentifier(principal));"
        return exec(sql: sql).map { _ in () }
    }
    public struct ServerPermissionInfo: Sendable {
        public let permission: String
        public let state: String
        public let principalName: String
        public let grantor: String?
    }
    internal func listPermissions(principal: String? = nil) -> EventLoopFuture<[ServerPermissionInfo]> {
        var sql = """
        SELECT p.permission_name, p.state_desc, grantee.name AS principal_name, grantor.name AS grantor_name
        FROM sys.server_permissions p
        JOIN sys.server_principals grantee ON grantee.principal_id = p.grantee_principal_id
        LEFT JOIN sys.server_principals grantor ON grantor.principal_id = p.grantor_principal_id
        WHERE 1=1
        """
        if let pr = principal { sql += " AND grantee.name = N'\(SQLServerSQL.escapeLiteral(pr))'" }
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
    internal func listCredentials() -> EventLoopFuture<[CredentialInfo]> {
        run(sql: "SELECT name, credential_identity FROM sys.credentials ORDER BY name").map { rows in
            rows.map { r in CredentialInfo(name: r.column("name")?.string ?? "", identity: r.column("credential_identity")?.string) }
        }
    }
    internal func createCredential(name: String, identity: String, secret: String) -> EventLoopFuture<Void> {
        let sql = "CREATE CREDENTIAL \(SQLServerSQL.escapeIdentifier(name)) WITH IDENTITY = N'\(SQLServerSQL.escapeLiteral(identity))', SECRET = N'\(SQLServerSQL.escapeLiteral(secret))';"
        return exec(sql: sql).map { _ in () }
    }
    internal func alterCredential(name: String, identity: String?, secret: String?) -> EventLoopFuture<Void> {
        var parts: [String] = []
        if let id = identity { parts.append("IDENTITY = N'\(SQLServerSQL.escapeLiteral(id))'") }
        if let s = secret { parts.append("SECRET = N'\(SQLServerSQL.escapeLiteral(s))'") }
        guard !parts.isEmpty else { return futureSucceeded(()) }
        let sql = "ALTER CREDENTIAL \(SQLServerSQL.escapeIdentifier(name)) WITH \(parts.joined(separator: ", "));"
        return exec(sql: sql).map { _ in () }
    }
    internal func dropCredential(name: String) -> EventLoopFuture<Void> {
        exec(sql: "DROP CREDENTIAL \(SQLServerSQL.escapeIdentifier(name));").map { _ in () }
    }

    /// List all database roles in a specific database, with membership status for a given user.
    internal func listDatabaseRolesForUser(database: String, userName: String) -> EventLoopFuture<[DatabaseUserRoleMembership]> {
        let userLit = SQLServerSQL.escapeLiteral(userName)
        let sql = """
        USE \(SQLServerSQL.escapeIdentifier(database));
        SELECT r.name AS role_name,
               CASE WHEN rm.member_principal_id IS NOT NULL THEN 1 ELSE 0 END AS is_member
        FROM sys.database_principals r
        LEFT JOIN sys.database_role_members rm
            ON rm.role_principal_id = r.principal_id
            AND rm.member_principal_id = (SELECT principal_id FROM sys.database_principals WHERE name = N'\(userLit)')
        WHERE r.type = 'R'
        ORDER BY r.name;
        """
        return run(sql: sql).map { rows in
            rows.compactMap { row -> DatabaseUserRoleMembership? in
                guard let roleName = row.column("role_name")?.string else { return nil }
                let isMember = row.column("is_member")?.int == 1
                return DatabaseUserRoleMembership(roleName: roleName, isMember: isMember)
            }
        }
    }

    /// Add a user to a database role.
    internal func addUserToDatabaseRole(database: String, userName: String, role: String) -> EventLoopFuture<Void> {
        let sql = "USE \(SQLServerSQL.escapeIdentifier(database)); ALTER ROLE \(SQLServerSQL.escapeIdentifier(role)) ADD MEMBER \(SQLServerSQL.escapeIdentifier(userName));"
        return exec(sql: sql).map { _ in () }
    }

    /// Remove a user from a database role.
    internal func removeUserFromDatabaseRole(database: String, userName: String, role: String) -> EventLoopFuture<Void> {
        let sql = "USE \(SQLServerSQL.escapeIdentifier(database)); ALTER ROLE \(SQLServerSQL.escapeIdentifier(role)) DROP MEMBER \(SQLServerSQL.escapeIdentifier(userName));"
        return exec(sql: sql).map { _ in () }
    }

    // MARK: - Role Membership Check

    /// Returns `true` if the current login is a member of the specified server role.
    @available(macOS 12.0, *)
    public func isMemberOf(role: String) async throws -> Bool {
        let escapedRole = SQLServerSQL.escapeLiteral(role)
        let sql = "SELECT CASE WHEN IS_SRVROLEMEMBER(N'\(escapedRole)') = 1 THEN 1 ELSE 0 END AS is_member"
        let rows = try await run(sql: sql).get()
        return rows.first?.column("is_member")?.int == 1
    }

    // MARK: - Async convenience
    @available(macOS 12.0, *)
    public func listLogins(includeDisabled: Bool = true, includeSystemLogins: Bool = false) async throws -> [ServerLoginInfo] { try await listLogins(includeDisabled: includeDisabled, includeSystemLogins: includeSystemLogins).get() }
    @available(macOS 12.0, *)
    public func mapLoginToDatabase(login: String, database: String, userName: String? = nil, defaultSchema: String? = nil) async throws { _ = try await mapLoginToDatabase(login: login, database: database, userName: userName, defaultSchema: defaultSchema).get() }
    @available(macOS 12.0, *)
    public func listLoginDatabaseMappings(login: String) async throws -> [LoginDatabaseMapping] { try await listLoginDatabaseMappings(login: login).get() }
    @available(macOS 12.0, *)
    public func unmapLoginFromDatabase(login: String, database: String, userName: String? = nil) async throws { _ = try await unmapLoginFromDatabase(login: login, database: database, userName: userName).get() }
    @available(macOS 12.0, *)
    public func createSqlLogin(name: String, password: String, options: LoginOptions = .init()) async throws { _ = try await createSqlLogin(name: name, password: password, options: options).get() }
    @available(macOS 12.0, *)
    public func createWindowsLogin(name: String) async throws { _ = try await createWindowsLogin(name: name).get() }
    @available(macOS 12.0, *)
    public func createCertificateLogin(name: String, certificateName: String, defaultDatabase: String? = nil, defaultLanguage: String? = nil) async throws { _ = try await createCertificateLogin(name: name, certificateName: certificateName, defaultDatabase: defaultDatabase, defaultLanguage: defaultLanguage).get() }
    @available(macOS 12.0, *)
    public func createAsymmetricKeyLogin(name: String, asymmetricKeyName: String, defaultDatabase: String? = nil, defaultLanguage: String? = nil) async throws { _ = try await createAsymmetricKeyLogin(name: name, asymmetricKeyName: asymmetricKeyName, defaultDatabase: defaultDatabase, defaultLanguage: defaultLanguage).get() }
    @available(macOS 12.0, *)
    public func createExternalLogin(name: String) async throws { _ = try await createExternalLogin(name: name).get() }
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
    public func listAllServerPermissions() async throws -> [String] { try await listAllServerPermissions().get() }
    @available(macOS 12.0, *)
    public func grant(permission: ServerPermissionName, to principal: String, withGrantOption: Bool = false) async throws { _ = try await grant(permission: permission, to: principal, withGrantOption: withGrantOption).get() }
    @available(macOS 12.0, *)
    public func revoke(permission: ServerPermissionName, from principal: String, cascade: Bool = false) async throws { _ = try await revoke(permission: permission, from: principal, cascade: cascade).get() }
    @available(macOS 12.0, *)
    public func deny(permission: ServerPermissionName, to principal: String) async throws { _ = try await deny(permission: permission, to: principal).get() }
    @available(macOS 12.0, *)
    public func grantRaw(permission: String, to principal: String, withGrantOption: Bool = false) async throws { _ = try await grantRaw(permission: permission, to: principal, withGrantOption: withGrantOption).get() }
    @available(macOS 12.0, *)
    public func revokeRaw(permission: String, from principal: String, cascade: Bool = false) async throws { _ = try await revokeRaw(permission: permission, from: principal, cascade: cascade).get() }
    @available(macOS 12.0, *)
    public func denyRaw(permission: String, to principal: String) async throws { _ = try await denyRaw(permission: permission, to: principal).get() }
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
    @available(macOS 12.0, *)
    public func listDatabaseRolesForUser(database: String, userName: String) async throws -> [DatabaseUserRoleMembership] { try await listDatabaseRolesForUser(database: database, userName: userName).get() }
    @available(macOS 12.0, *)
    public func addUserToDatabaseRole(database: String, userName: String, role: String) async throws { _ = try await addUserToDatabaseRole(database: database, userName: userName, role: role).get() }
    @available(macOS 12.0, *)
    public func removeUserFromDatabaseRole(database: String, userName: String, role: String) async throws { _ = try await removeUserFromDatabaseRole(database: database, userName: userName, role: role).get() }

    // MARK: - Helpers
    private func run(sql: String) -> EventLoopFuture<[SQLServerRow]> {
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
    private func futureSucceeded<T: Sendable>(_ value: T) -> EventLoopFuture<T> {
        switch backing {
        case .client(let c): return c.eventLoopGroup.next().makeSucceededFuture(value)
        case .connection(let conn): return conn.eventLoop.makeSucceededFuture(value)
        }
    }
    private func buildCreateSqlLogin(name: String, password: String, options: LoginOptions) -> String {
        // MUST_CHANGE is a flag associated with the PASSWORD option and should not use = ON/OFF
        // nor be separated from PASSWORD by a comma.
        var sql = "CREATE LOGIN \(SQLServerSQL.escapeIdentifier(name)) WITH PASSWORD = N'\(SQLServerSQL.escapeLiteral(password))'"
        if let mc = options.mustChange, mc { sql += " MUST_CHANGE" }

        var trailing: [String] = []
        if let defDb = options.defaultDatabase { trailing.append("DEFAULT_DATABASE = \(SQLServerSQL.escapeIdentifier(defDb))") }
        if let defLang = options.defaultLanguage { trailing.append("DEFAULT_LANGUAGE = \(SQLServerSQL.escapeIdentifier(defLang))") }
        if let cp = options.checkPolicy { trailing.append("CHECK_POLICY = \(cp ? "ON" : "OFF")") }
        if let ce = options.checkExpiration { trailing.append("CHECK_EXPIRATION = \(ce ? "ON" : "OFF")") }
        if let sid = options.sid { trailing.append("SID = 0x\(sid.map { String(format: "%02X", $0) }.joined())") }
        if !trailing.isEmpty { sql += ", \(trailing.joined(separator: ", "))" }
        sql += ";"
        return sql
    }
}
