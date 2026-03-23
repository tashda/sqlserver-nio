import NIO
import SQLServerTDS

extension SQLServerSecurityClient {
    // MARK: - User Management
    
    public func createUser(
        name: String,
        login: String? = nil,
        options: UserOptions = UserOptions()
    ) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
        let promise = loop.makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                if let login {
                    try await self.createUser(name: name, type: .mappedToLogin(login), options: options)
                } else {
                    try await self.createUser(name: name, type: .withoutLogin, options: options)
                }
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
        if let login {
            try await createUser(name: name, type: .mappedToLogin(login), options: options)
        } else {
            try await createUser(name: name, type: .withoutLogin, options: options)
        }
    }

    /// Creates a database user with a specific user type.
    ///
    /// Supports all six SQL Server database user types:
    /// - `mappedToLogin`: `CREATE USER ... FOR LOGIN [login]`
    /// - `withPassword`: `CREATE USER ... WITH PASSWORD = '...'` (contained databases)
    /// - `withoutLogin`: `CREATE USER ... WITHOUT LOGIN`
    /// - `windowsUser`: `CREATE USER ... FOR LOGIN [DOMAIN\user]`
    /// - `mappedToCertificate`: `CREATE USER ... FOR CERTIFICATE [cert]`
    /// - `mappedToAsymmetricKey`: `CREATE USER ... FOR ASYMMETRIC KEY [key]`
    @available(macOS 12.0, *)
    public func createUser(
        name: String,
        type: DatabaseUserType,
        options: UserOptions = UserOptions()
    ) async throws {
        let escapedUserName = Self.escapeIdentifier(name)
        var sql = "CREATE USER \(escapedUserName)"

        switch type {
        case .mappedToLogin(let login), .windowsUser(let login):
            sql += " FOR LOGIN \(Self.escapeIdentifier(login))"
        case .withPassword(let password):
            let escapedPassword = password.replacingOccurrences(of: "'", with: "''")
            sql += " WITH PASSWORD = N'\(escapedPassword)'"
        case .withoutLogin:
            sql += " WITHOUT LOGIN"
        case .mappedToCertificate(let certName):
            sql += " FOR CERTIFICATE \(Self.escapeIdentifier(certName))"
        case .mappedToAsymmetricKey(let keyName):
            sql += " FOR ASYMMETRIC KEY \(Self.escapeIdentifier(keyName))"
        }

        var withClauses: [String] = []

        if let defaultSchema = options.defaultSchema {
            withClauses.append("DEFAULT_SCHEMA = \(Self.escapeIdentifier(defaultSchema))")
        }

        if let defaultLanguage = options.defaultLanguage {
            let escaped = defaultLanguage.replacingOccurrences(of: "'", with: "''")
            withClauses.append("DEFAULT_LANGUAGE = '\(escaped)'")
        }

        if options.allowEncryptedValueModifications {
            withClauses.append("ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = ON")
        }

        if !withClauses.isEmpty {
            // For withPassword, WITH is already present; append with comma
            if case .withPassword = type {
                sql += ", " + withClauses.joined(separator: ", ")
            } else {
                sql += " WITH " + withClauses.joined(separator: ", ")
            }
        }

        _ = try await exec(sql)
    }
    
    internal func dropUser(name: String) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
        let loop = self.eventLoop
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
    
    @available(macOS 12.0, *)
    public func userExists(name: String) async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.database_principals
        WHERE name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND type IN ('S', 'U', 'G', 'C', 'K', 'E')
        """
        let result = try await queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }
    
    @available(macOS 12.0, *)
    public func listUsers() async throws -> [UserInfo] {
        let sql = """
        SELECT
            dp.name,
            dp.principal_id,
            dp.type_desc AS type,
            dp.default_schema_name,
            dp.create_date,
            dp.modify_date,
            0 AS is_disabled,
            sp.name AS login_name,
            dp.authentication_type_desc
        FROM sys.database_principals dp
        LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
        WHERE dp.type IN ('S', 'U', 'G', 'C', 'K', 'E')
        AND dp.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')
        ORDER BY dp.name
        """

        let rows = try await query(sql)
        return rows.map { row in
            let authTypeStr = row.column("authentication_type_desc")?.string
            let authType = authTypeStr.flatMap { DatabaseUserAuthenticationType(rawValue: $0) }
            return UserInfo(
                name: row.column("name")?.string ?? "",
                principalId: row.column("principal_id")?.int ?? 0,
                type: row.column("type")?.string ?? "",
                defaultSchema: row.column("default_schema_name")?.string,
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string,
                isDisabled: (row.column("is_disabled")?.int ?? 0) != 0,
                loginName: row.column("login_name")?.string,
                authenticationType: authType
            )
        }
    }
}
