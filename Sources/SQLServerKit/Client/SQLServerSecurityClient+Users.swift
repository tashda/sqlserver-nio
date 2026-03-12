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
        AND type IN ('S', 'U', 'G')
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
}
