import NIO
import SQLServerTDS

extension SQLServerSecurityClient {
    // MARK: - Permission Management
    
    public func grantPermission(
        permission: Permission,
        on object: String,
        to principal: String,
        withGrantOption: Bool = false
    ) -> EventLoopFuture<Void> {
        let loop = self.eventLoop
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
        let escapedObject = SQLServerSQL.escapeIdentifier(object)
        let escapedPrincipal = SQLServerSQL.escapeIdentifier(principal)
        
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
        let loop = self.eventLoop
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
        let escapedObject = SQLServerSQL.escapeIdentifier(object)
        let escapedPrincipal = SQLServerSQL.escapeIdentifier(principal)
        
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
        let loop = self.eventLoop
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
        let escapedObject = SQLServerSQL.escapeIdentifier(object)
        let escapedPrincipal = SQLServerSQL.escapeIdentifier(principal)
        
        let sql = "DENY \(permission.rawValue) ON \(escapedObject) TO \(escapedPrincipal)"
        _ = try await exec(sql)
    }

    // MARK: - Securable-aware permissions

    internal func grant(permission: DatabasePermissionName, on securable: Securable, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "GRANT", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: withGrantOption, cascade: nil)
    }
    internal func grant(permission: ObjectPermissionName, on securable: Securable, to principal: String, withGrantOption: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "GRANT", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: withGrantOption, cascade: nil)
    }

    internal func revoke(permission: DatabasePermissionName, on securable: Securable, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "REVOKE", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: cascade)
    }
    internal func revoke(permission: ObjectPermissionName, on securable: Securable, from principal: String, cascade: Bool = false) -> EventLoopFuture<Void> {
        emitPermission(kind: "REVOKE", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: cascade)
    }

    internal func deny(permission: DatabasePermissionName, on securable: Securable, to principal: String) -> EventLoopFuture<Void> {
        emitPermission(kind: "DENY", permission: permission.rawValue, on: securable, principal: principal, withGrantOption: nil, cascade: nil)
    }
    internal func deny(permission: ObjectPermissionName, on securable: Securable, to principal: String) -> EventLoopFuture<Void> {
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
        let loop = self.eventLoop
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
                    targetClause = "SCHEMA::\(SQLServerSQL.escapeIdentifier(schema))"
                case .object(let oid):
                    targetClause = "OBJECT::\(SQLServerSQL.escapeIdentifier(oid.schema)).\(SQLServerSQL.escapeIdentifier(oid.name))"
                case .column(let oid, let columns):
                    // Column list goes after permission token
                    if !columns.isEmpty {
                        let cols = columns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ",")
                        sql += " (\(cols))"
                    }
                    targetClause = "OBJECT::\(SQLServerSQL.escapeIdentifier(oid.schema)).\(SQLServerSQL.escapeIdentifier(oid.name))"
                }
                if let target = targetClause { sql += " ON \(target)" }
                sql += " \(kind == "REVOKE" ? "FROM" : "TO") \(SQLServerSQL.escapeIdentifier(principal))"
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
    
    // MARK: - Effective Permissions (fn_my_permissions)

    @available(macOS 12.0, *)
    public func listEffectivePermissions(on securable: String?, class securableClass: String) async throws -> [EffectivePermissionInfo] {
        let securableLiteral: String
        if let securable {
            securableLiteral = "N'\(securable.replacingOccurrences(of: "'", with: "''"))'"
        } else {
            securableLiteral = "NULL"
        }
        let sql = "SELECT entity_name, subentity_name, permission_name FROM fn_my_permissions(\(securableLiteral), N'\(securableClass.replacingOccurrences(of: "'", with: "''"))')"
        let rows = try await query(sql)
        return rows.map { row in
            EffectivePermissionInfo(
                entityName: row.column("entity_name")?.string,
                subentityName: row.column("subentity_name")?.string,
                permissionName: row.column("permission_name")?.string ?? ""
            )
        }
    }

    @available(macOS 12.0, *)
    public func listBuiltinPermissions(class securableClass: String) async throws -> [String] {
        let sql = "SELECT permission_name FROM fn_builtin_permissions(N'\(securableClass.replacingOccurrences(of: "'", with: "''"))') ORDER BY permission_name"
        let rows = try await query(sql)
        return rows.compactMap { $0.column("permission_name")?.string }
    }

    @available(macOS 12.0, *)
    public func listObjectPermissions(schema: String, object: String) async throws -> [DetailedPermissionInfo] {
        let sql = """
        SELECT
            p.permission_name, p.state_desc, p.class_desc,
            s.name AS schema_name, o.name AS object_name,
            ISNULL(col.name, '') AS column_name,
            grantee.name AS principal_name,
            ISNULL(grantor.name, '') AS grantor_name
        FROM sys.database_permissions AS p
        INNER JOIN sys.objects AS o ON o.object_id = p.major_id
        INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
        INNER JOIN sys.database_principals AS grantee ON grantee.principal_id = p.grantee_principal_id
        LEFT JOIN sys.database_principals AS grantor ON grantor.principal_id = p.grantor_principal_id
        LEFT JOIN sys.columns AS col ON (p.minor_id > 0 AND col.object_id = p.major_id AND col.column_id = p.minor_id)
        WHERE s.name = N'\(schema.replacingOccurrences(of: "'", with: "''"))'
          AND o.name = N'\(object.replacingOccurrences(of: "'", with: "''"))'
          AND p.class = 1
        ORDER BY grantee.name, p.permission_name
        """
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
}
