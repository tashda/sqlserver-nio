import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Permissions

    internal func getCurrentPrincipalAgentRoles() -> EventLoopFuture<[String]> {
        let sql = """
        SELECT role_name = r.name
        FROM msdb.sys.database_role_members AS drm
        JOIN msdb.sys.database_principals AS r ON r.principal_id = drm.role_principal_id
        JOIN msdb.sys.database_principals AS u ON u.principal_id = drm.member_principal_id
        WHERE u.sid = SUSER_SID()
          AND r.name IN (N'SQLAgentUserRole', N'SQLAgentReaderRole', N'SQLAgentOperatorRole')
        UNION ALL
        SELECT CASE WHEN IS_SRVROLEMEMBER('sysadmin') = 1 THEN 'sysadmin' END
        WHERE IS_SRVROLEMEMBER('sysadmin') = 1;
        """
        return run(sql).map { rows in
            rows.compactMap { $0.column("role_name")?.string }
        }
    }

    internal func listProxyCredentialPermissions() -> EventLoopFuture<SQLServerAgentPermissionReport> {
        return checkServerPermissionFlags().flatMap { flags in
            self.getCurrentPrincipalAgentRoles().map { roles in
                SQLServerAgentPermissionReport(isSysadmin: flags.isSysadmin, hasAlterAnyCredential: flags.hasAlterAnyCredential, msdbRoles: roles)
            }
        }
    }

    internal func checkServerPermissionFlags() -> EventLoopFuture<(isSysadmin: Bool, hasAlterAnyCredential: Bool)> {
        let sql = "SELECT is_sysadmin = IS_SRVROLEMEMBER('sysadmin'), has_alter_any_credential = HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY CREDENTIAL');"
        return run(sql).map { rows in
            let row = rows.first
            return (isSysadmin: (row?.column("is_sysadmin")?.int ?? 0) != 0,
                    hasAlterAnyCredential: (row?.column("has_alter_any_credential")?.int ?? 0) != 0)
        }
    }

    internal func assertCanManageProxiesAndCredentials() -> EventLoopFuture<Void> {
        return listProxyCredentialPermissions().flatMapThrowing { report in
            let hasOperator = report.msdbRoles.contains { $0.caseInsensitiveCompare("SQLAgentOperatorRole") == .orderedSame }
            guard report.isSysadmin || (report.hasAlterAnyCredential && hasOperator) else {
                throw SQLServerError.invalidArgument("Principal lacks permissions to manage Agent proxies or credentials.")
            }
        }
    }

    @available(macOS 12.0, *)
    public func getCurrentPrincipalAgentRoles() async throws -> [String] {
        let future: EventLoopFuture<[String]> = self.getCurrentPrincipalAgentRoles()
        return try await future.get()
    }

    @available(*, deprecated, renamed: "getCurrentPrincipalAgentRoles()")
    @available(macOS 12.0, *)
    public func fetchCurrentPrincipalAgentRoles() async throws -> [String] {
        try await getCurrentPrincipalAgentRoles()
    }
}
