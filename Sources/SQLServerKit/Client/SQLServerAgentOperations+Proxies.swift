import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Proxy & Credential Management

    internal func createProxy(name: String, credentialName: String, description: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        return assertCanManageProxiesAndCredentials().flatMap { (_: Void) -> EventLoopFuture<[TDSRow]> in
            var sql = "EXEC msdb.dbo.sp_add_proxy @proxy_name = N'\(SQLServerSQL.escapeLiteral(name))', @credential_name = N'\(SQLServerSQL.escapeLiteral(credentialName))', @enabled = \(enabled ? 1 : 0)"
            if let description, !description.isEmpty { sql += ", @description = N'\(SQLServerSQL.escapeLiteral(description))'" }
            sql += ";"
            return self.run(sql)
        }.flatMap { (rows: [TDSRow]) -> EventLoopFuture<[TDSRow]> in
            self.run("SELECT 1 AS present FROM msdb.dbo.sysproxies WHERE name = N'\(SQLServerSQL.escapeLiteral(name))'")
        }.flatMapThrowing { (rows: [TDSRow]) -> Void in
            guard rows.first?.column("present")?.int == 1 else {
                throw SQLServerError.invalidArgument("Proxy not visible after creation: \(name). Ensure credential exists and you have required permissions.")
            }
            return ()
        }
    }

    internal func deleteProxy(name: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_proxy @proxy_name = N'\(SQLServerSQL.escapeLiteral(name))';").map { _ in () }
    }

    internal func grantLoginToProxy(proxyName: String, loginName: String) -> EventLoopFuture<Void> {
        return assertCanManageProxiesAndCredentials().flatMap { (_: Void) -> EventLoopFuture<Void> in
            self.run("EXEC msdb.dbo.sp_grant_login_to_proxy @proxy_name = N'\(SQLServerSQL.escapeLiteral(proxyName))', @login_name = N'\(SQLServerSQL.escapeLiteral(loginName))';").map { _ in () }
        }
    }

    internal func revokeLoginFromProxy(proxyName: String, loginName: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_revoke_login_from_proxy @proxy_name = N'\(SQLServerSQL.escapeLiteral(proxyName))', @login_name = N'\(SQLServerSQL.escapeLiteral(loginName))';").map { _ in () }
    }

    internal func grantProxyToSubsystem(proxyName: String, subsystem: String) -> EventLoopFuture<Void> {
        return assertCanManageProxiesAndCredentials().flatMap { (_: Void) -> EventLoopFuture<Void> in
            self.run("EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name = N'\(SQLServerSQL.escapeLiteral(proxyName))', @subsystem_id = NULL, @subsystem_name = N'\(SQLServerSQL.escapeLiteral(subsystem))';").map { _ in () }
        }
    }

    internal func revokeProxyFromSubsystem(proxyName: String, subsystem: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_revoke_proxy_from_subsystem @proxy_name = N'\(SQLServerSQL.escapeLiteral(proxyName))', @subsystem_name = N'\(SQLServerSQL.escapeLiteral(subsystem))';").map { _ in () }
    }

    internal func listProxySubsystems(proxyName: String) -> EventLoopFuture<[String]> {
        let sql = """
        SELECT s.subsystem_name
        FROM msdb.dbo.sysproxysubsystem AS ps
        JOIN msdb.dbo.sysproxies AS p ON ps.proxy_id = p.proxy_id
        JOIN msdb.dbo.syssubsystems AS s ON ps.subsystem_id = s.subsystem_id
        WHERE p.name = N'\(SQLServerSQL.escapeLiteral(proxyName))'
        ORDER BY s.subsystem_name;
        """
        return run(sql).map { rows in
            rows.compactMap { $0.column("subsystem_name")?.string }
        }
    }

    internal func listProxyLogins(proxyName: String) -> EventLoopFuture<[String]> {
        let sql = """
        SELECT sp.name AS login_name
        FROM msdb.dbo.sysproxylogin AS pl
        JOIN msdb.dbo.sysproxies AS p ON pl.proxy_id = p.proxy_id
        JOIN master.sys.server_principals AS sp ON pl.sid = sp.sid
        WHERE p.name = N'\(SQLServerSQL.escapeLiteral(proxyName))'
        ORDER BY sp.name;
        """
        return run(sql).map { rows in
            rows.compactMap { $0.column("login_name")?.string }
        }
    }

    internal func listProxies() -> EventLoopFuture<[SQLServerAgentProxyInfo]> {
        let sql = """
        SELECT p.name, c.name AS credential_name, p.enabled
        FROM msdb.dbo.sysproxies AS p
        LEFT JOIN master.sys.credentials AS c ON p.credential_id = c.credential_id
        ORDER BY p.name;
        """
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SQLServerAgentProxyInfo(name: name, credentialName: row.column("credential_name")?.string, enabled: (row.column("enabled")?.int ?? 0) != 0)
            }
        }
    }
}
