import XCTest
import NIO
@testable import SQLServerKit

final class SQLServerServerSecurityTests: XCTestCase {
    let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    func testListLogins() throws {
        loadEnvFileIfPresent()
        // Gated: server enumeration can require VIEW SERVER STATE
        try requireEnvFlag("TDS_ENABLE_SERVER_SECURITY_TESTS", description: "server security: login enumeration")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let sec = SQLServerServerSecurityClient(connection: conn)
        let logins = try waitForResult(sec.listLogins(), timeout: TIMEOUT, description: "list logins")
        XCTAssertNotNil(logins as [ServerLoginInfo]?)
    }

    func testCredentialCrud() throws {
        loadEnvFileIfPresent()
        // Requires ALTER ANY CREDENTIAL (or sysadmin)
        try requireEnvFlag("TDS_ENABLE_SERVER_SECURITY_TESTS", description: "server security: credential CRUD")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let sec = SQLServerServerSecurityClient(connection: conn)
        let name = "cred_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(10))"
        // Create
        _ = try waitForResult(sec.createCredential(name: name, identity: "test_identity", secret: "s3cret!"), timeout: TIMEOUT, description: "create cred")
        // List contains
        let creds1 = try waitForResult(sec.listCredentials(), timeout: TIMEOUT, description: "list creds")
        XCTAssertTrue(creds1.contains { $0.name == name })
        // Alter identity
        _ = try waitForResult(sec.alterCredential(name: name, identity: "test_identity2", secret: nil), timeout: TIMEOUT, description: "alter cred")
        // Drop
        _ = try waitForResult(sec.dropCredential(name: name), timeout: TIMEOUT, description: "drop cred")
        // List not contains (best-effort; allow eventual consistency)
        let creds2 = try waitForResult(sec.listCredentials(), timeout: TIMEOUT, description: "list creds 2")
        XCTAssertFalse(creds2.contains { $0.name == name })
    }

    func testServerLoginCrudEnableDisablePasswordAndDrop() async throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_SERVER_SECURITY_TESTS", description: "server security: login CRUD + enable/disable + password change")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        let admin = try await connectSQLServer(on: group.next()).get()
        let serverSec = SQLServerServerSecurityClient(connection: admin)
        let pooled = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()

        let login = "tds_srv_login_" + UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(10)
        let pass1 = "P@ssw0rd!1aA"
        let pass2 = "P@ssw0rd!2bB"

        try await withTemporaryDatabase(client: pooled, prefix: "srvsec") { db in
            // Create login
            _ = try await serverSec.createSqlLogin(name: String(login), password: pass1, options: .init(defaultDatabase: db, checkPolicy: false))

            // Map user in that database
            try await withDbConnection(client: pooled, database: db) { conn in
                let dbSec = SQLServerSecurityClient(connection: conn)
                try await dbSec.createUser(name: String(login), login: String(login))
            }

            // Helper to attempt connection as the login
            func connectAsLogin(password: String) async -> Bool {
                var cfg = makeSQLServerClientConfiguration()
                cfg.connection.login.database = db
                cfg.connection.login.authentication = .sqlPassword(username: String(login), password: password)
                do {
                    let c = try await SQLServerClient.connect(configuration: cfg, eventLoopGroupProvider: .shared(group)).get()
                    do {
                        let rows = try await c.query("SELECT 1 AS ok").get()
                        let ok = (rows.first?.column("ok")?.int ?? 0) == 1
                        _ = try? await c.shutdownGracefully().get()
                        return ok
                    } catch {
                        _ = try? await c.shutdownGracefully().get()
                        return false
                    }
                } catch {
                    return false
                }
            }

            // Baseline connect succeeds
            do {
                let ok = await connectAsLogin(password: pass1)
                XCTAssertTrue(ok)
            }

            // Disable prevents connect
            try await serverSec.enableLogin(name: String(login), enabled: false)
            do {
                let ok = await connectAsLogin(password: pass1)
                XCTAssertFalse(ok)
            }

            // Re-enable allows connect
            try await serverSec.enableLogin(name: String(login), enabled: true)
            do {
                let ok = await connectAsLogin(password: pass1)
                XCTAssertTrue(ok)
            }

            // Change password
            try await serverSec.setLoginPassword(name: String(login), newPassword: pass2, oldPassword: pass1)
            do {
                let ok1 = await connectAsLogin(password: pass1)
                let ok2 = await connectAsLogin(password: pass2)
                XCTAssertFalse(ok1)
                XCTAssertTrue(ok2)
            }
            // cleanup created login
            _ = try? await serverSec.dropLogin(name: String(login))
        }
        // shutdown/close resources explicitly at end to avoid async in defer
        _ = try? await admin.close().get()
        _ = try? await pooled.shutdownGracefully().get()
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            group.shutdownGracefully { _ in cont.resume() }
        }
    }

    func testServerRoleCrudAndMembership() async throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_SERVER_SECURITY_TESTS", description: "server security: server role CRUD + membership")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        let admin = try await connectSQLServer(on: group.next()).get()
        let serverSec = SQLServerServerSecurityClient(connection: admin)

        let role = "tds_srvrole_" + UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(10)
        let login = "tds_srvmem_" + UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(10)
        defer { }

        // Create login
        _ = try await serverSec.createSqlLogin(name: String(login), password: "P@ssw0rd!aA1", options: .init(checkPolicy: false))

        // Create custom server role
        _ = try await serverSec.createServerRole(name: role)

        // Add membership
        _ = try await serverSec.addMemberToServerRole(role: role, principal: String(login))
        do {
            let members = try await serverSec.listServerRoleMembers(role: role)
            XCTAssertTrue(members.contains(String(login)))
        }

        // Remove
        _ = try await serverSec.removeMemberFromServerRole(role: role, principal: String(login))
        do {
            let members = try await serverSec.listServerRoleMembers(role: role)
            XCTAssertFalse(members.contains(String(login)))
        }

        // Rename role
        let newRole = role + "_x"
        _ = try await serverSec.alterServerRole(name: role, newName: newRole)
        do {
            let roles = try await serverSec.listServerRoles()
            XCTAssertTrue(roles.contains { $0.name == newRole })
        }
        // cleanup
        _ = try? await serverSec.dropServerRole(name: newRole)
        _ = try? await serverSec.dropLogin(name: String(login))
        _ = try? await admin.close().get()
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            group.shutdownGracefully { _ in cont.resume() }
        }
    }

    func testServerPermissionGrantDenyRevoke() async throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_SERVER_SECURITY_TESTS", description: "server security: grant/deny/revoke server permissions")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        let admin = try await connectSQLServer(on: group.next()).get()
        let serverSec = SQLServerServerSecurityClient(connection: admin)

        let login = "tds_srvperm_" + UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(10)
        defer { }
        _ = try await serverSec.createSqlLogin(name: String(login), password: "P@ssw0rd!aA1", options: .init(checkPolicy: false))

        // Grant
        _ = try await serverSec.grant(permission: .viewServerState, to: String(login))
        do {
            let perms = try await serverSec.listPermissions(principal: String(login))
            XCTAssertTrue(perms.contains { $0.permission == ServerPermissionName.viewServerState.rawValue && $0.state == "GRANT" })
        }
        // Revoke clears
        _ = try await serverSec.revoke(permission: .viewServerState, from: String(login))
        do {
            let perms = try await serverSec.listPermissions(principal: String(login))
            XCTAssertFalse(perms.contains { $0.permission == ServerPermissionName.viewServerState.rawValue })
        }
        // Deny
        _ = try await serverSec.deny(permission: .viewServerState, to: String(login))
        do {
            let perms = try await serverSec.listPermissions(principal: String(login))
            XCTAssertTrue(perms.contains { $0.permission == ServerPermissionName.viewServerState.rawValue && $0.state == "DENY" })
        }
        // Revoke again clears
        _ = try await serverSec.revoke(permission: .viewServerState, from: String(login))
        do {
            let perms = try await serverSec.listPermissions(principal: String(login))
            XCTAssertFalse(perms.contains { $0.permission == ServerPermissionName.viewServerState.rawValue })
        }
        // cleanup
        _ = try? await serverSec.dropLogin(name: String(login))
        _ = try? await admin.close().get()
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            group.shutdownGracefully { _ in cont.resume() }
        }
    }
}
