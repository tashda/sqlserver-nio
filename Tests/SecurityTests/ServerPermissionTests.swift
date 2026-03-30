import XCTest
import SQLServerKit
import SQLServerKitTesting

/// Tests for server-level permissions: listAllServerPermissions, grantRaw/revokeRaw/denyRaw,
/// and the expanded ServerPermissionName enum.
final class ServerPermissionTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private var serverSec: SQLServerServerSecurityClient!
    private var loginsToDrop: [String] = []

    override func setUp() async throws {
        try await super.setUp()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        let config = makeSQLServerClientConfiguration()
        client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        serverSec = SQLServerServerSecurityClient(client: client)
    }

    override func tearDown() async throws {
        for login in loginsToDrop {
            try? await serverSec.dropLogin(name: login)
        }
        loginsToDrop.removeAll()
        try? await client?.shutdownGracefully()
        client = nil
        try await super.tearDown()
    }

    // MARK: - ServerPermissionName enum

    func testServerPermissionNameCaseIterableHasAllCases() {
        let allCases = ServerPermissionName.allCases
        XCTAssertGreaterThanOrEqual(allCases.count, 34, "Expected at least 34 server permission cases")
    }

    func testServerPermissionNameRawValuesAreUnique() {
        let rawValues = ServerPermissionName.allCases.map(\.rawValue)
        let uniqueValues = Set(rawValues)
        XCTAssertEqual(rawValues.count, uniqueValues.count, "ServerPermissionName raw values must be unique")
    }

    func testServerPermissionNameKnownCases() {
        XCTAssertEqual(ServerPermissionName.connectSql.rawValue, "CONNECT SQL")
        XCTAssertEqual(ServerPermissionName.viewServerState.rawValue, "VIEW SERVER STATE")
        XCTAssertEqual(ServerPermissionName.controlServer.rawValue, "CONTROL SERVER")
        XCTAssertEqual(ServerPermissionName.shutdown.rawValue, "SHUTDOWN")
        XCTAssertEqual(ServerPermissionName.alterAnyLogin.rawValue, "ALTER ANY LOGIN")
        XCTAssertEqual(ServerPermissionName.createAnyDatabase.rawValue, "CREATE ANY DATABASE")
        XCTAssertEqual(ServerPermissionName.impersonateAnyLogin.rawValue, "IMPERSONATE ANY LOGIN")
    }

    // MARK: - listAllServerPermissions

    func testListAllServerPermissionsReturnsResults() async throws {
        let permissions = try await serverSec.listAllServerPermissions()
        XCTAssertGreaterThanOrEqual(permissions.count, 30, "SQL Server should have at least 30 server-level permissions")
    }

    func testListAllServerPermissionsContainsKnownPermissions() async throws {
        let permissions = try await serverSec.listAllServerPermissions()
        let knownPermissions = ["CONNECT SQL", "VIEW SERVER STATE", "ALTER ANY LOGIN", "CONTROL SERVER", "SHUTDOWN"]
        for known in knownPermissions {
            XCTAssertTrue(permissions.contains(known), "Expected \(known) in server permissions list")
        }
    }

    func testListAllServerPermissionsResultsAreSorted() async throws {
        let permissions = try await serverSec.listAllServerPermissions()
        let sorted = permissions.sorted()
        XCTAssertEqual(permissions, sorted, "Server permissions should be returned in alphabetical order")
    }

    // MARK: - grantRaw / revokeRaw / denyRaw

    func testGrantRawAndListPermissions() async throws {
        let loginName = "spt_gr_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        try await serverSec.grantRaw(permission: "VIEW SERVER STATE", to: loginName)

        let perms = try await serverSec.listPermissions(principal: loginName)
        XCTAssertTrue(perms.contains { $0.permission == "VIEW SERVER STATE" && $0.state == "GRANT" })
    }

    func testGrantRawWithGrantOption() async throws {
        let loginName = "spt_gwo_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        try await serverSec.grantRaw(permission: "VIEW ANY DATABASE", to: loginName, withGrantOption: true)

        let perms = try await serverSec.listPermissions(principal: loginName)
        XCTAssertTrue(perms.contains { $0.permission == "VIEW ANY DATABASE" && $0.state == "GRANT_WITH_GRANT_OPTION" })
    }

    func testDenyRawPermission() async throws {
        let loginName = "spt_dn_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        try await serverSec.denyRaw(permission: "ALTER ANY DATABASE", to: loginName)

        let perms = try await serverSec.listPermissions(principal: loginName)
        XCTAssertTrue(perms.contains { $0.permission == "ALTER ANY DATABASE" && $0.state == "DENY" })
    }

    func testRevokeRawPermission() async throws {
        let loginName = "spt_rv_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        // Grant then revoke
        try await serverSec.grantRaw(permission: "VIEW SERVER STATE", to: loginName)
        let permsBefore = try await serverSec.listPermissions(principal: loginName)
        XCTAssertTrue(permsBefore.contains { $0.permission == "VIEW SERVER STATE" })

        try await serverSec.revokeRaw(permission: "VIEW SERVER STATE", from: loginName)
        let permsAfter = try await serverSec.listPermissions(principal: loginName)
        XCTAssertFalse(permsAfter.contains { $0.permission == "VIEW SERVER STATE" && $0.state == "GRANT" })
    }

    func testRevokeRawWithCascade() async throws {
        let loginName = "spt_rvc_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        try await serverSec.grantRaw(permission: "VIEW ANY DEFINITION", to: loginName, withGrantOption: true)
        try await serverSec.revokeRaw(permission: "VIEW ANY DEFINITION", from: loginName, cascade: true)

        let perms = try await serverSec.listPermissions(principal: loginName)
        XCTAssertFalse(perms.contains { $0.permission == "VIEW ANY DEFINITION" })
    }

    func testGrantDenyOverridesGrant() async throws {
        let loginName = "spt_od_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        // Grant, then deny the same permission
        try await serverSec.grantRaw(permission: "VIEW SERVER STATE", to: loginName)
        try await serverSec.revokeRaw(permission: "VIEW SERVER STATE", from: loginName)
        try await serverSec.denyRaw(permission: "VIEW SERVER STATE", to: loginName)

        let perms = try await serverSec.listPermissions(principal: loginName)
        XCTAssertTrue(perms.contains { $0.permission == "VIEW SERVER STATE" && $0.state == "DENY" })
        XCTAssertFalse(perms.contains { $0.permission == "VIEW SERVER STATE" && $0.state == "GRANT" })
    }

    // MARK: - Typed enum grant still works

    func testTypedEnumGrantStillWorks() async throws {
        let loginName = "spt_te_\(UUID().uuidString.prefix(8))"
        loginsToDrop.append(loginName)

        try await serverSec.createSqlLogin(name: loginName, password: "T3stP@ss!")

        try await serverSec.grant(permission: .viewServerState, to: loginName)

        let perms = try await serverSec.listPermissions(principal: loginName)
        XCTAssertTrue(perms.contains { $0.permission == "VIEW SERVER STATE" && $0.state == "GRANT" })

        try await serverSec.revoke(permission: .viewServerState, from: loginName)
    }
}
