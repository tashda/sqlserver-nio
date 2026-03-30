import XCTest
import SQLServerKit
import SQLServerKitTesting

final class EffectivePermissionTests: SecurityTestBase, @unchecked Sendable {

    // MARK: - Effective Permissions

    func testListEffectivePermissionsOnServer() async throws {
        let perms = try await securityClient.listEffectivePermissions(on: nil, class: "SERVER")
        XCTAssertFalse(perms.isEmpty, "Should return at least one server-level effective permission")
        // Every entry should have a non-empty permission name
        for perm in perms {
            XCTAssertFalse(perm.permissionName.isEmpty)
        }
    }

    func testListEffectivePermissionsOnDatabase() async throws {
        let perms = try await securityClient.listEffectivePermissions(on: "master", class: "DATABASE")
        XCTAssertFalse(perms.isEmpty, "Should return at least one database-level effective permission")
    }

    // MARK: - Built-in Permissions

    func testListBuiltinPermissionsForServer() async throws {
        let perms = try await securityClient.listBuiltinPermissions(class: "SERVER")
        XCTAssertFalse(perms.isEmpty, "Should return built-in server permissions")
        XCTAssertTrue(perms.contains("CONNECT SQL"), "Should contain CONNECT SQL permission")
    }

    func testListBuiltinPermissionsForDatabase() async throws {
        let perms = try await securityClient.listBuiltinPermissions(class: "DATABASE")
        XCTAssertFalse(perms.isEmpty, "Should return built-in database permissions")
        XCTAssertTrue(perms.contains("CONNECT"), "Should contain CONNECT permission")
    }

    // MARK: - Object Permissions

    func testListObjectPermissions() async throws {
        let tableName = "test_obj_perms_\(Int.random(in: 1000...9999))"
        try await createTestTable(name: tableName)

        let userName = "test_perm_user_\(Int.random(in: 1000...9999))"
        try await securityClient.createUser(name: userName, type: .withoutLogin)
        usersToDrop.append(userName)

        try await securityClient.grantPermission(permission: .select, on: tableName, to: userName)

        let perms = try await securityClient.listObjectPermissions(schema: "dbo", object: tableName)
        XCTAssertFalse(perms.isEmpty, "Should return at least the granted SELECT permission")
        XCTAssertTrue(perms.contains(where: { $0.permission == "SELECT" && $0.principalName == userName }))
    }
}
