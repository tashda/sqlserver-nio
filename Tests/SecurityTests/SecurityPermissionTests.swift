import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class SecurityPermissionTests: SecurityTestBase, @unchecked Sendable {
    func testGrantRevokePermission() async throws {
        let userName = "sp_gr_\(UUID().uuidString.prefix(8))"
        let tableName = "sp_gr_t_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        try await securityClient.createUser(name: userName)
        try await createTestTable(name: tableName)

        try await securityClient.grantPermission(permission: .select, on: tableName, to: userName)

        let permissions = try await securityClient.listPermissions(principal: userName, object: tableName)
        XCTAssertTrue(permissions.contains { $0.permission == "SELECT" && $0.state == "GRANT" })

        try await securityClient.revokePermission(permission: .select, on: tableName, from: userName)
        let permissionsAfter = try await securityClient.listPermissions(principal: userName, object: tableName)
        XCTAssertFalse(permissionsAfter.contains { $0.permission == "SELECT" && $0.state == "GRANT" })
    }

    func testDenyPermission() async throws {
        let userName = "sp_dn_\(UUID().uuidString.prefix(8))"
        let tableName = "sp_dn_t_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        try await securityClient.createUser(name: userName)
        try await createTestTable(name: tableName)

        try await securityClient.denyPermission(permission: .insert, on: tableName, to: userName)

        let permissions = try await securityClient.listPermissions(principal: userName, object: tableName)
        XCTAssertTrue(permissions.contains { $0.permission == "INSERT" && $0.state == "DENY" })
    }

    func testListUsers() async throws {
        let u1 = "sp_ls_u1_\(UUID().uuidString.prefix(8))"
        let u2 = "sp_ls_u2_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(u1)
        usersToDrop.append(u2)

        try await securityClient.createUser(name: u1)
        try await securityClient.createUser(name: u2)

        let users = try await securityClient.listUsers()
        let names = users.map { $0.name }
        XCTAssertTrue(names.contains(u1))
        XCTAssertTrue(names.contains(u2))
    }

    func testListRoles() async throws {
        let r1 = "sp_ls_r1_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(r1)
        try await securityClient.createRole(name: r1)

        let roles = try await securityClient.listRoles()
        let names = roles.map { $0.name }
        XCTAssertTrue(names.contains(r1))
        XCTAssertTrue(names.contains("db_owner"))
    }
}
