import XCTest
import NIO
import SQLServerKit
import SQLServerKitTesting

final class SecurityRoleTests: SecurityTestBase, @unchecked Sendable {
    func testCreateRole() async throws {
        let roleName = "sr_cr_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)

        try await securityClient.createRole(name: roleName)

        let roleExists = try await securityClient.roleExists(name: roleName)
        XCTAssertTrue(roleExists)

        let roles = try await securityClient.listRoles()
        XCTAssertTrue(roles.contains { $0.name == roleName })
    }

    func testAlterRole() async throws {
        let roleName = "sr_al_\(UUID().uuidString.prefix(8))"
        let newRoleName = "sr_al_n_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)
        rolesToDrop.append(newRoleName)

        try await securityClient.createRole(name: roleName)
        try await securityClient.alterRole(name: roleName, newName: newRoleName)

        let oldRoleStillExists = try await securityClient.roleExists(name: roleName)
        XCTAssertFalse(oldRoleStillExists)
        let renamedRoleExists = try await securityClient.roleExists(name: newRoleName)
        XCTAssertTrue(renamedRoleExists)
    }

    func testDropRole() async throws {
        let roleName = "sr_dr_\(UUID().uuidString.prefix(8))"
        try await securityClient.createRole(name: roleName)
        let roleExistsBeforeDrop = try await securityClient.roleExists(name: roleName)
        XCTAssertTrue(roleExistsBeforeDrop)
        try await securityClient.dropRole(name: roleName)
        let roleExistsAfterDrop = try await securityClient.roleExists(name: roleName)
        XCTAssertFalse(roleExistsAfterDrop)
    }

    func testAddUserToRole() async throws {
        let userName = "sr_mem_u_\(UUID().uuidString.prefix(8))"
        let roleName = "sr_mem_r_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        rolesToDrop.append(roleName)

        try await securityClient.createUser(name: userName)
        try await securityClient.createRole(name: roleName)

        try await securityClient.addUserToRole(user: userName, role: roleName)

        let userRoles = try await securityClient.listUserRoles(user: userName)
        XCTAssertTrue(userRoles.contains(roleName))

        let roleMembers = try await securityClient.listRoleMembers(role: roleName)
        XCTAssertTrue(roleMembers.contains(userName))
    }

    func testRemoveUserFromRole() async throws {
        let userName = "sr_rem_u_\(UUID().uuidString.prefix(8))"
        let roleName = "sr_rem_r_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        rolesToDrop.append(roleName)

        try await securityClient.createUser(name: userName)
        try await securityClient.createRole(name: roleName)
        try await securityClient.addUserToRole(user: userName, role: roleName)

        let rolesBeforeRemoval = try await securityClient.listUserRoles(user: userName)
        XCTAssertTrue(rolesBeforeRemoval.contains(roleName))

        try await securityClient.removeUserFromRole(user: userName, role: roleName)
        let rolesAfterRemoval = try await securityClient.listUserRoles(user: userName)
        XCTAssertFalse(rolesAfterRemoval.contains(roleName))
    }

    func testAddUserToDatabaseRole() async throws {
        let userName = "sr_db_u_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        try await securityClient.createUser(name: userName)
        try await securityClient.addUserToDatabaseRole(user: userName, role: .dbDataReader)

        let userRoles = try await securityClient.listUserRoles(user: userName)
        XCTAssertTrue(userRoles.contains("db_datareader"))

        try await securityClient.removeUserFromDatabaseRole(user: userName, role: .dbDataReader)
    }
}
