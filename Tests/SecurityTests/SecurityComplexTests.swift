import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class SecurityComplexTests: SecurityTestBase, @unchecked Sendable {
    func testComplexRoleHierarchy() async throws {
        let managerRole = "sc_mgr_r_\(UUID().uuidString.prefix(8))"
        let employeeRole = "sc_emp_r_\(UUID().uuidString.prefix(8))"
        let user1 = "sc_mgr_u_\(UUID().uuidString.prefix(8))"
        let user2 = "sc_emp_u_\(UUID().uuidString.prefix(8))"
        let tableName = "sc_hier_t_\(UUID().uuidString.prefix(8))"
        
        rolesToDrop.append(managerRole)
        rolesToDrop.append(employeeRole)
        usersToDrop.append(user1)
        usersToDrop.append(user2)

        try await securityClient.createRole(name: managerRole)
        try await securityClient.createRole(name: employeeRole)
        try await securityClient.createUser(name: user1)
        try await securityClient.createUser(name: user2)
        try await createTestTable(name: tableName)

        try await securityClient.grantPermission(permission: .select, on: tableName, to: managerRole)
        try await securityClient.grantPermission(permission: .insert, on: tableName, to: managerRole)
        try await securityClient.grantPermission(permission: .select, on: tableName, to: employeeRole)

        try await securityClient.addUserToRole(user: user1, role: managerRole)
        try await securityClient.addUserToRole(user: user2, role: employeeRole)

        let user1Roles = try await securityClient.listUserRoles(user: user1)
        XCTAssertTrue(user1Roles.contains(managerRole))
        let user2Roles = try await securityClient.listUserRoles(user: user2)
        XCTAssertTrue(user2Roles.contains(employeeRole))

        let mPerms = try await securityClient.listPermissions(principal: managerRole)
        XCTAssertEqual(mPerms.filter { $0.state == "GRANT" }.count, 2)
    }
}
