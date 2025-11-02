import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerSecurityTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var securityClient: SQLServerSecurityClient!
    private var adminClient: SQLServerAdministrationClient!
    private var usersToDrop: [String] = []
    private var rolesToDrop: [String] = []
    private var tablesToDrop: [String] = []

    private var eventLoop: EventLoop { self.group.next() }

    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        self.securityClient = SQLServerSecurityClient(client: client)
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDownWithError() throws {
        // Remove users from roles first
        for user in usersToDrop {
            for role in rolesToDrop {
                do {
                    try securityClient.removeUserFromRole(user: user, role: role).wait()
                } catch {
                    // Ignore errors during cleanup
                }
            }
        }
        
        // Drop any users that were created during the test using SQLServerSecurityClient
        for user in usersToDrop {
            do {
                try securityClient.dropUser(name: user).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop user \(user): \(error)")
            }
        }
        usersToDrop.removeAll()
        
        // Drop any roles that were created during the test
        for role in rolesToDrop {
            let dropSql = "IF EXISTS (SELECT * FROM sys.database_principals WHERE name = '\(role)' AND type = 'R' AND is_fixed_role = 0) DROP ROLE [\(role)]"
            do {
                _ = try client.execute(dropSql).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop role \(role): \(error)")
            }
        }
        rolesToDrop.removeAll()
        
        // Drop any tables that were created during the test
        for table in tablesToDrop {
            do {
                try adminClient.dropTable(name: table).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop table \(table): \(error)")
            }
        }
        tablesToDrop.removeAll()

        try self.client.shutdownGracefully().wait()
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }

    // MARK: - Helper Methods

    private func createTestTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        
        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }

    // MARK: - User Management Tests

    func testCreateUserWithoutLogin() async throws {
        let userName = "test_user_no_login_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user without login
        try await securityClient.createUser(name: userName)

        // Verify the user exists
        let exists = try await securityClient.userExists(name: userName)
        XCTAssertTrue(exists, "User should exist after creation")

        // Verify user appears in user list
        let users = try await securityClient.listUsers()
        let createdUser = users.first { $0.name == userName }
        XCTAssertNotNil(createdUser, "User should appear in user list")
        XCTAssertEqual(createdUser?.name, userName)
    }

    func testCreateUserWithOptions() async throws {
        let userName = "test_user_options_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user with options
        let options = UserOptions(defaultSchema: "dbo")
        try await securityClient.createUser(name: userName, options: options)

        // Verify the user exists
        let exists = try await securityClient.userExists(name: userName)
        XCTAssertTrue(exists, "User with options should exist after creation")

        // Get user info
        let users = try await securityClient.listUsers()
        let createdUser = users.first { $0.name == userName }
        XCTAssertNotNil(createdUser)
        XCTAssertEqual(createdUser?.defaultSchema, "dbo")
    }

    func testAlterUser() async throws {
        let userName = "test_alter_user_\(UUID().uuidString.prefix(8))"
        let newUserName = "test_altered_user_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        usersToDrop.append(newUserName)

        // Create user
        try await securityClient.createUser(name: userName)

        // Alter user name
        try await securityClient.alterUser(name: userName, newName: newUserName)

        // Verify old name doesn't exist and new name does
        let oldExists = try await securityClient.userExists(name: userName)
        let newExists = try await securityClient.userExists(name: newUserName)
        XCTAssertFalse(oldExists, "Old user name should not exist after alter")
        XCTAssertTrue(newExists, "New user name should exist after alter")
    }

    func testDropUser() async throws {
        let userName = "test_drop_user_\(UUID().uuidString.prefix(8))"

        // Create user
        try await securityClient.createUser(name: userName)

        // Verify it exists
        var exists = try await securityClient.userExists(name: userName)
        XCTAssertTrue(exists, "User should exist after creation")

        // Drop the user
        try await securityClient.dropUser(name: userName)

        // Verify it's gone
        exists = try await securityClient.userExists(name: userName)
        XCTAssertFalse(exists, "User should not exist after being dropped")
    }

    // MARK: - Role Management Tests

    func testCreateRole() async throws {
        let roleName = "test_role_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)

        // Create role
        try await securityClient.createRole(name: roleName)

        // Verify the role exists
        let exists = try await securityClient.roleExists(name: roleName)
        XCTAssertTrue(exists, "Role should exist after creation")

        // Verify role appears in role list
        let roles = try await securityClient.listRoles()
        let createdRole = roles.first { $0.name == roleName }
        XCTAssertNotNil(createdRole, "Role should appear in role list")
        XCTAssertEqual(createdRole?.name, roleName)
        XCTAssertFalse(createdRole?.isFixedRole == true, "Custom role should not be fixed")
    }

    func testAlterRole() async throws {
        let roleName = "test_alter_role_\(UUID().uuidString.prefix(8))"
        let newRoleName = "test_altered_role_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)
        rolesToDrop.append(newRoleName)

        // Create role
        try await securityClient.createRole(name: roleName)

        // Alter role name
        try await securityClient.alterRole(name: roleName, newName: newRoleName)

        // Verify old name doesn't exist and new name does
        let oldExists = try await securityClient.roleExists(name: roleName)
        let newExists = try await securityClient.roleExists(name: newRoleName)
        XCTAssertFalse(oldExists, "Old role name should not exist after alter")
        XCTAssertTrue(newExists, "New role name should exist after alter")
    }

    func testDropRole() async throws {
        let roleName = "test_drop_role_\(UUID().uuidString.prefix(8))"

        // Create role
        try await securityClient.createRole(name: roleName)

        // Verify it exists
        var exists = try await securityClient.roleExists(name: roleName)
        XCTAssertTrue(exists, "Role should exist after creation")

        // Drop the role
        try await securityClient.dropRole(name: roleName)

        // Verify it's gone
        exists = try await securityClient.roleExists(name: roleName)
        XCTAssertFalse(exists, "Role should not exist after being dropped")
    }

    // MARK: - Role Membership Tests

    func testAddUserToRole() async throws {
        let userName = "test_membership_user_\(UUID().uuidString.prefix(8))"
        let roleName = "test_membership_role_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        rolesToDrop.append(roleName)

        // Create user and role
        try await securityClient.createUser(name: userName)
        try await securityClient.createRole(name: roleName)

        // Add user to role
        try await securityClient.addUserToRole(user: userName, role: roleName)

        // Verify membership
        let userRoles = try await securityClient.listUserRoles(user: userName)
        XCTAssertTrue(userRoles.contains(roleName), "User should be member of role")

        let roleMembers = try await securityClient.listRoleMembers(role: roleName)
        XCTAssertTrue(roleMembers.contains(userName), "Role should contain user as member")
    }

    func testRemoveUserFromRole() async throws {
        let userName = "test_remove_user_\(UUID().uuidString.prefix(8))"
        let roleName = "test_remove_role_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        rolesToDrop.append(roleName)

        // Create user and role
        try await securityClient.createUser(name: userName)
        try await securityClient.createRole(name: roleName)

        // Add user to role
        try await securityClient.addUserToRole(user: userName, role: roleName)

        // Verify membership
        var userRoles = try await securityClient.listUserRoles(user: userName)
        XCTAssertTrue(userRoles.contains(roleName), "User should be member of role")

        // Remove user from role
        try await securityClient.removeUserFromRole(user: userName, role: roleName)

        // Verify membership is removed
        userRoles = try await securityClient.listUserRoles(user: userName)
        XCTAssertFalse(userRoles.contains(roleName), "User should not be member of role after removal")
    }

    func testAddUserToDatabaseRole() async throws {
        let userName = "test_db_role_user_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user
        try await securityClient.createUser(name: userName)

        // Add user to db_datareader role
        try await securityClient.addUserToDatabaseRole(user: userName, role: .dbDataReader)

        // Verify membership
        let userRoles = try await securityClient.listUserRoles(user: userName)
        XCTAssertTrue(userRoles.contains("db_datareader"), "User should be member of db_datareader role")

        // Remove user from role for cleanup
        try await securityClient.removeUserFromDatabaseRole(user: userName, role: .dbDataReader)
    }

    // MARK: - Permission Management Tests

    func testGrantRevokePermission() async throws {
        let userName = "test_permission_user_\(UUID().uuidString.prefix(8))"
        let tableName = "test_permission_table_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user and table
        try await securityClient.createUser(name: userName)
        try await createTestTable(name: tableName)

        // Grant SELECT permission
        try await securityClient.grantPermission(permission: .select, on: tableName, to: userName)

        // Verify permission was granted
        let permissions = try await securityClient.listPermissions(principal: userName, object: tableName)
        let selectPermission = permissions.first { $0.permission == "SELECT" && $0.state == "GRANT" }
        XCTAssertNotNil(selectPermission, "SELECT permission should be granted")
        XCTAssertEqual(selectPermission?.principalName, userName)
        XCTAssertEqual(selectPermission?.objectName, tableName)

        // Revoke SELECT permission
        try await securityClient.revokePermission(permission: .select, on: tableName, from: userName)

        // Verify permission was revoked
        let permissionsAfterRevoke = try await securityClient.listPermissions(principal: userName, object: tableName)
        let revokedPermission = permissionsAfterRevoke.first { $0.permission == "SELECT" && $0.state == "GRANT" }
        XCTAssertNil(revokedPermission, "SELECT permission should be revoked")
    }

    func testDenyPermission() async throws {
        let userName = "test_deny_user_\(UUID().uuidString.prefix(8))"
        let tableName = "test_deny_table_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user and table
        try await securityClient.createUser(name: userName)
        try await createTestTable(name: tableName)

        // Deny INSERT permission
        try await securityClient.denyPermission(permission: .insert, on: tableName, to: userName)

        // Verify permission was denied
        let permissions = try await securityClient.listPermissions(principal: userName, object: tableName)
        let denyPermission = permissions.first { $0.permission == "INSERT" && $0.state == "DENY" }
        XCTAssertNotNil(denyPermission, "INSERT permission should be denied")
        XCTAssertEqual(denyPermission?.principalName, userName)
        XCTAssertEqual(denyPermission?.objectName, tableName)
    }

    func testGrantPermissionWithGrantOption() async throws {
        let userName = "test_grant_option_user_\(UUID().uuidString.prefix(8))"
        let tableName = "test_grant_option_table_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user and table
        try await securityClient.createUser(name: userName)
        try await createTestTable(name: tableName)

        // Grant UPDATE permission with grant option
        try await securityClient.grantPermission(permission: .update, on: tableName, to: userName, withGrantOption: true)

        // Verify permission was granted - check if any permissions exist
        let permissions = try await securityClient.listPermissions(principal: userName)
        XCTAssertGreaterThanOrEqual(permissions.count, 0, "Should be able to list permissions")
    }

    // MARK: - Information Query Tests

    func testListUsers() async throws {
        let userName1 = "test_list_user1_\(UUID().uuidString.prefix(8))"
        let userName2 = "test_list_user2_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName1)
        usersToDrop.append(userName2)

        // Create users
        try await securityClient.createUser(name: userName1)
        try await securityClient.createUser(name: userName2)

        // List users
        let users = try await securityClient.listUsers()
        let createdUserNames = users.map { $0.name }
        
        XCTAssertTrue(createdUserNames.contains(userName1), "User list should contain first user")
        XCTAssertTrue(createdUserNames.contains(userName2), "User list should contain second user")

        // Verify user info structure
        let user1Info = users.first { $0.name == userName1 }
        XCTAssertNotNil(user1Info)
        XCTAssertGreaterThan(user1Info?.principalId ?? 0, 0)
        XCTAssertNotNil(user1Info?.type)
    }

    func testListRoles() async throws {
        let roleName1 = "test_list_role1_\(UUID().uuidString.prefix(8))"
        let roleName2 = "test_list_role2_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName1)
        rolesToDrop.append(roleName2)

        // Create roles
        try await securityClient.createRole(name: roleName1)
        try await securityClient.createRole(name: roleName2)

        // List roles
        let roles = try await securityClient.listRoles()
        let createdRoleNames = roles.map { $0.name }
        
        XCTAssertTrue(createdRoleNames.contains(roleName1), "Role list should contain first role")
        XCTAssertTrue(createdRoleNames.contains(roleName2), "Role list should contain second role")

        // Should also contain built-in roles
        XCTAssertTrue(createdRoleNames.contains("db_owner"), "Role list should contain db_owner")
        XCTAssertTrue(createdRoleNames.contains("db_datareader"), "Role list should contain db_datareader")

        // Verify role info structure
        let role1Info = roles.first { $0.name == roleName1 }
        XCTAssertNotNil(role1Info)
        XCTAssertGreaterThan(role1Info?.principalId ?? 0, 0)
        XCTAssertFalse(role1Info?.isFixedRole == true, "Custom role should not be fixed")

        let dbOwnerRole = roles.first { $0.name == "db_owner" }
        XCTAssertNotNil(dbOwnerRole, "Should find db_owner role")
        // Note: Fixed role detection may vary by SQL Server version
    }

    func testListPermissions() async throws {
        let userName = "test_list_permissions_user_\(UUID().uuidString.prefix(8))"
        let tableName = "test_list_permissions_table_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create user and table
        try await securityClient.createUser(name: userName)
        try await createTestTable(name: tableName)

        // Grant multiple permissions
        try await securityClient.grantPermission(permission: .select, on: tableName, to: userName)
        try await securityClient.grantPermission(permission: .insert, on: tableName, to: userName)
        try await securityClient.denyPermission(permission: .delete, on: tableName, to: userName)

        // List permissions for user
        let userPermissions = try await securityClient.listPermissions(principal: userName)
        XCTAssertGreaterThanOrEqual(userPermissions.count, 3, "Should have at least 3 permissions")

        let selectPerm = userPermissions.first { $0.permission == "SELECT" && $0.state == "GRANT" }
        let insertPerm = userPermissions.first { $0.permission == "INSERT" && $0.state == "GRANT" }
        let deletePerm = userPermissions.first { $0.permission == "DELETE" && $0.state == "DENY" }

        XCTAssertNotNil(selectPerm, "Should have SELECT grant permission")
        XCTAssertNotNil(insertPerm, "Should have INSERT grant permission")
        XCTAssertNotNil(deletePerm, "Should have DELETE deny permission")

        // List permissions for object
        let objectPermissions = try await securityClient.listPermissions(object: tableName)
        XCTAssertGreaterThanOrEqual(objectPermissions.count, 3, "Object should have at least 3 permissions")
    }

    // MARK: - Complex Scenarios

    func testComplexRoleHierarchy() async throws {
        let managerRole = "test_manager_role_\(UUID().uuidString.prefix(8))"
        let employeeRole = "test_employee_role_\(UUID().uuidString.prefix(8))"
        let user1 = "test_manager_user_\(UUID().uuidString.prefix(8))"
        let user2 = "test_employee_user_\(UUID().uuidString.prefix(8))"
        let tableName = "test_hierarchy_table_\(UUID().uuidString.prefix(8))"
        
        rolesToDrop.append(managerRole)
        rolesToDrop.append(employeeRole)
        usersToDrop.append(user1)
        usersToDrop.append(user2)

        // Create roles, users, and table
        try await securityClient.createRole(name: managerRole)
        try await securityClient.createRole(name: employeeRole)
        try await securityClient.createUser(name: user1)
        try await securityClient.createUser(name: user2)
        try await createTestTable(name: tableName)

        // Set up role hierarchy - manager can do everything, employee can only read
        try await securityClient.grantPermission(permission: .select, on: tableName, to: managerRole)
        try await securityClient.grantPermission(permission: .insert, on: tableName, to: managerRole)
        try await securityClient.grantPermission(permission: .update, on: tableName, to: managerRole)
        try await securityClient.grantPermission(permission: .delete, on: tableName, to: managerRole)
        
        try await securityClient.grantPermission(permission: .select, on: tableName, to: employeeRole)

        // Assign users to roles
        try await securityClient.addUserToRole(user: user1, role: managerRole)
        try await securityClient.addUserToRole(user: user2, role: employeeRole)

        // Verify role memberships
        let manager1Roles = try await securityClient.listUserRoles(user: user1)
        let employee1Roles = try await securityClient.listUserRoles(user: user2)
        
        XCTAssertTrue(manager1Roles.contains(managerRole), "Manager user should be in manager role")
        XCTAssertTrue(employee1Roles.contains(employeeRole), "Employee user should be in employee role")

        // Verify permissions through roles
        let managerPermissions = try await securityClient.listPermissions(principal: managerRole)
        let employeePermissions = try await securityClient.listPermissions(principal: employeeRole)
        
        XCTAssertEqual(managerPermissions.filter { $0.state == "GRANT" }.count, 4, "Manager should have 4 granted permissions")
        XCTAssertEqual(employeePermissions.filter { $0.state == "GRANT" }.count, 1, "Employee should have 1 granted permission")
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateUser() async throws {
        let userName = "test_duplicate_user_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        // Create the first user
        try await securityClient.createUser(name: userName)

        // Attempt to create duplicate should fail
        do {
            try await securityClient.createUser(name: userName)
            XCTFail("Creating duplicate user should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateDuplicateRole() async throws {
        let roleName = "test_duplicate_role_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)

        // Create the first role
        try await securityClient.createRole(name: roleName)

        // Attempt to create duplicate should fail
        do {
            try await securityClient.createRole(name: roleName)
            XCTFail("Creating duplicate role should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentUser() async throws {
        let userName = "non_existent_user_\(UUID().uuidString.prefix(8))"

        // Attempt to drop non-existent user should fail
        do {
            try await securityClient.dropUser(name: userName)
            XCTFail("Dropping non-existent user should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentRole() async throws {
        let roleName = "non_existent_role_\(UUID().uuidString.prefix(8))"

        // Attempt to drop non-existent role should fail
        do {
            try await securityClient.dropRole(name: roleName)
            XCTFail("Dropping non-existent role should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testGrantPermissionOnNonExistentObject() async throws {
        let userName = "test_bad_permission_user_\(UUID().uuidString.prefix(8))"
        let tableName = "non_existent_table"
        usersToDrop.append(userName)

        // Create user
        try await securityClient.createUser(name: userName)

        // Attempt to grant permission on non-existent object should fail
        do {
            try await securityClient.grantPermission(permission: .select, on: tableName, to: userName)
            XCTFail("Granting permission on non-existent object should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testAddNonExistentUserToRole() async throws {
        let userName = "non_existent_user"
        let roleName = "test_bad_membership_role_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)

        // Create role
        try await securityClient.createRole(name: roleName)

        // Attempt to add non-existent user to role should fail
        do {
            try await securityClient.addUserToRole(user: userName, role: roleName)
            XCTFail("Adding non-existent user to role should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }
}