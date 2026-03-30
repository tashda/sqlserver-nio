import XCTest
import NIO
import SQLServerKit
import SQLServerKitTesting

final class SecurityUserTests: SecurityTestBase, @unchecked Sendable {
    func testCreateUserWithoutLogin() async throws {
        let userName = "su_nl_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        try await securityClient.createUser(name: userName)

        let userExists = try await securityClient.userExists(name: userName)
        XCTAssertTrue(userExists)

        let users = try await securityClient.listUsers()
        XCTAssertTrue(users.contains { $0.name == userName })
    }

    func testCreateUserWithOptions() async throws {
        let userName = "su_op_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)

        let options = UserOptions(defaultSchema: "dbo")
        try await securityClient.createUser(name: userName, options: options)

        let userExistsWithOptions = try await securityClient.userExists(name: userName)
        XCTAssertTrue(userExistsWithOptions)

        let users = try await securityClient.listUsers()
        let createdUser = users.first { $0.name == userName }
        XCTAssertEqual(createdUser?.defaultSchema, "dbo")
    }

    func testAlterUser() async throws {
        let userName = "su_al_\(UUID().uuidString.prefix(8))"
        let newUserName = "su_al_n_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        usersToDrop.append(newUserName)

        try await securityClient.createUser(name: userName)
        try await securityClient.alterUser(name: userName, newName: newUserName)

        let oldUserStillExists = try await securityClient.userExists(name: userName)
        XCTAssertFalse(oldUserStillExists)
        let renamedUserExists = try await securityClient.userExists(name: newUserName)
        XCTAssertTrue(renamedUserExists)
    }

    func testDropUser() async throws {
        let userName = "su_dr_\(UUID().uuidString.prefix(8))"
        try await securityClient.createUser(name: userName)
        let droppedUserExistsBeforeDrop = try await securityClient.userExists(name: userName)
        XCTAssertTrue(droppedUserExistsBeforeDrop)
        try await securityClient.dropUser(name: userName)
        let droppedUserExistsAfterDrop = try await securityClient.userExists(name: userName)
        XCTAssertFalse(droppedUserExistsAfterDrop)
    }
}
