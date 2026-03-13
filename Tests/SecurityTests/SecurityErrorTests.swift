import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

final class SecurityErrorTests: SecurityTestBase, @unchecked Sendable {
    func testCreateDuplicateUser() async throws {
        let userName = "se_dup_u_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        try await securityClient.createUser(name: userName)
        do {
            try await securityClient.createUser(name: userName)
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateDuplicateRole() async throws {
        let roleName = "se_dup_r_\(UUID().uuidString.prefix(8))"
        rolesToDrop.append(roleName)
        try await securityClient.createRole(name: roleName)
        do {
            try await securityClient.createRole(name: roleName)
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentUser() async throws {
        do {
            try await securityClient.dropUser(name: "no_user")
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testGrantPermissionOnNonExistentObject() async throws {
        let userName = "se_bad_p_u_\(UUID().uuidString.prefix(8))"
        usersToDrop.append(userName)
        try await securityClient.createUser(name: userName)
        do {
            try await securityClient.grantPermission(permission: .select, on: "no_table", to: userName)
            XCTFail("Should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
