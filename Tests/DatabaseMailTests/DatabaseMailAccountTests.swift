import XCTest
import SQLServerKit
import SQLServerKitTesting

final class DatabaseMailAccountTests: DatabaseMailTestBase {

    // MARK: - Account CRUD

    func testCreateAccount() async throws {
        let (id, name) = try await createManagedAccount()

        XCTAssertGreaterThan(id, 0, "createAccount should return a positive account ID")

        let accounts = try await mailClient.listAccounts()
        let created = accounts.first(where: { $0.accountID == id })
        XCTAssertNotNil(created)
        XCTAssertEqual(created?.name, name)
        XCTAssertEqual(created?.emailAddress, "\(name)@test.local")
        XCTAssertEqual(created?.serverName, "smtp.test.local")
        XCTAssertEqual(created?.serverPort, 25)
        XCTAssertFalse(created?.enableSSL ?? true)
        XCTAssertFalse(created?.useDefaultCredentials ?? true)
    }

    func testCreateAccountWithAllFields() async throws {
        let name = "dbmail_acct_full_\(UUID().uuidString.prefix(8))"
        let config = SQLServerMailAccountConfig(
            accountName: name,
            emailAddress: "full@test.local",
            displayName: "Full Test Account",
            replyToAddress: "reply@test.local",
            description: "Account with all fields populated",
            serverName: "smtp.full.local",
            port: 587,
            username: "testuser",
            password: "testpass",
            useDefaultCredentials: false,
            enableSSL: true
        )
        let id = try await mailClient.createAccount(config)
        accountsToDelete.append(id)

        let accounts = try await mailClient.listAccounts()
        let created = accounts.first(where: { $0.accountID == id })
        XCTAssertNotNil(created)
        XCTAssertEqual(created?.name, name)
        XCTAssertEqual(created?.emailAddress, "full@test.local")
        XCTAssertEqual(created?.displayName, "Full Test Account")
        XCTAssertEqual(created?.replyToAddress, "reply@test.local")
        XCTAssertEqual(created?.description, "Account with all fields populated")
        XCTAssertEqual(created?.serverName, "smtp.full.local")
        XCTAssertEqual(created?.serverPort, 587)
        XCTAssertTrue(created?.enableSSL ?? false)
    }

    func testCreateAccountWithSpecialCharacters() async throws {
        let name = "dbmail_acct_sp_\(UUID().uuidString.prefix(8))"
        let config = SQLServerMailAccountConfig(
            accountName: name,
            emailAddress: "test+tag@test.local",
            displayName: "O'Brien's Account",
            description: "Test with <special> & \"chars\"",
            serverName: "smtp.test.local"
        )
        let id = try await mailClient.createAccount(config)
        accountsToDelete.append(id)

        let accounts = try await mailClient.listAccounts()
        let created = accounts.first(where: { $0.accountID == id })
        XCTAssertEqual(created?.displayName, "O'Brien's Account")
        XCTAssertEqual(created?.description, "Test with <special> & \"chars\"")
    }

    func testUpdateAccount() async throws {
        let (id, _) = try await createManagedAccount()
        let newName = "dbmail_acct_upd_\(UUID().uuidString.prefix(8))"

        let updatedConfig = SQLServerMailAccountConfig(
            accountName: newName,
            emailAddress: "updated@test.local",
            displayName: "Updated Display",
            replyToAddress: "updated-reply@test.local",
            description: "Updated description",
            serverName: "smtp.updated.local",
            port: 465,
            enableSSL: true
        )
        try await mailClient.updateAccount(accountID: id, updatedConfig)

        let accounts = try await mailClient.listAccounts()
        let updated = accounts.first(where: { $0.accountID == id })
        XCTAssertNotNil(updated)
        XCTAssertEqual(updated?.name, newName)
        XCTAssertEqual(updated?.emailAddress, "updated@test.local")
        XCTAssertEqual(updated?.displayName, "Updated Display")
        XCTAssertEqual(updated?.serverName, "smtp.updated.local")
        XCTAssertEqual(updated?.serverPort, 465)
        XCTAssertTrue(updated?.enableSSL ?? false)
    }

    func testDeleteAccount() async throws {
        let name = "dbmail_acct_del_\(UUID().uuidString.prefix(8))"
        let config = SQLServerMailAccountConfig(
            accountName: name,
            emailAddress: "delete@test.local",
            serverName: "smtp.test.local"
        )
        let id = try await mailClient.createAccount(config)
        // Don't track — we're deleting manually

        let beforeAccounts = try await mailClient.listAccounts()
        XCTAssertTrue(beforeAccounts.contains(where: { $0.accountID == id }))

        try await mailClient.deleteAccount(accountID: id)

        let afterAccounts = try await mailClient.listAccounts()
        XCTAssertFalse(afterAccounts.contains(where: { $0.accountID == id }))
    }

    func testListAccountsReturnsOrdered() async throws {
        let (_, nameA) = try await createManagedAccount(suffix: "aaa_\(UUID().uuidString.prefix(4))")
        let (_, nameZ) = try await createManagedAccount(suffix: "zzz_\(UUID().uuidString.prefix(4))")

        let accounts = try await mailClient.listAccounts()
        let names = accounts.map(\.name)
        guard let indexA = names.firstIndex(of: nameA),
              let indexZ = names.firstIndex(of: nameZ) else {
            XCTFail("Created accounts not found in list")
            return
        }
        XCTAssertLessThan(indexA, indexZ, "Accounts should be ordered by name")
    }

    // MARK: - Account with Windows Authentication

    func testCreateAccountWithWindowsAuth() async throws {
        let name = "dbmail_acct_win_\(UUID().uuidString.prefix(8))"
        let config = SQLServerMailAccountConfig(
            accountName: name,
            emailAddress: "winauth@test.local",
            serverName: "smtp.test.local",
            useDefaultCredentials: true
        )
        let id = try await mailClient.createAccount(config)
        accountsToDelete.append(id)

        let accounts = try await mailClient.listAccounts()
        let created = accounts.first(where: { $0.accountID == id })
        XCTAssertTrue(created?.useDefaultCredentials ?? false)
    }
}
