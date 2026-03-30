import XCTest
import SQLServerKit
import SQLServerKitTesting

class DatabaseMailTestBase: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    var mailClient: SQLServerDatabaseMailClient!

    /// Profile IDs to delete during tearDown.
    var profilesToDelete: [Int] = []
    /// Account IDs to delete during tearDown.
    var accountsToDelete: [Int] = []

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        self.mailClient = client.databaseMail

        // Ensure Database Mail XPs are enabled
        let enabled = try await mailClient.isFeatureEnabled()
        if !enabled {
            try await mailClient.enableFeature()
        }
    }

    override func tearDown() async throws {
        if let mailClient {
            // Remove profile-account links before deleting profiles/accounts
            for profileID in profilesToDelete {
                let links = try? await mailClient.listProfileAccounts(profileID: profileID)
                for link in (links ?? []) {
                    try? await mailClient.removeAccountFromProfile(profileID: link.profileID, accountID: link.accountID)
                }
                // Remove principal-profile grants
                let grants = try? await mailClient.listPrincipalProfiles(profileID: profileID)
                for grant in (grants ?? []) {
                    let name = grant.principalName ?? "public"
                    try? await mailClient.revokeProfileAccess(profileID: grant.profileID, principalName: name)
                }
            }

            for profileID in profilesToDelete.reversed() {
                try? await mailClient.deleteProfile(profileID: profileID)
            }
            profilesToDelete.removeAll()

            for accountID in accountsToDelete.reversed() {
                try? await mailClient.deleteAccount(accountID: accountID)
            }
            accountsToDelete.removeAll()
        }

        try? await client?.shutdownGracefully()
    }

    // MARK: - Helpers

    /// Creates a test profile with a unique name and tracks it for cleanup.
    @discardableResult
    func createManagedProfile(suffix: String = "") async throws -> (id: Int, name: String) {
        let name = "dbmail_prof_\(suffix.isEmpty ? UUID().uuidString.prefix(8) : Substring(suffix))"
        let id = try await mailClient.createProfile(name: name, description: "Test profile")
        profilesToDelete.append(id)
        return (id, name)
    }

    /// Creates a test account with a unique name and tracks it for cleanup.
    @discardableResult
    func createManagedAccount(suffix: String = "") async throws -> (id: Int, name: String) {
        let name = "dbmail_acct_\(suffix.isEmpty ? UUID().uuidString.prefix(8) : Substring(suffix))"
        let config = SQLServerMailAccountConfig(
            accountName: name,
            emailAddress: "\(name)@test.local",
            serverName: "smtp.test.local",
            port: 25
        )
        let id = try await mailClient.createAccount(config)
        accountsToDelete.append(id)
        return (id, name)
    }
}
