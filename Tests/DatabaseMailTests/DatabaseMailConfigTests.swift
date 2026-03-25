import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class DatabaseMailConfigTests: DatabaseMailTestBase {

    // MARK: - Status

    func testStatus() async throws {
        let status = try await mailClient.status()
        // Status should be either STARTED or STOPPED — just verify it's a valid response
        XCTAssertFalse(status.statusDescription.isEmpty)
    }

    func testStartAndStop() async throws {
        // Start the service
        try await mailClient.start()
        let startedStatus = try await mailClient.status()
        XCTAssertTrue(startedStatus.isStarted)

        // Stop the service
        try await mailClient.stop()
        let stoppedStatus = try await mailClient.status()
        XCTAssertFalse(stoppedStatus.isStarted)

        // Restart so other tests aren't affected
        try await mailClient.start()
    }

    // MARK: - Configuration Parameters

    func testListConfiguration() async throws {
        let params = try await mailClient.configuration()
        XCTAssertFalse(params.isEmpty, "Configuration should return system parameters")

        // Well-known parameters that should always exist
        let paramNames = params.map(\.name)
        XCTAssertTrue(paramNames.contains("AccountRetryAttempts"))
        XCTAssertTrue(paramNames.contains("AccountRetryDelay"))
        XCTAssertTrue(paramNames.contains("MaxFileSize"))
        XCTAssertTrue(paramNames.contains("ProhibitedExtensions"))
        XCTAssertTrue(paramNames.contains("LoggingLevel"))
    }

    func testSetConfigurationAndRestore() async throws {
        let params = try await mailClient.configuration()
        guard let loggingParam = params.first(where: { $0.name == "LoggingLevel" }) else {
            XCTFail("LoggingLevel parameter not found")
            return
        }
        let originalValue = loggingParam.value

        // Change to extended logging (2)
        let newValue = originalValue == "2" ? "1" : "2"
        try await mailClient.setConfiguration(parameter: "LoggingLevel", value: newValue)

        // Verify the change
        let updatedParams = try await mailClient.configuration()
        let updatedLogging = updatedParams.first(where: { $0.name == "LoggingLevel" })
        XCTAssertEqual(updatedLogging?.value, newValue)

        // Restore original value
        try await mailClient.setConfiguration(parameter: "LoggingLevel", value: originalValue)

        // Verify restore
        let restoredParams = try await mailClient.configuration()
        let restoredLogging = restoredParams.first(where: { $0.name == "LoggingLevel" })
        XCTAssertEqual(restoredLogging?.value, originalValue)
    }

    // MARK: - Mail Queue

    func testMailQueueReturnsWithinLimit() async throws {
        // Queue may be empty on a test server — just verify the call succeeds
        let items = try await mailClient.mailQueue(limit: 10)
        XCTAssertTrue(items.count <= 10)
    }

    func testMailQueueDefaultLimit() async throws {
        let items = try await mailClient.mailQueue()
        XCTAssertTrue(items.count <= 100)
    }

    // MARK: - Send Test Email

    func testSendTestEmail() async throws {
        let (_, profileName) = try await createManagedProfile()
        let (accountID, _) = try await createManagedAccount()

        // Link account to profile so the profile has an SMTP route
        try await mailClient.addAccountToProfile(profileID: profilesToDelete.last!, accountID: accountID, sequenceNumber: 1)

        // Ensure mail is started
        try await mailClient.start()

        // Send test email — this will queue the mail but delivery will fail (fake SMTP)
        // The call should succeed without throwing (the email gets queued)
        try await mailClient.sendTestEmail(
            profileName: profileName,
            recipients: "test@test.local",
            subject: "sqlserver-nio test",
            body: "Automated test email from DatabaseMailConfigTests"
        )

        // Verify the email appeared in the queue
        let items = try await mailClient.mailQueue(limit: 10)
        let testItem = items.first(where: { $0.subject == "sqlserver-nio test" })
        XCTAssertNotNil(testItem, "Test email should appear in the mail queue")
        XCTAssertEqual(testItem?.recipients, "test@test.local")
    }

    // MARK: - Feature Enable/Disable

    func testDisableAndReenableFeature() async throws {
        // Disable
        try await mailClient.disableFeature()
        let disabledCheck = try await mailClient.isFeatureEnabled()
        XCTAssertFalse(disabledCheck)

        // Re-enable (setUp will also do this, but let's be explicit)
        try await mailClient.enableFeature()
        let enabledCheck = try await mailClient.isFeatureEnabled()
        XCTAssertTrue(enabledCheck)
    }

    // MARK: - End-to-End Workflow

    func testFullWorkflow() async throws {
        // 1. Create a profile
        let (profileID, profileName) = try await createManagedProfile(suffix: "wf_\(UUID().uuidString.prefix(4))")

        // 2. Create two accounts
        let (accountID1, _) = try await createManagedAccount(suffix: "wf1_\(UUID().uuidString.prefix(4))")
        let (accountID2, _) = try await createManagedAccount(suffix: "wf2_\(UUID().uuidString.prefix(4))")

        // 3. Link both accounts with failover sequence
        try await mailClient.addAccountToProfile(profileID: profileID, accountID: accountID1, sequenceNumber: 1)
        try await mailClient.addAccountToProfile(profileID: profileID, accountID: accountID2, sequenceNumber: 2)

        // 4. Verify profile-account links
        let links = try await mailClient.listProfileAccounts(profileID: profileID)
        XCTAssertEqual(links.count, 2)

        // 5. Grant public access
        try await mailClient.grantProfileAccess(profileID: profileID, principalName: "public", isDefault: true)

        let grants = try await mailClient.listPrincipalProfiles(profileID: profileID)
        XCTAssertEqual(grants.count, 1)
        XCTAssertTrue(grants.first?.isDefault ?? false)

        // 6. Update profile name
        let newName = "dbmail_prof_wf_upd_\(UUID().uuidString.prefix(4))"
        try await mailClient.updateProfile(profileID: profileID, name: newName, description: "Updated workflow profile")

        let profiles = try await mailClient.listProfiles()
        let updated = profiles.first(where: { $0.profileID == profileID })
        XCTAssertEqual(updated?.name, newName)

        // 7. Remove second account from profile
        try await mailClient.removeAccountFromProfile(profileID: profileID, accountID: accountID2)
        let linksAfter = try await mailClient.listProfileAccounts(profileID: profileID)
        XCTAssertEqual(linksAfter.count, 1)
        XCTAssertEqual(linksAfter.first?.accountID, accountID1)

        // 8. Revoke public access
        try await mailClient.revokeProfileAccess(profileID: profileID, principalName: "public")
        let grantsAfter = try await mailClient.listPrincipalProfiles(profileID: profileID)
        XCTAssertEqual(grantsAfter.count, 0)

        // 9. Verify status is accessible
        let status = try await mailClient.status()
        XCTAssertFalse(status.statusDescription.isEmpty)
    }
}
