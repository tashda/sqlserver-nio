import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class DatabaseMailProfileTests: DatabaseMailTestBase {

    // MARK: - Feature Status

    func testIsFeatureEnabled() async throws {
        let enabled = try await mailClient.isFeatureEnabled()
        // setUp enables it, so it should be true
        XCTAssertTrue(enabled)
    }

    // MARK: - Profile CRUD

    func testCreateProfile() async throws {
        let (id, name) = try await createManagedProfile()

        XCTAssertGreaterThan(id, 0, "createProfile should return a positive profile ID")

        let profiles = try await mailClient.listProfiles()
        let created = profiles.first(where: { $0.profileID == id })
        XCTAssertNotNil(created)
        XCTAssertEqual(created?.name, name)
        XCTAssertEqual(created?.description, "Test profile")
    }

    func testCreateProfileWithDescription() async throws {
        let name = "dbmail_prof_desc_\(UUID().uuidString.prefix(8))"
        let desc = "Profile with a longer description for testing"
        let id = try await mailClient.createProfile(name: name, description: desc)
        profilesToDelete.append(id)

        let profiles = try await mailClient.listProfiles()
        let created = profiles.first(where: { $0.profileID == id })
        XCTAssertEqual(created?.description, desc)
    }

    func testCreateProfileWithSpecialCharacters() async throws {
        let name = "dbmail_prof_sp_\(UUID().uuidString.prefix(8))"
        let desc = "O'Brien's \"test\" profile & more <special>"
        let id = try await mailClient.createProfile(name: name, description: desc)
        profilesToDelete.append(id)

        let profiles = try await mailClient.listProfiles()
        let created = profiles.first(where: { $0.profileID == id })
        XCTAssertEqual(created?.description, desc)
    }

    func testUpdateProfile() async throws {
        let (id, _) = try await createManagedProfile()
        let newName = "dbmail_prof_upd_\(UUID().uuidString.prefix(8))"
        let newDesc = "Updated description"

        try await mailClient.updateProfile(profileID: id, name: newName, description: newDesc)

        let profiles = try await mailClient.listProfiles()
        let updated = profiles.first(where: { $0.profileID == id })
        XCTAssertNotNil(updated)
        XCTAssertEqual(updated?.name, newName)
        XCTAssertEqual(updated?.description, newDesc)
    }

    func testDeleteProfile() async throws {
        let name = "dbmail_prof_del_\(UUID().uuidString.prefix(8))"
        let id = try await mailClient.createProfile(name: name)
        // Don't track — we're deleting manually

        let beforeProfiles = try await mailClient.listProfiles()
        XCTAssertTrue(beforeProfiles.contains(where: { $0.profileID == id }))

        try await mailClient.deleteProfile(profileID: id)

        let afterProfiles = try await mailClient.listProfiles()
        XCTAssertFalse(afterProfiles.contains(where: { $0.profileID == id }))
    }

    func testListProfilesReturnsOrdered() async throws {
        let (_, nameA) = try await createManagedProfile(suffix: "aaa_\(UUID().uuidString.prefix(4))")
        let (_, nameZ) = try await createManagedProfile(suffix: "zzz_\(UUID().uuidString.prefix(4))")

        let profiles = try await mailClient.listProfiles()
        let names = profiles.map(\.name)
        guard let indexA = names.firstIndex(of: nameA),
              let indexZ = names.firstIndex(of: nameZ) else {
            XCTFail("Created profiles not found in list")
            return
        }
        XCTAssertLessThan(indexA, indexZ, "Profiles should be ordered by name")
    }

    // MARK: - Profile-Account Association

    func testLinkAccountToProfile() async throws {
        let (profileID, _) = try await createManagedProfile()
        let (accountID, accountName) = try await createManagedAccount()

        try await mailClient.addAccountToProfile(profileID: profileID, accountID: accountID, sequenceNumber: 1)

        let links = try await mailClient.listProfileAccounts(profileID: profileID)
        XCTAssertEqual(links.count, 1)
        XCTAssertEqual(links.first?.accountID, accountID)
        XCTAssertEqual(links.first?.accountName, accountName)
        XCTAssertEqual(links.first?.sequenceNumber, 1)
    }

    func testLinkMultipleAccountsWithFailoverSequence() async throws {
        let (profileID, _) = try await createManagedProfile()
        let (accountID1, _) = try await createManagedAccount(suffix: "seq1_\(UUID().uuidString.prefix(4))")
        let (accountID2, _) = try await createManagedAccount(suffix: "seq2_\(UUID().uuidString.prefix(4))")

        try await mailClient.addAccountToProfile(profileID: profileID, accountID: accountID1, sequenceNumber: 1)
        try await mailClient.addAccountToProfile(profileID: profileID, accountID: accountID2, sequenceNumber: 2)

        let links = try await mailClient.listProfileAccounts(profileID: profileID)
        XCTAssertEqual(links.count, 2)

        let sorted = links.sorted(by: { $0.sequenceNumber < $1.sequenceNumber })
        XCTAssertEqual(sorted[0].accountID, accountID1)
        XCTAssertEqual(sorted[0].sequenceNumber, 1)
        XCTAssertEqual(sorted[1].accountID, accountID2)
        XCTAssertEqual(sorted[1].sequenceNumber, 2)
    }

    func testUnlinkAccountFromProfile() async throws {
        let (profileID, _) = try await createManagedProfile()
        let (accountID, _) = try await createManagedAccount()

        try await mailClient.addAccountToProfile(profileID: profileID, accountID: accountID, sequenceNumber: 1)

        let linksBefore = try await mailClient.listProfileAccounts(profileID: profileID)
        XCTAssertEqual(linksBefore.count, 1)

        try await mailClient.removeAccountFromProfile(profileID: profileID, accountID: accountID)

        let linksAfter = try await mailClient.listProfileAccounts(profileID: profileID)
        XCTAssertEqual(linksAfter.count, 0)
    }

    // MARK: - Profile Security

    func testGrantPublicAccess() async throws {
        let (profileID, _) = try await createManagedProfile()

        try await mailClient.grantProfileAccess(profileID: profileID, principalName: "public", isDefault: true)

        let grants = try await mailClient.listPrincipalProfiles(profileID: profileID)
        XCTAssertEqual(grants.count, 1)
        XCTAssertTrue(grants.first?.isDefault ?? false)
    }

    func testRevokePublicAccess() async throws {
        let (profileID, _) = try await createManagedProfile()

        try await mailClient.grantProfileAccess(profileID: profileID, principalName: "public", isDefault: false)

        let grantsBefore = try await mailClient.listPrincipalProfiles(profileID: profileID)
        XCTAssertEqual(grantsBefore.count, 1)

        try await mailClient.revokeProfileAccess(profileID: profileID, principalName: "public")

        let grantsAfter = try await mailClient.listPrincipalProfiles(profileID: profileID)
        XCTAssertEqual(grantsAfter.count, 0)
    }
}
