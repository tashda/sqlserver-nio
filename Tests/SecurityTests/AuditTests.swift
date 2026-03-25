import XCTest
@testable import SQLServerKit
import SQLServerKitTesting

final class AuditTests: SecurityTestBase, @unchecked Sendable {

    var auditClient: SQLServerAuditClient!

    override func setUp() async throws {
        try await super.setUp()
        auditClient = SQLServerAuditClient(client: client)
    }

    // MARK: - Audit Types

    func testAuditDestinationDisplayNames() {
        XCTAssertEqual(AuditDestination.file.displayName, "File")
        XCTAssertEqual(AuditDestination.securityLog.displayName, "Security Log")
        XCTAssertEqual(AuditDestination.applicationLog.displayName, "Application Log")
        XCTAssertEqual(AuditDestination.externalMonitor.displayName, "External Monitor")
    }

    func testAuditOnFailureDisplayNames() {
        XCTAssertEqual(AuditOnFailure.continueOperation.displayName, "Continue")
        XCTAssertEqual(AuditOnFailure.shutdownServer.displayName, "Shutdown Server")
        XCTAssertEqual(AuditOnFailure.failOperation.displayName, "Fail Operation")
    }

    func testServerAuditInfoIdentifiable() {
        let info = ServerAuditInfo(auditID: 1, name: "TestAudit", isEnabled: false, destination: .file)
        XCTAssertEqual(info.id, "TestAudit")
    }

    func testAuditSpecificationInfoIdentifiable() {
        let info = AuditSpecificationInfo(name: "TestSpec", auditName: "TestAudit", isEnabled: false)
        XCTAssertEqual(info.id, "TestSpec")
    }

    // MARK: - Integration Tests

    func testListServerAudits() async throws {
        let audits = try await auditClient.listServerAudits()
        // Should not throw; may be empty on test instances
        _ = audits
    }

    func testListServerAuditSpecifications() async throws {
        let specs = try await auditClient.listServerAuditSpecifications()
        _ = specs
    }

    func testListDatabaseAuditSpecifications() async throws {
        let specs = try await auditClient.listDatabaseAuditSpecifications()
        _ = specs
    }

    func testListAuditActions() async throws {
        let actions = try await auditClient.listAuditActions()
        XCTAssertFalse(actions.isEmpty, "Should return at least some built-in audit actions")
    }
}
