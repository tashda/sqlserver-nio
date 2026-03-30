import Foundation
import SQLServerKit
import SQLServerKitTesting
import XCTest

final class AlertEnableTests: AgentTestBase, @unchecked Sendable {
    var managedAlertNames: [String] = []

    override func tearDown() async throws {
        // Clean up managed alerts before base tearDown
        if let client = client {
            let agent = SQLServerAgentOperations(client: client)
            for alertName in managedAlertNames.reversed() {
                try? await agent.deleteAlert(name: alertName)
            }
            managedAlertNames.removeAll()
        }

        try await super.tearDown()
    }

    // MARK: - Alert Enable / Disable Lifecycle

    func testCreateEnableDisableDeleteAlert() async throws {
        let agent = SQLServerAgentOperations(client: self.client)
        let alertName = "alert_enable_test_\(UUID().uuidString.prefix(8))"
        managedAlertNames.append(alertName)

        // Create alert with severity (must be between 1-25)
        try await withTimeout(operationTimeout) {
            try await agent.createAlert(name: alertName, severity: 17, enabled: true)
        }

        // Verify alert was created and is enabled
        let alertsAfterCreate = try await withTimeout(operationTimeout) {
            try await agent.listAlerts()
        }
        let createdAlert = alertsAfterCreate.first(where: { $0.name == alertName })
        XCTAssertNotNil(createdAlert, "Created alert should appear in listAlerts")
        XCTAssertTrue(createdAlert?.enabled ?? false, "Alert should be enabled after creation")

        // Disable the alert
        try await withTimeout(operationTimeout) {
            try await agent.enableAlert(name: alertName, enabled: false)
        }

        // Verify alert is now disabled
        let alertsAfterDisable = try await withTimeout(operationTimeout) {
            try await agent.listAlerts()
        }
        let disabledAlert = alertsAfterDisable.first(where: { $0.name == alertName })
        XCTAssertNotNil(disabledAlert, "Alert should still exist after disabling")
        XCTAssertFalse(disabledAlert?.enabled ?? true, "Alert should be disabled")

        // Re-enable the alert
        try await withTimeout(operationTimeout) {
            try await agent.enableAlert(name: alertName, enabled: true)
        }

        // Verify alert is enabled again
        let alertsAfterReenable = try await withTimeout(operationTimeout) {
            try await agent.listAlerts()
        }
        let reenabledAlert = alertsAfterReenable.first(where: { $0.name == alertName })
        XCTAssertNotNil(reenabledAlert, "Alert should still exist after re-enabling")
        XCTAssertTrue(reenabledAlert?.enabled ?? false, "Alert should be enabled again")

        // Delete the alert
        try await withTimeout(operationTimeout) {
            try await agent.deleteAlert(name: alertName)
        }
        managedAlertNames.removeAll(where: { $0 == alertName })

        // Verify alert is gone
        let alertsAfterDelete = try await withTimeout(operationTimeout) {
            try await agent.listAlerts()
        }
        let deletedAlert = alertsAfterDelete.first(where: { $0.name == alertName })
        XCTAssertNil(deletedAlert, "Deleted alert should no longer appear in listAlerts")
    }

    // MARK: - Alert Info Type

    func testAlertInfoFields() {
        let alert = SQLServerAgentAlertInfo(
            name: "TestAlert",
            severity: 14,
            messageId: 0,
            enabled: true
        )

        XCTAssertEqual(alert.name, "TestAlert")
        XCTAssertEqual(alert.severity, 14)
        XCTAssertEqual(alert.messageId, 0)
        XCTAssertTrue(alert.enabled)
    }

    func testListAlertsReturnsArray() async throws {
        let agent = SQLServerAgentOperations(client: self.client)

        let alerts = try await withTimeout(operationTimeout) {
            try await agent.listAlerts()
        }

        XCTAssertNotNil(alerts, "listAlerts should return a non-nil array")
        XCTAssertTrue(alerts.count >= 0, "Should return zero or more alerts")
    }
}
