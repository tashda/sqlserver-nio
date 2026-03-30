import Foundation
import SQLServerKit
import SQLServerKitTesting
import XCTest

final class ExtendedEventsAlterTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    var managedSessionNames: [String] = []

    let operationTimeout: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)

        _ = try await withTimeout(operationTimeout) {
            try await self.client.query("SELECT 1")
        }
    }

    override func tearDown() async throws {
        // Clean up any managed sessions
        if let client = client {
            for sessionName in managedSessionNames.reversed() {
                try? await client.extendedEvents.dropSession(name: sessionName)
            }
            managedSessionNames.removeAll()
        }

        do {
            try await client?.shutdownGracefully()
        } catch {
            let message = error.localizedDescription
            if !message.contains("Already closed") && !message.contains("ChannelError error 6") {
                throw error
            }
        }
    }

    // MARK: - Add and Drop Event

    func testAddEventAndDropEvent() async throws {
        let sessionName = "xe_alter_test_\(UUID().uuidString.prefix(8))"
        managedSessionNames.append(sessionName)

        // Create a session with a single event
        let config = SQLServerXESessionConfiguration(
            name: sessionName,
            events: [
                SQLServerXESessionConfiguration.EventSpec(eventName: "sqlserver.sql_statement_completed")
            ],
            target: .ringBuffer(maxMemoryKB: 1024),
            maxMemoryKB: 1024,
            startupState: false
        )

        try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.createSession(config)
        }

        // Verify session exists with initial event
        let detailsBefore = try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.sessionDetails(name: sessionName)
        }
        XCTAssertEqual(detailsBefore.sessionName, sessionName)
        XCTAssertTrue(
            detailsBefore.events.contains(where: { $0.eventName == "sql_statement_completed" }),
            "Session should contain the initial sql_statement_completed event"
        )

        // Add a new event (session must be stopped, which it is since startupState is false)
        try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.addEvent(
                sessionName: sessionName,
                eventName: "sql_batch_completed"
            )
        }

        // Verify the event was added
        let detailsAfterAdd = try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.sessionDetails(name: sessionName)
        }
        XCTAssertTrue(
            detailsAfterAdd.events.contains(where: { $0.eventName == "sql_batch_completed" }),
            "Session should contain the added sql_batch_completed event"
        )
        XCTAssertTrue(
            detailsAfterAdd.events.contains(where: { $0.eventName == "sql_statement_completed" }),
            "Session should still contain the original sql_statement_completed event"
        )

        // Drop the added event
        try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.dropEvent(
                sessionName: sessionName,
                eventName: "sql_batch_completed"
            )
        }

        // Verify the event was removed
        let detailsAfterDrop = try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.sessionDetails(name: sessionName)
        }
        XCTAssertFalse(
            detailsAfterDrop.events.contains(where: { $0.eventName == "sql_batch_completed" }),
            "Session should no longer contain the dropped sql_batch_completed event"
        )
        XCTAssertTrue(
            detailsAfterDrop.events.contains(where: { $0.eventName == "sql_statement_completed" }),
            "Session should still contain the original sql_statement_completed event"
        )
    }

    // MARK: - Session Lifecycle

    func testCreateAndDropSession() async throws {
        let sessionName = "xe_lifecycle_test_\(UUID().uuidString.prefix(8))"
        managedSessionNames.append(sessionName)

        let config = SQLServerXESessionConfiguration(
            name: sessionName,
            events: [
                SQLServerXESessionConfiguration.EventSpec(eventName: "sqlserver.error_reported")
            ],
            target: .ringBuffer(maxMemoryKB: 1024),
            maxMemoryKB: 1024,
            startupState: false
        )

        try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.createSession(config)
        }

        // Verify session is listed
        let sessions = try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.listSessions()
        }
        XCTAssertTrue(
            sessions.contains(where: { $0.name == sessionName }),
            "Created session should appear in listSessions"
        )

        // Drop the session
        try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.dropSession(name: sessionName)
        }
        managedSessionNames.removeAll(where: { $0 == sessionName })

        // Verify session is gone
        let afterDrop = try await withTimeout(operationTimeout) {
            try await self.client.extendedEvents.listSessions()
        }
        XCTAssertFalse(
            afterDrop.contains(where: { $0.name == sessionName }),
            "Dropped session should no longer appear in listSessions"
        )
    }
}
