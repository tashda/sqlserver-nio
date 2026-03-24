@testable import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerServerTriggerClientTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), numberOfThreads: 1)
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - Server-Level Triggers

    @available(macOS 12.0, *)
    func testListServerTriggers() async throws {
        // Server triggers may or may not exist — just verify the query runs cleanly
        let triggers = try await client.triggers.listServerTriggers()
        // If there are triggers, verify they have valid structure
        for trigger in triggers {
            XCTAssertFalse(trigger.name.isEmpty, "Trigger name should not be empty")
            XCTAssertFalse(trigger.typeDescription.isEmpty, "Type description should not be empty")
        }
    }

    // MARK: - Database DDL Triggers

    @available(macOS 12.0, *)
    func testDatabaseDDLTriggerRoundTrip() async throws {
        do {
            try await withTemporaryDatabase(client: self.client, prefix: "tmp_ddl") { db in
                let triggerName = "ddl_trg_\(UUID().uuidString.prefix(6))"

                // Create a DDL trigger
                let createSQL = """
                USE [\(db)];
                CREATE TRIGGER [\(triggerName)] ON DATABASE
                FOR CREATE_TABLE
                AS
                BEGIN
                    PRINT 'Table created'
                END
                """
                _ = try await self.client.execute(createSQL)

                // List database DDL triggers
                var triggers = try await self.client.triggers.listDatabaseDDLTriggers(database: db)
                let found = triggers.first(where: { $0.name == triggerName })
                XCTAssertNotNil(found, "Expected to find created DDL trigger")
                XCTAssertFalse(found?.isDisabled ?? true)
                XCTAssertTrue(found?.events.contains("CREATE_TABLE") ?? false)

                // Get definition
                let definition = try await self.client.triggers.getDatabaseDDLTriggerDefinition(name: triggerName, database: db)
                XCTAssertNotNil(definition)
                XCTAssertTrue(definition?.contains("Table created") ?? false)

                // Disable
                try await self.client.triggers.disableDatabaseDDLTrigger(name: triggerName, database: db)
                triggers = try await self.client.triggers.listDatabaseDDLTriggers(database: db)
                XCTAssertTrue(triggers.first(where: { $0.name == triggerName })?.isDisabled ?? false)

                // Enable
                try await self.client.triggers.enableDatabaseDDLTrigger(name: triggerName, database: db)
                triggers = try await self.client.triggers.listDatabaseDDLTriggers(database: db)
                XCTAssertFalse(triggers.first(where: { $0.name == triggerName })?.isDisabled ?? true)

                // Drop
                try await self.client.triggers.dropDatabaseDDLTrigger(name: triggerName, database: db)
                triggers = try await self.client.triggers.listDatabaseDDLTriggers(database: db)
                XCTAssertNil(triggers.first(where: { $0.name == triggerName }))
            }
        } catch let e as SQLServerError {
            if case .connectionClosed = e { throw XCTSkip("Connection closed during DDL trigger test") }
            throw e
        }
    }
}
