import XCTest
import SQLServerKit
import SQLServerKitTesting

class TriggerTestBase: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    var triggerClient: SQLServerTriggerClient!
    var adminClient: SQLServerAdministrationClient!
    var triggersToDrop: [(name: String, schema: String)] = []
    var tablesToDrop: [String] = []
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        // Ensure Docker is started if requested
        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
        self.triggerClient = SQLServerTriggerClient(client: client)
        self.adminClient = SQLServerAdministrationClient(client: client)
        do { _ = try await withTimeout(10) { try await self.client.query("SELECT 1") } } catch { throw error }
    }

    override func tearDown() async throws {
        for trigger in triggersToDrop {
            try? await triggerClient.dropTrigger(name: trigger.name, schema: trigger.schema).get()
        }
        triggersToDrop.removeAll()

        for table in tablesToDrop {
            try? await adminClient.dropTable(name: table)
        }
        tablesToDrop.removeAll()

        try? await self.client?.shutdownGracefully()
    }

    // MARK: - Helper Methods

    func createTestTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200))))),
            SQLServerColumnDefinition(name: "created_date", definition: .standard(.init(dataType: .datetime2(precision: 3)))),
            SQLServerColumnDefinition(name: "modified_date", definition: .standard(.init(dataType: .datetime2(precision: 3))))
        ]

        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }

    func createAuditTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "audit_id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (1, 1)))),
            SQLServerColumnDefinition(name: "table_name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "operation", definition: .standard(.init(dataType: .nvarchar(length: .length(10))))),
            SQLServerColumnDefinition(name: "record_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "old_values", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: true))),
            SQLServerColumnDefinition(name: "new_values", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: true))),
            SQLServerColumnDefinition(name: "audit_date", definition: .standard(.init(dataType: .datetime2(precision: 3), defaultValue: "GETDATE()")))
        ]

        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }
}
