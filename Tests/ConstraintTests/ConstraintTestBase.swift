import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

class ConstraintTestBase: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    var constraintClient: SQLServerConstraintClient!
    var adminClient: SQLServerAdministrationClient!
    var testDatabase: String!
    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        
        // Ensure Docker is started if requested
        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }
        
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do {
            _ = try await withTimeout(10) { try await self.baseClient.query("SELECT 1").get() }
        } catch {
            throw error
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "cst")
        self.client = try await makeClient(forDatabase: testDatabase, using: group)
        self.adminClient = SQLServerAdministrationClient(client: self.client)
        self.constraintClient = SQLServerConstraintClient(client: self.client)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully().get()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try? await baseClient?.shutdownGracefully().get()
        try? await group?.shutdownGracefully()
        testDatabase = nil
        group = nil
    }

    func createTestTable(name: String, withPrimaryKey: Bool = false) async throws {
        var columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: withPrimaryKey))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200))))),
            SQLServerColumnDefinition(name: "age", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "status", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
        ]

        if !withPrimaryKey {
            columns[0] = SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))
        }

        try await self.adminClient.createTable(name: name, columns: columns)
    }

    func createReferenceTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "category_name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]

        try await self.adminClient.createTable(name: name, columns: columns)
        try await self.client.withConnection { connection in
            try await connection.insertRow(into: name, values: ["id": .int(1), "category_name": .nString("Category A")])
            try await connection.insertRow(into: name, values: ["id": .int(2), "category_name": .nString("Category B")])
            try await connection.insertRow(into: name, values: ["id": .int(3), "category_name": .nString("Category C")])
        }
    }
}
