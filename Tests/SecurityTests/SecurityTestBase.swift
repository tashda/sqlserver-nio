import XCTest
import NIO
@testable import SQLServerKit
import SQLServerKitTesting

class SecurityTestBase: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    var securityClient: SQLServerSecurityClient!
    var adminClient: SQLServerAdministrationClient!
    var usersToDrop: [String] = []
    var rolesToDrop: [String] = []
    var tablesToDrop: [String] = []

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        
        // Ensure Docker is started if requested
        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }
        
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()
        self.securityClient = SQLServerSecurityClient(client: client)
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDown() async throws {
        // Remove users from roles first
        for user in usersToDrop {
            for role in rolesToDrop {
                try? await securityClient.removeUserFromRole(user: user, role: role).get()
            }
        }
        
        for user in usersToDrop {
            try? await securityClient.dropUser(name: user).get()
        }
        usersToDrop.removeAll()
        
        for role in rolesToDrop {
            try? await securityClient.dropRole(name: role).get()
        }
        rolesToDrop.removeAll()
        
        for table in tablesToDrop {
            try? await adminClient.dropTable(name: table)
        }
        tablesToDrop.removeAll()

        try? await self.client?.shutdownGracefully().get()
        try? await self.group?.shutdownGracefully()
        self.group = nil
    }

    func createTestTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        
        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
    }
}
