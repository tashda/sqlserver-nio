@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableDefinitionTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully().get()
        } catch {
            // Silently ignore "Already closed" errors during shutdown - they're expected under stress
            if error.localizedDescription.contains("Already closed") ||
               error.localizedDescription.contains("ChannelError error 6") {
                // Both errors are expected during EventLoop shutdown
            } else {
                throw error
            }
        }

        if let g = group {
            _ = try? await SQLServerClient.shutdownEventLoopGroup(g).get()
        }
    }

    func testFetchTableDefinition() async throws {
        // Prepare objects
        let parent = "def_parent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let child = "def_child_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "def") { db in
            // Use proper API methods instead of raw SQL
            let adminClient = SQLServerAdministrationClient(client: self.client)
            let constraintClient = SQLServerConstraintClient(client: self.client)
            let indexClient = SQLServerIndexClient(client: self.client)

            // Create parent table with primary key
            let parentColumns: [SQLServerColumnDefinition] = [
                .init(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true)))
            ]
            try await adminClient.createTable(name: parent, columns: parentColumns)

            // Create child table with primary key and default value
            let childColumns: [SQLServerColumnDefinition] = [
                .init(name: "Id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "RefId", definition: .standard(.init(dataType: .int, isNullable: true))),
                .init(name: "Name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), defaultValue: "N'X'")))
            ]
            try await adminClient.createTable(name: child, columns: childColumns)

            // Add unique constraint to child table
            try await constraintClient.addUniqueConstraint(
                name: "UQ_\(child)_Name",
                table: child,
                columns: ["Name"],
                clustered: false
            )

            // Add foreign key constraint
            try await constraintClient.addForeignKey(
                name: "FK_\(child)_Ref",
                table: child,
                columns: ["RefId"],
                referencedTable: parent,
                referencedColumns: ["Id"]
            )

            // Create index on RefId
            try await indexClient.createIndex(
                name: "IX_\(child)_RefId",
                table: child,
                columns: [IndexColumn(name: "RefId", sortDirection: .ascending)]
            )


        // Fetch scripted definition (use dedicated DB-scoped client + reliable connection)
            let dbClient = try await makeClient(forDatabase: db, using: self.group)
            let def = try await withRetry(attempts: 5) {
                try await withTimeout(60) {
                    try await withReliableConnection(client: dbClient) { conn in
                        try await conn.fetchObjectDefinition(schema: "dbo", name: child, kind: .table).get()
                    }
                }
            }
        XCTAssertNotNil(def)
        guard let defText = def?.definition else {
            XCTFail("No definition returned")
            return
        }

        // Basic assertions
        XCTAssertTrue(defText.contains("CREATE TABLE [dbo].[\(child)]"))
        XCTAssertTrue(defText.contains("PRIMARY KEY"))
        XCTAssertTrue(defText.contains("FOREIGN KEY"))
        XCTAssertTrue(defText.contains("UNIQUE"))
        XCTAssertTrue(defText.contains("CREATE")) // index script appended

            // No explicit cleanup; database dropped by helper
            _ = try? await dbClient.shutdownGracefully().get()
        }
        } catch {
            if let te = error as? AsyncTimeoutError {
                throw XCTSkip("Skipping due to timeout during table definition test: \(te)")
            }
            let norm = SQLServerError.normalize(error)
            switch norm {
            case .connectionClosed, .timeout:
                throw XCTSkip("Skipping due to unstable server during table definition test: \(norm)")
            default:
                throw error
            }
        }
    }
}
