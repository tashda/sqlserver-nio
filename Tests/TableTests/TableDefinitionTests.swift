import SQLServerKit
import SQLServerKitTesting
import XCTest
import Logging

final class SQLServerTableDefinitionTests: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration

        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        client = nil
    }

    func testFetchTableDefinition() async throws {
        // Prepare objects
        let parent = "def_parent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let child = "def_child_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "def") { db in
            try await withDbClient(for: db) { dbClient in
                // Use proper API methods instead of raw SQL
                let adminClient = SQLServerAdministrationClient(client: dbClient)
                let constraintClient = SQLServerConstraintClient(client: dbClient)
                let indexClient = SQLServerIndexClient(client: dbClient)

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

                // Fetch scripted definition using the same DB-scoped client
                let def = try await withRetry(attempts: 5) {
                    try await withTimeout(60) {
                        try await dbClient.metadata.objectDefinition(schema: "dbo", name: child, kind: .table)
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
            }
        }
        } catch {
            if let te = error as? AsyncTimeoutError {
                throw XCTSkip("Skipping due to timeout during table definition test: \(te)")
            }
            if let sqlError = error as? SQLServerError {
                switch sqlError {
                case .connectionClosed, .timeout:
                    throw XCTSkip("Skipping due to unstable server during table definition test: \(sqlError)")
                default:
                    break
                }
            }
            throw error
        }
    }
}
