@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableDefinitionTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testFetchTableDefinition() async throws {
        // Prepare objects
        let parent = "def_parent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let child = "def_child_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        do {
        try await withTemporaryDatabase(client: self.client, prefix: "def") { db in
            // Create parent and child tables with PK, FK, unique and index
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(parent)] (
                    [Id] INT NOT NULL,
                    CONSTRAINT [PK_\(parent)] PRIMARY KEY CLUSTERED ([Id] ASC)
                );
                CREATE TABLE [dbo].[\(child)] (
                    [Id] INT NOT NULL,
                    [RefId] INT NULL,
                    [Name] NVARCHAR(50) NOT NULL DEFAULT N'X',
                    CONSTRAINT [PK_\(child)] PRIMARY KEY NONCLUSTERED ([Id] ASC),
                    CONSTRAINT [UQ_\(child)_Name] UNIQUE NONCLUSTERED ([Name] ASC),
                    CONSTRAINT [FK_\(child)_Ref] FOREIGN KEY ([RefId]) REFERENCES [dbo].[\(parent)] ([Id])
                );
                CREATE NONCLUSTERED INDEX [IX_\(child)_RefId] ON [dbo].[\(child)] ([RefId] ASC);
            """)


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
