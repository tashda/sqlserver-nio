@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTableIndexOptionsTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    func testIndexOptionsAreScripted() async throws {
        let table = "opt_tbl_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let idx = "IX_\(table)_Options"
        try await withTemporaryDatabase(client: self.client, prefix: "idx") { db in
            _ = try await executeInDb(client: self.client, database: db, """
                CREATE TABLE [dbo].[\(table)] (
                    [Id] INT NOT NULL,
                    [Code] NVARCHAR(50) NOT NULL,
                    CONSTRAINT [PK_\(table)] PRIMARY KEY CLUSTERED ([Id])
                );
                CREATE UNIQUE NONCLUSTERED INDEX [\(idx)] ON [dbo].[\(table)] ([Code] ASC)
                WITH (PAD_INDEX = ON, FILLFACTOR = 80, ALLOW_PAGE_LOCKS = OFF, ALLOW_ROW_LOCKS = ON, IGNORE_DUP_KEY = ON);
            """)

            guard let def = try await withRetry({
                try await withTimeout(10, {
                    try await withDbConnection(client: self.client, database: db) { conn in
                        try await conn.fetchObjectDefinition(schema: "dbo", name: table, kind: .table).get()
                    }
                })
            }), let ddl = def.definition else {
                XCTFail("No definition returned")
                return
            }

        XCTAssertTrue(ddl.contains("CREATE UNIQUE NONCLUSTERED INDEX [\(idx)]"))
        XCTAssertTrue(ddl.contains("WITH ("))
        XCTAssertTrue(ddl.contains("PAD_INDEX = ON"))
        XCTAssertTrue(ddl.contains("FILLFACTOR = 80"))
        XCTAssertTrue(ddl.contains("ALLOW_PAGE_LOCKS = OFF"))
        XCTAssertTrue(ddl.contains("ALLOW_ROW_LOCKS = ON"))
        XCTAssertTrue(ddl.contains("IGNORE_DUP_KEY = ON"))

        // Cleanup
            // No explicit cleanup; database is dropped
        }
    }
}
