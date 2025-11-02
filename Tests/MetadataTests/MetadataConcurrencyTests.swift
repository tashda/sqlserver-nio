@testable import SQLServerKit
import NIO
import XCTest

final class SQLServerMetadataConcurrencyTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testConcurrentMetadataCallsShareConnectionSafely() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "mdc") { database in
            let tableA = "tbl_" + UUID().uuidString.prefix(8)
            let tableB = "tbl_" + UUID().uuidString.prefix(8)
            let viewName = "vw_" + UUID().uuidString.prefix(8)

            // Create tables in one batch
            let tablesSQL = """
            CREATE TABLE dbo.[\(tableA)] (
                id INT NOT NULL PRIMARY KEY,
                name NVARCHAR(40) NOT NULL,
                info XML NULL
            );

            CREATE TABLE dbo.[\(tableB)] (
                id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                ref INT NOT NULL,
                created DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            );
            """
            _ = try await executeInDb(client: self.client, database: database, tablesSQL)

            // Create view in its own batch (CREATE VIEW must be first in batch)
            let viewSQL = """
            CREATE VIEW dbo.[\(viewName)] AS
            SELECT a.id, a.name, COALESCE(b.created, SYSUTCDATETIME()) AS snapshotDate
            FROM dbo.[\(tableA)] AS a
            LEFT JOIN dbo.[\(tableB)] AS b ON CAST(b.id AS uniqueidentifier) = CAST(NEWID() AS uniqueidentifier);
            """
            _ = try await executeInDb(client: self.client, database: database, viewSQL)

            try await withDbConnection(client: self.client, database: database) { connection in
                async let tables = connection.listTables(database: database, schema: "dbo").get()
                async let columnsA = connection.listColumns(database: database, schema: "dbo", table: tableA).get()
                async let columnsB = connection.listColumns(database: database, schema: "dbo", table: viewName).get()
                async let primaryKeys = connection.listPrimaryKeys(database: database, schema: "dbo").get()

                let (tableMetadata, tableAColumns, viewColumns, pkMetadata) = try await (tables, columnsA, columnsB, primaryKeys)

                XCTAssertTrue(tableMetadata.contains(where: { $0.name.caseInsensitiveCompare(tableA) == .orderedSame }))
                XCTAssertTrue(viewColumns.contains(where: { $0.name.caseInsensitiveCompare("snapshotDate") == .orderedSame }))
                XCTAssertTrue(pkMetadata.contains(where: { $0.table.caseInsensitiveCompare(tableA) == .orderedSame }))
                XCTAssertEqual(tableAColumns.count, 3)
            }
        }
    }
}
