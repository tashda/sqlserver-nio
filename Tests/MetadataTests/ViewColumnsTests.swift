@testable import SQLServerKit
import NIO
import XCTest

final class SQLServerMetadataViewColumnsTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testListColumnsReturnsMetadataForViews() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "viewcolumns") { database in
            let tableName = "tbl_" + UUID().uuidString.prefix(8)
            let viewName = "vw_" + UUID().uuidString.prefix(8)

            let tableSQL = """
            CREATE TABLE dbo.[\(tableName)] (
                id INT NOT NULL PRIMARY KEY,
                displayName NVARCHAR(200) NOT NULL,
                extra XML NULL,
                lastUpdated DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            );
            """
            _ = try await executeInDb(client: self.client, database: database, tableSQL)

            let viewSQL = """
            CREATE VIEW dbo.[\(viewName)] AS
            SELECT
                t.id,
                t.displayName,
                t.lastUpdated,
                COALESCE(
                    bcast.Phone.value('(./number/text())[1]', 'nvarchar(32)'),
                    '(none)'
                ) AS phoneNumber
            FROM dbo.[\(tableName)] AS t
            OUTER APPLY t.extra.nodes('/broadcast') AS bcast(Phone);
            """
            _ = try await executeInDb(client: self.client, database: database, viewSQL)

            let tableColumns = try await withDbConnection(client: self.client, database: database) { connection in
                try await connection.listColumns(database: database, schema: "dbo", table: tableName).get()
            }

            let viewColumns = try await withDbConnection(client: self.client, database: database) { connection in
                try await connection.listColumns(database: database, schema: "dbo", table: viewName).get()
            }

            // Expect the view to project: id, displayName, lastUpdated, phoneNumber
            XCTAssertEqual(viewColumns.map(\.name), ["id", "displayName", "lastUpdated", "phoneNumber"])
            XCTAssertEqual(viewColumns.map(\.typeName), ["int", "nvarchar", "datetime2", "nvarchar"])
            XCTAssertEqual(viewColumns.count, 4)

            // Table path (stored procedure) should capture the default definition for lastUpdated.
            if let lastColumn = tableColumns.first(where: { $0.name.caseInsensitiveCompare("lastUpdated") == .orderedSame }) {
                XCTAssertNotNil(lastColumn.defaultDefinition)
            } else {
                XCTFail("Missing lastUpdated column metadata for table")
            }

            // View path (catalog query) should not attempt to dereference default/computed definitions.
            XCTAssertTrue(viewColumns.allSatisfy { $0.defaultDefinition == nil && $0.computedDefinition == nil })
        }
    }

    @available(macOS 12.0, *)
    func testListColumnsAdventureWorksVEEmployeeLikeViews() async throws {
        try await withDbConnection(client: self.client, database: "AdventureWorks2022") { connection in
            let viewNames = [
                (schema: "HumanResources", name: "vEmployee"),
                (schema: "HumanResources", name: "vEmployeeDepartment"),
                (schema: "HumanResources", name: "vEmployeeDepartmentHistory")
            ]
            for (schema, name) in viewNames {
                let columns = try await connection.listColumns(database: "AdventureWorks2022", schema: schema, table: name).get()
                XCTAssertFalse(columns.isEmpty, "Expected columns for \(schema).\(name)")
            }
        }
    }

    @available(macOS 12.0, *)
    func testListColumnsAdventureWorksVJobCandidateDoesNotExecuteViewBody() async throws {
        let start = DispatchTime.now()
        try await withDbConnection(client: self.client, database: "AdventureWorks2022") { connection in
            let columns = try await connection.listColumns(database: "AdventureWorks2022", schema: "HumanResources", table: "vJobCandidate").get()
            XCTAssertFalse(columns.isEmpty, "Expected metadata for HumanResources.vJobCandidate")
        }
        let elapsedMs = Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
        XCTAssertLessThan(elapsedMs, 5000, "Catalog lookup still running long, indicates stored procedure fallback")
    }

    @available(macOS 12.0, *)
    func testListColumnsAdventureWorksProductionCatalogView() async throws {
        let start = DispatchTime.now()
        try await withDbConnection(client: self.client, database: "AdventureWorks2022") { connection in
            let columns = try await connection.listColumns(
                database: "AdventureWorks2022",
                schema: "Production",
                table: "vProductModelCatalogDescription"
            ).get()
            XCTAssertEqual(columns.count, 25, "Expected 25 columns for Production.vProductModelCatalogDescription")
            XCTAssertFalse(columns.contains { $0.name.isEmpty }, "Column names should not be empty")
        }
        let elapsedMs = Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
        XCTAssertLessThan(elapsedMs, 5000, "Catalog lookup for Production.vProductModelCatalogDescription unexpectedly slow")
    }
}
