@testable import SQLServerKit
import SQLServerKitTesting
import NIO
import XCTest

final class SQLServerMetadataConcurrencyTests: XCTestCase, @unchecked Sendable {
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
            try await withDbConnection(client: self.client, database: database) { connection in
                try await connection.createTable(
                    name: String(tableA),
                    columns: [
                        SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                        SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(40))))),
                        SQLServerColumnDefinition(name: "info", definition: .standard(.init(dataType: .xml, isNullable: true)))
                    ]
                )
                try await connection.createTable(
                    name: String(tableB),
                    columns: [
                        SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .uniqueidentifier, isPrimaryKey: true))),
                        SQLServerColumnDefinition(name: "ref", definition: .standard(.init(dataType: .int))),
                        SQLServerColumnDefinition(name: "created", definition: .standard(.init(dataType: .datetime2(precision: 7), defaultValue: "SYSUTCDATETIME()")))
                    ]
                )
                try await connection.createView(
                    name: String(viewName),
                    query: """
                    SELECT a.id, a.name, COALESCE(b.created, SYSUTCDATETIME()) AS snapshotDate
                    FROM dbo.[\(tableA)] AS a
                    LEFT JOIN dbo.[\(tableB)] AS b ON CAST(b.id AS uniqueidentifier) = CAST(NEWID() AS uniqueidentifier)
                    """
                )
            }

            try await withDbConnection(client: self.client, database: database) { connection in
                async let tables = connection.listTables(database: database, schema: "dbo")
                async let columnsA = connection.listColumns(database: database, schema: "dbo", table: tableA)
                async let columnsB = connection.listColumns(database: database, schema: "dbo", table: viewName)
                async let primaryKeys = connection.listPrimaryKeys(database: database, schema: "dbo")

                let (tableMetadata, tableAColumns, viewColumns, pkMetadata) = try await (tables, columnsA, columnsB, primaryKeys)

                XCTAssertTrue(tableMetadata.contains(where: { $0.name.caseInsensitiveCompare(tableA) == .orderedSame }))
                XCTAssertTrue(viewColumns.contains(where: { $0.name.caseInsensitiveCompare("snapshotDate") == .orderedSame }))
                XCTAssertTrue(pkMetadata.contains(where: { $0.table.caseInsensitiveCompare(tableA) == .orderedSame }))
                XCTAssertEqual(tableAColumns.count, 3)
            }
        }
    }
}
