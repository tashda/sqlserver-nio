import XCTest
@testable import SQLServerKit

final class SQLServerTableAdministrationTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!

    override func setUp() async throws {
        continueAfterFailure = false

        // Load environment configuration
        TestEnvironmentManager.loadEnvironmentVariables()

        // Configure logging
        _ = isLoggingConfigured

        // Create connection
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()

        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        group = nil
    }

    // MARK: - Tests

    func testDropTable() async throws {
        try await inTempDb { client in
            let tableName = "test_drop_table_\(UUID().uuidString.prefix(8))"

            // 1. Create a simple table
            let columns = [SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))]
            try await self.adminClient.createTable(name: tableName, columns: columns)

            // 2. Verify it exists
            var tableCount = try await self.getTableCount(client: client, name: tableName)
            XCTAssertEqual(tableCount, 1, "Table should exist after creation")

            // 3. Drop the table
            try await self.adminClient.dropTable(name: tableName)

            // 4. Verify it's gone
            tableCount = try await self.getTableCount(client: client, name: tableName)
            XCTAssertEqual(tableCount, 0, "Table should not exist after being dropped")
        }
    }

    func testCreateTableWithIdentityAndDefaults() async throws {
        try await inTempDb { client in
            let tableName = "test_identity_defaults_\(UUID().uuidString.prefix(8))"

            let columns = [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (seed: 1, increment: 1)))),
                SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), defaultValue: "Default Name")))
            ]

              try await self.adminClient.createTable(name: tableName, columns: columns)

            // Verify table was created
            let tableCount = try await self.getTableCount(client: client, name: tableName)
            XCTAssertEqual(tableCount, 1, "The simple table should have been created.")
        }
    }

    func testCreateTableWithConstraints() async throws {
        try await inTempDb { client in
            let tableName = "test_constraints_\(UUID().uuidString.prefix(8))"

            let columns = [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
                SQLServerColumnDefinition(name: "unique_col", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isUnique: true))),
                SQLServerColumnDefinition(name: "check_col", definition: .standard(.init(dataType: .int))),
                SQLServerColumnDefinition(name: "sparse_col", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isSparse: true))),
            ]

              try await self.adminClient.createTable(name: tableName, columns: columns)

            // Verify constraints using enhanced metadata APIs
            let metadataClient = try await client.withConnection { connection in
                SQLServerMetadataClient(connection: connection)
            }

            // Verify unique indexes
            let indexes = try await metadataClient.listIndexes(schema: "dbo", table: tableName).get()
            let uniqueIndexes = indexes.filter { $0.isUnique && !$0.isPrimaryKey }
            XCTAssertEqual(uniqueIndexes.count, 1, "Should find one unique index.")

            // Verify sparse column and nullability through detailed columns
            let tableColumns = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
            let sparseColumn = tableColumns.first { $0.name == "sparse_col" }
            XCTAssertNotNil(sparseColumn, "Should find sparse_col.")
            if let sparseColumn = sparseColumn {
                // Note: Sparse information would need to be added to ColumnMetadata if needed
                XCTAssertTrue(sparseColumn.isNullable, "Sparse column should be nullable")
            }

            // Verify check constraints
            let checkColumns = tableColumns.filter { $0.checkDefinition != nil }
            XCTAssertEqual(checkColumns.count, 1, "Should find one check constraint.")
            if let checkColumn = checkColumns.first {
                XCTAssertTrue(checkColumn.checkDefinition?.contains("check_col > 0") == true, "Should contain the check constraint definition")
            }
        }
    }

    func testCreateTableWithCompositePrimaryKey() async throws {
        try await inTempDb { client in
            let tableName = "test_composite_pk_\(UUID().uuidString.prefix(8))"

            let columns = [
                SQLServerColumnDefinition(name: "id1", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                SQLServerColumnDefinition(name: "id2", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
            ]

              try await self.adminClient.createTable(name: tableName, columns: columns)

            // Verify composite primary key using enhanced metadata APIs
            let metadataClient = try await client.withConnection { connection in
                SQLServerMetadataClient(connection: connection)
            }

            let indexes = try await metadataClient.listIndexes(schema: "dbo", table: tableName).get()
            let primaryKeyIndexes = indexes.filter { $0.isPrimaryKey }
            XCTAssertEqual(primaryKeyIndexes.count, 1, "Should have one primary key index.")

            if let pkIndex = primaryKeyIndexes.first {
                XCTAssertEqual(pkIndex.columns.count, 2, "Primary key should have 2 columns.")
                XCTAssertTrue(pkIndex.columns.contains { $0.column == "id1" }, "Primary key should include id1")
                XCTAssertTrue(pkIndex.columns.contains { $0.column == "id2" }, "Primary key should include id2")
            }
        }
    }

    func testCreateTableWithAllDataTypes() async throws {
        try await inTempDb { client in
            let tableName = "test_all_types_\(UUID().uuidString.prefix(8))"

  
            // Create table with a comprehensive set of data types
            let columns = [
                SQLServerColumnDefinition(name: "t_tinyint", definition: .standard(.init(dataType: .tinyint))),
                SQLServerColumnDefinition(name: "t_smallint", definition: .standard(.init(dataType: .smallint))),
                SQLServerColumnDefinition(name: "t_int", definition: .standard(.init(dataType: .int))),
                SQLServerColumnDefinition(name: "t_bigint", definition: .standard(.init(dataType: .bigint))),
                SQLServerColumnDefinition(name: "t_bit", definition: .standard(.init(dataType: .bit))),
                SQLServerColumnDefinition(name: "t_decimal", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2)))),
                SQLServerColumnDefinition(name: "t_numeric", definition: .standard(.init(dataType: .numeric(precision: 18, scale: 0)))),
                SQLServerColumnDefinition(name: "t_money", definition: .standard(.init(dataType: .money))),
                SQLServerColumnDefinition(name: "t_smallmoney", definition: .standard(.init(dataType: .smallmoney))),
                SQLServerColumnDefinition(name: "t_float", definition: .standard(.init(dataType: .float(mantissa: 53)))),
                SQLServerColumnDefinition(name: "t_real", definition: .standard(.init(dataType: .real))),
                SQLServerColumnDefinition(name: "t_date", definition: .standard(.init(dataType: .date))),
                SQLServerColumnDefinition(name: "t_datetime", definition: .standard(.init(dataType: .datetime))),
                SQLServerColumnDefinition(name: "t_datetime2", definition: .standard(.init(dataType: .datetime2(precision: 7)))),
                SQLServerColumnDefinition(name: "t_smalldatetime", definition: .standard(.init(dataType: .smalldatetime))),
                SQLServerColumnDefinition(name: "t_char", definition: .standard(.init(dataType: .char(length: 10)))),
                SQLServerColumnDefinition(name: "t_varchar", definition: .standard(.init(dataType: .varchar(length: .length(50))))),
                SQLServerColumnDefinition(name: "t_nchar", definition: .standard(.init(dataType: .nchar(length: 10)))),
                SQLServerColumnDefinition(name: "t_nvarchar", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
                SQLServerColumnDefinition(name: "t_ntext", definition: .standard(.init(dataType: .ntext))),
                SQLServerColumnDefinition(name: "t_text", definition: .standard(.init(dataType: .text))),
                SQLServerColumnDefinition(name: "t_binary", definition: .standard(.init(dataType: .binary(length: 10)))),
                SQLServerColumnDefinition(name: "t_varbinary", definition: .standard(.init(dataType: .varbinary(length: .length(50))))),
                SQLServerColumnDefinition(name: "t_varbinarymax", definition: .standard(.init(dataType: .varbinary(length: .max)))),
                SQLServerColumnDefinition(name: "t_image", definition: .standard(.init(dataType: .image))),
                SQLServerColumnDefinition(name: "t_uniqueidentifier", definition: .standard(.init(dataType: .uniqueidentifier))),
                SQLServerColumnDefinition(name: "t_sql_variant", definition: .standard(.init(dataType: .sql_variant))),
                SQLServerColumnDefinition(name: "t_xml", definition: .standard(.init(dataType: .xml)))
            ]

            try await self.adminClient.createTable(name: tableName, columns: columns)

            // Verify table was created and contains all expected columns
            let metadataClient = try await client.withConnection { connection in
                SQLServerMetadataClient(connection: connection)
            }
            let metadataColumns = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()

            let expected = [
                "t_tinyint": "tinyint",
                "t_smallint": "smallint",
                "t_int": "int",
                "t_bigint": "bigint",
                "t_bit": "bit",
                "t_decimal": "decimal",
                "t_numeric": "numeric",
                "t_money": "money",
                "t_smallmoney": "smallmoney",
                "t_float": "float",
                "t_real": "real",
                "t_date": "date",
                "t_datetime": "datetime",
                "t_datetime2": "datetime2",
                "t_smalldatetime": "smalldatetime",
                "t_char": "char",
                "t_varchar": "varchar",
                "t_nchar": "nchar",
                "t_nvarchar": "nvarchar",
                "t_ntext": "ntext",
                "t_text": "text",
                "t_binary": "binary",
                "t_varbinary": "varbinary",
                "t_varbinarymax": "varbinary",
                "t_image": "image",
                "t_uniqueidentifier": "uniqueidentifier",
                "t_sql_variant": "sql_variant",
                "t_xml": "xml"
            ]

            XCTAssertEqual(metadataColumns.count, expected.count, "The number of created columns should match the number of expected columns.")

            let actualDataTypes = Dictionary(uniqueKeysWithValues: metadataColumns.map { ($0.name, $0.typeName) })

            for (columnName, expectedType) in expected {
                guard let actualType = actualDataTypes[columnName] else {
                    XCTFail("Column \(columnName) not found in created table")
                    continue
                }
                XCTAssertEqual(actualType, expectedType, "Column \(columnName) should have type \(expectedType)")
            }
        }
    }

    func testTableAndColumnComments() async throws {
        try await inTempDb { client in
            let tableName = "test_comments_\(UUID().uuidString.prefix(8))"

            let columns = [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, comment: "Primary key identifier"))),
                SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), comment: "Full name of the entity"))),
                SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(255)), isNullable: true, comment: "Email address (optional)")))
            ]

            // Create table with comments
              try await self.adminClient.createTable(name: tableName, columns: columns)

            // Verify all comments were added using enhanced metadata APIs
            let metadataClient = try await client.withConnection { connection in
                SQLServerMetadataClient(connection: connection)
            }

            let metadataColumns = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
            let columnsWithComments = metadataColumns.filter { $0.comment != nil }
            XCTAssertEqual(columnsWithComments.count, 3, "Should find three column comments.")

            let expectedComments = [
                ("id", "Primary key identifier"),
                ("name", "Full name of the entity"),
                ("email", "Email address (optional)")
            ]

            // Sort columns by ordinal position to match expected order
            let sortedColumns = columnsWithComments.sorted(by: { $0.ordinalPosition < $1.ordinalPosition })
            for (index, (expectedName, expectedComment)) in expectedComments.enumerated() {
                if index < sortedColumns.count {
                    let column = sortedColumns[index]
                    XCTAssertEqual(column.name, expectedName, "Column name should match at index \(index)")
                    XCTAssertEqual(column.comment, expectedComment, "Comment should match for \(expectedName)")
                }
            }

            // Verify table comment
            let tables = try await metadataClient.listTables(schema: "dbo", includeComments: true).get()
            let testTable = tables.first { $0.name == tableName }
            XCTAssertNotNil(testTable, "Should find the created table.")
            if let testTable = testTable {
                XCTAssertEqual(testTable.comment, "Test table with comments", "Table comment should match")
            }

            // Verify column comments
            let metadataColumns2 = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
            let columnsWithComments2 = metadataColumns2.filter { $0.comment != nil }
            XCTAssertEqual(columnsWithComments2.count, 2, "Should find two column comments.")
        }
    }

    // func testUpdateTableComment() async throws {
    //     try await inTempDb { client in
    //         let tableName = "test_update_comment_\(UUID().uuidString.prefix(8))"

    //         let columns = [
    //             SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
    //             SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
    //         ]

    //         let adminClient = SQLServerAdministrationClient(client: client)

    //         // Create table without comment
    //         try await self.adminClient.createTable(name: tableName, columns: columns)

    //         // Add table comment
    //         try await self.adminClient.setTableComment(tableName: tableName, comment: "Initial table comment")

    //         // Verify table comment was added
    //         let metadataClient = try await client.withConnection { connection in
    //             SQLServerMetadataClient(connection: connection)
    //         }
    //         let tables = try await metadataClient.listTables(schema: "dbo", includeComments: true).get()
    //         let testTable = tables.first { $0.name == tableName }
    //         XCTAssertNotNil(testTable, "Should find the created table.")
    //         if let testTable = testTable {
    //             XCTAssertEqual(testTable.comment, "Initial table comment", "Table comment should match")
    //         }

    //         // Update table comment
    //         try await self.adminClient.setTableComment(tableName: tableName, comment: "Updated table comment")

    //         // Verify updated comment
    //         let tables2 = try await metadataClient.listTables(schema: "dbo", includeComments: true).get()
    //         let testTable2 = tables2.first { $0.name == tableName }
    //         XCTAssertNotNil(testTable2, "Should find the created table.")
    //         if let testTable2 = testTable2 {
    //             XCTAssertEqual(testTable2.comment, "Updated table comment", "Table comment should be updated")
    //         }

    //         // Remove table comment
    //         try await self.adminClient.setTableComment(tableName: tableName, comment: nil)

    //         // Verify comment was removed
    //         let tables3 = try await metadataClient.listTables(schema: "dbo", includeComments: true).get()
    //         let testTable3 = tables3.first { $0.name == tableName }
    //         XCTAssertNotNil(testTable3, "Should find the created table.")
    //         if let testTable3 = testTable3 {
    //             XCTAssertNil(testTable3?.comment, "Should find no table comments after removal.")
    //         }
    //     }
    // }

    func testCommentWithLongText() async throws {
        try await inTempDb { client in
            let tableName = "test_long_comment_\(UUID().uuidString.prefix(8))"

            let longComment = String(repeating: "This is a very long comment that tests the limits of extended properties. ", count: 50)

            let columns = [
                SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
                SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .max), comment: longComment)))
            ]

              try await self.adminClient.createTable(name: tableName, columns: columns)

            // Verify long comment was stored
            let metadataClient = try await client.withConnection { connection in
                SQLServerMetadataClient(connection: connection)
            }
            let metadataColumns = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
            let descriptionColumn = metadataColumns.first { $0.name == "description" }
            XCTAssertNotNil(descriptionColumn, "Should find description column.")
            if let descriptionColumn = descriptionColumn {
                XCTAssertEqual(descriptionColumn.comment?.count, longComment.count, "Long comment should be stored completely.")
            }
        }
    }

    // MARK: - Helpers

    private func getTableCount(client: SQLServerClient, name: String) async throws -> Int {
        let metadataClient = try await client.withConnection { connection in
            SQLServerMetadataClient(connection: connection)
        }
        let tables = try await metadataClient.listTables(schema: "dbo").get()
        return tables.filter { $0.name == name }.count
    }

    // Helper to run each test in an ephemeral database using DB-scoped clients
    private func inTempDb(_ body: @escaping (SQLServerClient) async throws -> Void) async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "adm") { db in
            let dbClient = try await makeClient(forDatabase: db, using: self.group)
            let prev = self.client
            self.client = dbClient
            self.adminClient = SQLServerAdministrationClient(client: dbClient)
            defer {
                Task {
                    try? await dbClient.shutdownGracefully().get()
                    self.client = prev
                }
            }
            try await body(dbClient)
        }
    }
}