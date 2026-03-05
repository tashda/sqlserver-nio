import XCTest
@testable import SQLServerKit

final class SQLServerTableAdministrationTests: XCTestCase {
    var group: EventLoopGroup!
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var testDatabase: String!
    private var skipDueToEnv = false

    override func setUp() async throws {
        continueAfterFailure = false
        TestEnvironmentManager.loadEnvironmentVariables()
        _ = isLoggingConfigured
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do {
            _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1").get() }
        } catch {
            skipDueToEnv = true
            return
        }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "adm")
        self.client = try await makeClient(forDatabase: testDatabase, using: group)
        self.adminClient = SQLServerAdministrationClient(client: self.client)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully().get()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try await baseClient?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        testDatabase = nil; group = nil
    }

    // MARK: - Tests

    func testDropTable() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_drop_table_\(UUID().uuidString.prefix(8))"

        let columns = [SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))]
        try await self.adminClient.createTable(name: tableName, columns: columns)

        var tableCount = try await self.getTableCount(client: self.client, name: tableName)
        XCTAssertEqual(tableCount, 1, "Table should exist after creation")

        try await self.adminClient.dropTable(name: tableName)

        tableCount = try await self.getTableCount(client: self.client, name: tableName)
        XCTAssertEqual(tableCount, 0, "Table should not exist after being dropped")
    }

    func testCreateTableWithIdentityAndDefaults() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_identity_defaults_\(UUID().uuidString.prefix(8))"

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (seed: 1, increment: 1)))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), defaultValue: "N'Default Name'")))
        ]

        try await self.adminClient.createTable(name: tableName, columns: columns)

        let tableCount = try await self.getTableCount(client: self.client, name: tableName)
        XCTAssertEqual(tableCount, 1, "The simple table should have been created.")
    }

    func testCreateTableWithConstraints() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_constraints_\(UUID().uuidString.prefix(8))"

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "unique_col", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isUnique: true))),
            SQLServerColumnDefinition(name: "check_col", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "sparse_col", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true, isSparse: true))),
        ]

        try await self.adminClient.createTable(name: tableName, columns: columns)

        let metadataClient = try await self.client.withConnection { connection in
            SQLServerMetadataClient(connection: connection)
        }

        let uniqueConstraints = try await metadataClient.listUniqueConstraints(schema: "dbo", table: tableName).get()
        XCTAssertEqual(uniqueConstraints.count, 1, "Should find one unique constraint.")

        let tableColumns = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
        let sparseColumn = tableColumns.first { $0.name == "sparse_col" }
        XCTAssertNotNil(sparseColumn, "Should find sparse_col.")
        if let sparseColumn = sparseColumn {
            XCTAssertTrue(sparseColumn.isNullable, "Sparse column should be nullable")
        }
    }

    func testCreateTableWithCompositePrimaryKey() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_composite_pk_\(UUID().uuidString.prefix(8))"

        let columns = [
            SQLServerColumnDefinition(name: "id1", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "id2", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]

        try await self.adminClient.createTable(name: tableName, columns: columns)

        let metadataClient = try await self.client.withConnection { connection in
            SQLServerMetadataClient(connection: connection)
        }

        let primaryKeys = try await metadataClient.listPrimaryKeys(schema: "dbo", table: tableName).get()
        XCTAssertEqual(primaryKeys.count, 1, "Should have one primary key constraint.")

        if let pk = primaryKeys.first {
            XCTAssertEqual(pk.columns.count, 2, "Primary key should have 2 columns.")
            XCTAssertTrue(pk.columns.contains { $0.column == "id1" }, "Primary key should include id1")
            XCTAssertTrue(pk.columns.contains { $0.column == "id2" }, "Primary key should include id2")
        }
    }

    func testCreateTableWithAllDataTypes() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_all_types_\(UUID().uuidString.prefix(8))"

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

        let metadataClient = try await self.client.withConnection { connection in
            var config = SQLServerMetadataClient.Configuration()
            config.preferStoredProcedureColumns = false
            return SQLServerMetadataClient(connection: connection, configuration: config)
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

    func testTableAndColumnComments() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_comments_\(UUID().uuidString.prefix(8))"

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, comment: "Primary key identifier"))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), comment: "Full name of the entity"))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(255)), isNullable: true, comment: "Email address (optional)")))
        ]

        try await self.adminClient.createTable(name: tableName, columns: columns)
        try await self.adminClient.addTableComment(tableName: tableName, comment: "Test table with comments")

        let metadataClient = try await self.client.withConnection { connection in
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

        let sortedColumns = columnsWithComments.sorted(by: { $0.ordinalPosition < $1.ordinalPosition })
        for (index, (expectedName, expectedComment)) in expectedComments.enumerated() {
            if index < sortedColumns.count {
                let column = sortedColumns[index]
                XCTAssertEqual(column.name, expectedName, "Column name should match at index \(index)")
                XCTAssertEqual(column.comment, expectedComment, "Comment should match for \(expectedName)")
            }
        }

        let tables = try await metadataClient.listTables(schema: "dbo", includeComments: true).get()
        let testTable = tables.first { $0.name == tableName }
        XCTAssertNotNil(testTable, "Should find the created table.")
        if let testTable = testTable {
            XCTAssertEqual(testTable.comment, "Test table with comments", "Table comment should match")
        }

        let metadataColumns2 = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
        let columnsWithComments2 = metadataColumns2.filter { $0.comment != nil }
        XCTAssertEqual(columnsWithComments2.count, 3, "Should find three column comments on subsequent fetch.")
    }

    func testCommentWithLongText() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        let tableName = "test_long_comment_\(UUID().uuidString.prefix(8))"

        let longComment = String(repeating: "This is a very long comment that tests the limits of extended properties. ", count: 50)

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .max), comment: longComment)))
        ]

        try await self.adminClient.createTable(name: tableName, columns: columns)

        let metadataClient = try await self.client.withConnection { connection in
            SQLServerMetadataClient(connection: connection)
        }
        let metadataColumns = try await metadataClient.listColumns(schema: "dbo", table: tableName, includeComments: true).get()
        let descriptionColumn = metadataColumns.first { $0.name == "description" }
        XCTAssertNotNil(descriptionColumn, "Should find description column.")
        if let descriptionColumn = descriptionColumn {
            XCTAssertEqual(descriptionColumn.comment?.count, longComment.count, "Long comment should be stored completely.")
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
}
