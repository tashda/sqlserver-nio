import XCTest
import Logging
@testable import SQLServerKit
import SQLServerKitTesting

final class SQLServerIndexTests: XCTestCase, @unchecked Sendable {
    private var baseClient: SQLServerClient!
    private var client: SQLServerClient!
    private var indexClient: SQLServerIndexClient!
    private var adminClient: SQLServerAdministrationClient!
    private var testDatabase: String!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        self.baseClient = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            numberOfThreads: 1
        )
        _ = try await withTimeout(5) { try await self.baseClient.query("SELECT 1") }
        testDatabase = try await createTemporaryDatabase(client: baseClient, prefix: "idx")
        self.client = try await makeClient(forDatabase: testDatabase)
        self.adminClient = SQLServerAdministrationClient(client: self.client)
        self.indexClient = SQLServerIndexClient(client: self.client)
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
        if let db = testDatabase { try? await dropTemporaryDatabase(client: baseClient, name: db) }
        try? await baseClient?.shutdownGracefully()
        testDatabase = nil
        client = nil
        baseClient = nil
    }

    // MARK: - Helper Methods

    private func createTestTable(name: String, withPrimaryKey: Bool = true) async throws {
        var columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: withPrimaryKey))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(200))))),
            SQLServerColumnDefinition(name: "age", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "created_date", definition: .standard(.init(dataType: .datetime2(precision: 3))))
        ]

        if !withPrimaryKey {
            columns[0] = SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))
        }

        let tableColumns = columns
        try await withTimeout(15) { try await self.adminClient.createTable(name: name, columns: tableColumns) }

        let seedRows: [[String: SQLServerLiteralValue]] = [
            [
                "id": .int(1),
                "name": .nString("John Doe"),
                "email": .nString("john@example.com"),
                "age": .int(30),
                "created_date": .raw("'2023-01-01 10:00:00'")
            ],
            [
                "id": .int(2),
                "name": .nString("Jane Smith"),
                "email": .nString("jane@example.com"),
                "age": .int(25),
                "created_date": .raw("'2023-01-02 11:00:00'")
            ],
            [
                "id": .int(3),
                "name": .nString("Bob Johnson"),
                "email": .nString("bob@example.com"),
                "age": .int(35),
                "created_date": .raw("'2023-01-03 12:00:00'")
            ],
            [
                "id": .int(4),
                "name": .nString("Alice Brown"),
                "email": .nString("alice@example.com"),
                "age": .int(28),
                "created_date": .raw("'2023-01-04 13:00:00'")
            ],
            [
                "id": .int(5),
                "name": .nString("Charlie Wilson"),
                "email": .nString("charlie@example.com"),
                "age": .int(32),
                "created_date": .raw("'2023-01-05 14:00:00'")
            ]
        ]

        try await withTimeout(15) {
            try await self.client.withConnection { connection in
                for row in seedRows {
                    try await connection.insertRow(into: name, values: row)
                }
            }
        }
    }

    // MARK: - Basic Index Tests

    func testCreateSimpleIndex() async throws {
        let tableName = "test_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_name"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should exist after creation")

        let result = try await self.client.query("SELECT * FROM [\(tableName)] WHERE name = N'John Doe'")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("name")?.string, "John Doe")
    }

    func testCreateIndexWithMultipleColumns() async throws {
        let tableName = "test_multi_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_name_age"

        try await self.createTestTable(name: tableName)

        let columns = [
            IndexColumn(name: "name"),
            IndexColumn(name: "age", sortDirection: .descending)
        ]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Multi-column index should exist after creation")

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo, "Should retrieve index info")
        XCTAssertGreaterThanOrEqual(indexInfo?.columns.count ?? 0, 2, "Should have at least 2 columns")

        let columnNames = indexInfo?.columns.map { $0.name } ?? []
        XCTAssertTrue(columnNames.contains("name"), "Should contain name column")
        XCTAssertTrue(columnNames.contains("age"), "Should contain age column")
    }

    func testCreateIndexWithIncludedColumns() async throws {
        let tableName = "test_included_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_name_incl_email"

        try await self.createTestTable(name: tableName)

        let columns = [
            IndexColumn(name: "name"),
            IndexColumn(name: "email", isIncluded: true)
        ]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index with included columns should exist after creation")

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo, "Should retrieve index info")
        XCTAssertGreaterThanOrEqual(indexInfo?.columns.count ?? 0, 2, "Should have at least 2 columns")

        let columnNames = indexInfo?.columns.map { $0.name } ?? []
        XCTAssertTrue(columnNames.contains("name"), "Should contain name column")
        XCTAssertTrue(columnNames.contains("email"), "Should contain email column")
    }

    func testCreateUniqueIndex() async throws {
        let tableName = "test_unique_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_email_unique"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "email")]
        try await withTimeout(15) { try await self.indexClient.createUniqueIndex(name: indexName, table: tableName, columns: columns) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Unique index should exist after creation")

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo, "Should retrieve index info")

        do {
            try await self.client.withConnection { connection in
                try await connection.insertRow(into: tableName, values: [
                    "id": .int(6),
                    "name": .nString("Test User"),
                    "email": .nString("john@example.com"),
                    "age": .int(40),
                    "created_date": .raw("'2023-01-06 15:00:00'")
                ])
            }
            XCTFail("Inserting duplicate email should have failed due to unique index")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateClusteredIndex() async throws {
        let tableName = "test_clustered_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_clustered"

        try await self.createTestTable(name: tableName, withPrimaryKey: false)

        let columns = [IndexColumn(name: "id")]
        try await withTimeout(15) { try await self.indexClient.createClusteredIndex(name: indexName, table: tableName, columns: columns) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Clustered index should exist after creation")

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo)
        XCTAssertEqual(indexInfo?.indexType, .clustered, "Index should be clustered")
    }

    func testCreateIndexWithOptions() async throws {
        let tableName = "test_options_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_with_options"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name")]
        let options = IndexOptions(
            fillFactor: 80,
            padIndex: true,
            ignoreDuplicateKey: false,
            statisticsNoRecompute: false,
            allowRowLocks: true,
            allowPageLocks: true
        )
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns, options: options) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index with options should exist after creation")
    }

    func testDropIndex() async throws {
        let tableName = "test_drop_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_to_drop"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name")]
        try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)

        var exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should exist after creation")

        try await self.indexClient.dropIndex(name: indexName, table: tableName)

        exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertFalse(exists, "Index should not exist after being dropped")
    }

    // MARK: - Index Maintenance Tests

    func testRebuildIndex() async throws {
        let tableName = "test_rebuild_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_rebuild"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        try await withTimeout(15) { try await self.indexClient.rebuildIndex(name: indexName, table: tableName) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should still exist after rebuild")

        let result = try await self.client.query("SELECT * FROM [\(tableName)] WHERE name = N'John Doe'")
        XCTAssertEqual(result.count, 1)
    }

    func testReorganizeIndex() async throws {
        let tableName = "test_reorganize_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_reorganize"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        try await withTimeout(15) { try await self.indexClient.reorganizeIndex(name: indexName, table: tableName) }

        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should still exist after reorganize")

        let result = try await self.client.query("SELECT * FROM [\(tableName)] WHERE name = N'Jane Smith'")
        XCTAssertEqual(result.count, 1)
    }

    // MARK: - Index Information Tests

    func testGetIndexInfo() async throws {
        let tableName = "test_info_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_info"

        try await self.createTestTable(name: tableName)

        let columns = [
            IndexColumn(name: "name"),
            IndexColumn(name: "age", sortDirection: .descending),
            IndexColumn(name: "email", isIncluded: true)
        ]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo)
        XCTAssertEqual(indexInfo?.name, indexName)
        XCTAssertEqual(indexInfo?.tableName, tableName)
        XCTAssertEqual(indexInfo?.schemaName, "dbo")
        XCTAssertEqual(indexInfo?.indexType, .nonclustered)
        XCTAssertFalse(indexInfo?.isUnique == true)
        XCTAssertFalse(indexInfo?.isPrimaryKey == true)
        XCTAssertEqual(indexInfo?.columns.count, 3)

        let nameColumn = indexInfo?.columns.first { $0.name == "name" }
        XCTAssertNotNil(nameColumn, "Should have name column")

        let ageColumn = indexInfo?.columns.first { $0.name == "age" }
        XCTAssertNotNil(ageColumn, "Should have age column")

        let emailColumn = indexInfo?.columns.first { $0.name == "email" }
        XCTAssertNotNil(emailColumn, "Should have email column")
    }

    func testListTableIndexes() async throws {
        let tableName = "test_list_indexes_table_\(UUID().uuidString.prefix(8))"
        let index1Name = "IX_\(tableName)_name"
        let index2Name = "IX_\(tableName)_email"

        try await self.createTestTable(name: tableName)

        try await withTimeout(15) { try await self.indexClient.createIndex(name: index1Name, table: tableName, columns: [IndexColumn(name: "name")]) }
        try await withTimeout(15) { try await self.indexClient.createIndex(name: index2Name, table: tableName, columns: [IndexColumn(name: "email")]) }

        let indexes = try await self.indexClient.listTableIndexes(table: tableName)

        XCTAssertGreaterThanOrEqual(indexes.count, 2)

        let indexNames = indexes.map { $0.name }
        XCTAssertTrue(indexNames.contains(index1Name))
        XCTAssertTrue(indexNames.contains(index2Name))
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateIndex() async throws {
        let tableName = "test_duplicate_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_duplicate"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating duplicate index should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentIndex() async throws {
        let tableName = "test_nonexistent_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_nonexistent_index"

        try await self.createTestTable(name: tableName)

        do {
            try await self.indexClient.dropIndex(name: indexName, table: tableName)
            XCTFail("Dropping non-existent index should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateIndexOnNonExistentTable() async throws {
        let tableName = "non_existent_table"
        let indexName = "IX_test_index"

        let columns = [IndexColumn(name: "name")]

        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating index on non-existent table should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateIndexOnNonExistentColumn() async throws {
        let tableName = "test_bad_column_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_bad_column"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "non_existent_column")]

        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating index on non-existent column should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateClusteredIndexWithIncludedColumns() async throws {
        let tableName = "test_clustered_included_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_clustered_included"

        try await self.createTestTable(name: tableName, withPrimaryKey: false)

        let columns = [
            IndexColumn(name: "id"),
            IndexColumn(name: "name", isIncluded: true)
        ]

        do {
            try await self.indexClient.createClusteredIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating clustered index with included columns should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateIndexWithNoKeyColumns() async throws {
        let tableName = "test_no_key_columns_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_no_keys"

        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "name", isIncluded: true)]

        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating index with no key columns should have failed")
        } catch {
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
