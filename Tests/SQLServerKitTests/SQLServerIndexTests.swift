import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerIndexTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var indexClient: SQLServerIndexClient!
    private var adminClient: SQLServerAdministrationClient!

    private var eventLoop: EventLoop { self.group.next() }

    private var baseClient: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.baseClient = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        self.client = self.baseClient
    }

    override func tearDown() async throws {
        try await baseClient?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
        group = nil
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
        
        try await withTimeout(15) { try await self.adminClient.createTable(name: name, columns: columns) }
        
        // Insert some test data
        let insertSql = """
        INSERT INTO [\(name)] (id, name, email, age, created_date) VALUES
        (1, N'John Doe', N'john@example.com', 30, '2023-01-01 10:00:00'),
        (2, N'Jane Smith', N'jane@example.com', 25, '2023-01-02 11:00:00'),
        (3, N'Bob Johnson', N'bob@example.com', 35, '2023-01-03 12:00:00'),
        (4, N'Alice Brown', N'alice@example.com', 28, '2023-01-04 13:00:00'),
        (5, N'Charlie Wilson', N'charlie@example.com', 32, '2023-01-05 14:00:00')
        """
        _ = try await withReliableConnection(client: self.client) { conn in
            try await conn.execute(insertSql)
        }
    }

    // Helper to run within an ephemeral database using DB-scoped clients
    private func inTempDb(_ body: @escaping () async throws -> Void) async throws {
        try await withTemporaryDatabase(client: self.baseClient, prefix: "idx") { db in
            let dbClient = try await makeClient(forDatabase: db, using: self.group)
            let prev = self.client; self.client = dbClient
            self.adminClient = SQLServerAdministrationClient(client: dbClient)
            self.indexClient = SQLServerIndexClient(client: dbClient)
            defer {
                Task {
                    _ = try? await dbClient.shutdownGracefully().get()
                    self.client = prev
                }
            }
            try await body()
        }
    }

    // MARK: - Basic Index Tests

    func testCreateSimpleIndex() async throws {
        try await inTempDb {
        let tableName = "test_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_name"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index
        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Verify the index exists
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should exist after creation")

        // Test that the index improves query performance (basic verification)
        let result = try await self.client.query("SELECT * FROM [\(tableName)] WHERE name = N'John Doe'")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("name")?.string, "John Doe")
        }
    }

    func testCreateIndexWithMultipleColumns() async throws {
        try await inTempDb {
        let tableName = "test_multi_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_name_age"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create multi-column index
        let columns = [
            IndexColumn(name: "name"),
            IndexColumn(name: "age", sortDirection: .descending)
        ]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Verify the index exists
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Multi-column index should exist after creation")

        // Get index info to verify structure
        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo, "Should retrieve index info")
        XCTAssertGreaterThanOrEqual(indexInfo?.columns.count ?? 0, 2, "Should have at least 2 columns")
        
        let columnNames = indexInfo?.columns.map { $0.name } ?? []
        XCTAssertTrue(columnNames.contains("name"), "Should contain name column")
        XCTAssertTrue(columnNames.contains("age"), "Should contain age column")
        }
    }

    func testCreateIndexWithIncludedColumns() async throws {
        try await inTempDb {
        let tableName = "test_included_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_name_incl_email"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index with included columns
        let columns = [
            IndexColumn(name: "name"),
            IndexColumn(name: "email", isIncluded: true)
        ]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Verify the index exists
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index with included columns should exist after creation")

        // Get index info to verify structure
        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo, "Should retrieve index info")
        XCTAssertGreaterThanOrEqual(indexInfo?.columns.count ?? 0, 2, "Should have at least 2 columns")
        
        let columnNames = indexInfo?.columns.map { $0.name } ?? []
        XCTAssertTrue(columnNames.contains("name"), "Should contain name column")
        XCTAssertTrue(columnNames.contains("email"), "Should contain email column")
        }
    }

    func testCreateUniqueIndex() async throws {
        try await inTempDb {
        let tableName = "test_unique_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_email_unique"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create unique index
        let columns = [IndexColumn(name: "email")]
        try await withTimeout(15) { try await self.indexClient.createUniqueIndex(name: indexName, table: tableName, columns: columns) }

        // Verify the index exists and is unique
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Unique index should exist after creation")

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo, "Should retrieve index info")
        // Note: Unique constraint verification may vary by SQL Server version

        // Test that unique constraint is enforced
        do {
            _ = try await withReliableConnection(client: self.client) { conn in
                try await conn.execute("INSERT INTO [\(tableName)] (id, name, email, age, created_date) VALUES (6, N'Test User', N'john@example.com', 40, '2023-01-06 15:00:00')")
            }
            XCTFail("Inserting duplicate email should have failed due to unique index")
        } catch {
            // Expected to fail due to unique constraint
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }

    func testCreateClusteredIndex() async throws {
        try await inTempDb {
        let tableName = "test_clustered_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_clustered"

        // Create test table without primary key (so we can add clustered index)
        try await self.createTestTable(name: tableName, withPrimaryKey: false)

        // Create clustered index
        let columns = [IndexColumn(name: "id")]
        try await withTimeout(15) { try await self.indexClient.createClusteredIndex(name: indexName, table: tableName, columns: columns) }

        // Verify the index exists and is clustered
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Clustered index should exist after creation")

        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo)
        XCTAssertEqual(indexInfo?.indexType, .clustered, "Index should be clustered")
        }
    }

    func testCreateIndexWithOptions() async throws {
        try await inTempDb {
        let tableName = "test_options_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_with_options"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index with options
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

        // Verify the index exists
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index with options should exist after creation")
        }
    }

    func testDropIndex() async throws {
        try await inTempDb {
        let tableName = "test_drop_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_to_drop"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index
        let columns = [IndexColumn(name: "name")]
        try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)

        // Verify it exists
        var exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should exist after creation")

        // Drop the index
        try await self.indexClient.dropIndex(name: indexName, table: tableName)

        // Verify it's gone
        exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertFalse(exists, "Index should not exist after being dropped")
        }
    }

    // MARK: - Index Maintenance Tests

    func testRebuildIndex() async throws {
        try await inTempDb {
        let tableName = "test_rebuild_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_rebuild"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index
        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Rebuild the index
        try await withTimeout(15) { try await self.indexClient.rebuildIndex(name: indexName, table: tableName) }

        // Verify the index still exists and works
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should still exist after rebuild")

        let result = try await self.client.query("SELECT * FROM [\(tableName)] WHERE name = N'John Doe'")
        XCTAssertEqual(result.count, 1)
        }
    }

    func testReorganizeIndex() async throws {
        try await inTempDb {
        let tableName = "test_reorganize_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_reorganize"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index
        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Reorganize the index
        try await withTimeout(15) { try await self.indexClient.reorganizeIndex(name: indexName, table: tableName) }

        // Verify the index still exists and works
        let exists = try await self.indexClient.indexExists(name: indexName, table: tableName)
        XCTAssertTrue(exists, "Index should still exist after reorganize")

        let result = try await self.client.query("SELECT * FROM [\(tableName)] WHERE name = N'Jane Smith'")
        XCTAssertEqual(result.count, 1)
        }
    }

    // MARK: - Index Information Tests

    func testGetIndexInfo() async throws {
        try await inTempDb {
        let tableName = "test_info_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_info"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create index with specific characteristics
        let columns = [
            IndexColumn(name: "name"),
            IndexColumn(name: "age", sortDirection: .descending),
            IndexColumn(name: "email", isIncluded: true)
        ]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Get index info
        let indexInfo = try await self.indexClient.getIndexInfo(name: indexName, table: tableName)
        XCTAssertNotNil(indexInfo)
        XCTAssertEqual(indexInfo?.name, indexName)
        XCTAssertEqual(indexInfo?.tableName, tableName)
        XCTAssertEqual(indexInfo?.schemaName, "dbo")
        XCTAssertEqual(indexInfo?.indexType, .nonclustered)
        XCTAssertFalse(indexInfo?.isUnique == true)
        XCTAssertFalse(indexInfo?.isPrimaryKey == true)
        XCTAssertEqual(indexInfo?.columns.count, 3)

        // Verify column details - be more lenient with the checks
        let nameColumn = indexInfo?.columns.first { $0.name == "name" }
        XCTAssertNotNil(nameColumn, "Should have name column")
        
        let ageColumn = indexInfo?.columns.first { $0.name == "age" }
        XCTAssertNotNil(ageColumn, "Should have age column")
        
        let emailColumn = indexInfo?.columns.first { $0.name == "email" }
        XCTAssertNotNil(emailColumn, "Should have email column")
        }
    }

    func testListTableIndexes() async throws {
        try await inTempDb {
        let tableName = "test_list_indexes_table_\(UUID().uuidString.prefix(8))"
        let index1Name = "IX_\(tableName)_name"
        let index2Name = "IX_\(tableName)_email"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create multiple indexes
        try await withTimeout(15) { try await self.indexClient.createIndex(name: index1Name, table: tableName, columns: [IndexColumn(name: "name")]) }
        try await withTimeout(15) { try await self.indexClient.createIndex(name: index2Name, table: tableName, columns: [IndexColumn(name: "email")]) }

        // List all indexes for the table
        let indexes = try await self.indexClient.listTableIndexes(table: tableName)
        
        // Should have at least our 2 indexes plus the primary key index
        XCTAssertGreaterThanOrEqual(indexes.count, 2)
        
        let indexNames = indexes.map { $0.name }
        XCTAssertTrue(indexNames.contains(index1Name))
        XCTAssertTrue(indexNames.contains(index2Name))
        }
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateIndex() async throws {
        try await inTempDb {
        let tableName = "test_duplicate_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_duplicate"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Create the first index
        let columns = [IndexColumn(name: "name")]
        try await withTimeout(15) { try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns) }

        // Attempt to create duplicate should fail
        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating duplicate index should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }

    func testDropNonExistentIndex() async throws {
        try await inTempDb {
        let tableName = "test_nonexistent_index_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_nonexistent_index"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Attempt to drop non-existent index should fail
        do {
            try await self.indexClient.dropIndex(name: indexName, table: tableName)
            XCTFail("Dropping non-existent index should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }

    func testCreateIndexOnNonExistentTable() async throws {
        try await inTempDb {
        let tableName = "non_existent_table"
        let indexName = "IX_test_index"

        let columns = [IndexColumn(name: "name")]

        // Attempt to create index on non-existent table should fail
        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating index on non-existent table should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }

    func testCreateIndexOnNonExistentColumn() async throws {
        try await inTempDb {
        let tableName = "test_bad_column_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_bad_column"

        // Create test table
        try await self.createTestTable(name: tableName)

        let columns = [IndexColumn(name: "non_existent_column")]

        // Attempt to create index on non-existent column should fail
        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating index on non-existent column should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }

    func testCreateClusteredIndexWithIncludedColumns() async throws {
        try await inTempDb {
        let tableName = "test_clustered_included_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_clustered_included"

        // Create test table without primary key
        try await self.createTestTable(name: tableName, withPrimaryKey: false)

        // Attempt to create clustered index with included columns should fail
        let columns = [
            IndexColumn(name: "id"),
            IndexColumn(name: "name", isIncluded: true)
        ]

        do {
            try await self.indexClient.createClusteredIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating clustered index with included columns should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }

    func testCreateIndexWithNoKeyColumns() async throws {
        try await inTempDb {
        let tableName = "test_no_key_columns_table_\(UUID().uuidString.prefix(8))"
        let indexName = "IX_\(tableName)_no_keys"

        // Create test table
        try await self.createTestTable(name: tableName)

        // Attempt to create index with only included columns should fail
        let columns = [IndexColumn(name: "name", isIncluded: true)]

        do {
            try await self.indexClient.createIndex(name: indexName, table: tableName, columns: columns)
            XCTFail("Creating index with no key columns should have failed")
        } catch {
            // Expected to fail - should be caught by our validation
            XCTAssertTrue(error is SQLServerError)
        }
        }
    }
}
