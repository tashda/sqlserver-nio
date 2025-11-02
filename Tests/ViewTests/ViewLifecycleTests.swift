import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerViewTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var viewClient: SQLServerViewClient!
    private var adminClient: SQLServerAdministrationClient!
    private var viewsToDrop: [(name: String, schema: String)] = []
    private var tablesToDrop: [String] = []

    private var eventLoop: EventLoop { self.group.next() }

    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        self.viewClient = SQLServerViewClient(client: client)
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDownWithError() throws {
        // Drop any views that were created during the test using SQLServerViewClient
        for view in viewsToDrop {
            do {
                try viewClient.dropView(name: view.name, schema: view.schema).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop view \(view.schema).\(view.name): \(error)")
            }
        }
        viewsToDrop.removeAll()

        // Drop any tables that were created during the test
        for table in tablesToDrop {
            do {
                try adminClient.dropTable(name: table).wait()
            } catch {
                // Ignore errors during cleanup
                print("Warning: Failed to drop table \(table): \(error)")
            }
        }
        tablesToDrop.removeAll()

        try self.client.shutdownGracefully().wait()
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }

    // MARK: - Helper Methods

    private func createTestTable(name: String) async throws {
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "created_date", definition: .standard(.init(dataType: .datetime2(precision: 3))))
        ]
        
        try await adminClient.createTable(name: name, columns: columns)
        tablesToDrop.append(name)
        
        // Insert some test data
        let insertSql = """
        INSERT INTO [\(name)] (id, name, value, created_date) VALUES
        (1, N'First', 100, '2023-01-01 10:00:00'),
        (2, N'Second', 200, '2023-01-02 11:00:00'),
        (3, N'Third', 300, '2023-01-03 12:00:00')
        """
        _ = try await client.execute(insertSql)
    }

    // MARK: - Basic View Tests

    func testCreateSimpleView() async throws {
        let tableName = "test_view_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_simple_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create view
        let query = "SELECT id, name FROM [\(tableName)] WHERE value > 150"
        try await viewClient.createView(name: viewName, query: query)

        // Verify the view exists
        let exists = try await viewClient.viewExists(name: viewName)
        XCTAssertTrue(exists, "View should exist after creation")

        // Test querying the view
        let result = try await client.query("SELECT * FROM [\(viewName)] ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("id")?.int, 2)
        XCTAssertEqual(result[0].column("name")?.string, "Second")
        XCTAssertEqual(result[1].column("id")?.int, 3)
        XCTAssertEqual(result[1].column("name")?.string, "Third")
    }

    func testCreateViewWithOptions() async throws {
        let tableName = "test_view_options_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_options_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create view with options
        let options = ViewOptions(withCheckOption: true)
        let query = "SELECT id, name, value FROM [\(tableName)] WHERE value <= 250"
        try await viewClient.createView(name: viewName, query: query, options: options)

        // Verify the view exists
        let exists = try await viewClient.viewExists(name: viewName)
        XCTAssertTrue(exists, "View with options should exist after creation")

        // Test querying the view
        let result = try await client.query("SELECT * FROM [\(viewName)] ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("value")?.int, 100)
        XCTAssertEqual(result[1].column("value")?.int, 200)
    }

    func testAlterView() async throws {
        let tableName = "test_alter_view_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_alter_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create initial view
        let initialQuery = "SELECT id, name FROM [\(tableName)]"
        try await viewClient.createView(name: viewName, query: initialQuery)

        // Alter the view
        let alteredQuery = "SELECT id, name, value FROM [\(tableName)] WHERE id <= 2"
        try await viewClient.alterView(name: viewName, query: alteredQuery)

        // Test the altered view
        let result = try await client.query("SELECT * FROM [\(viewName)] ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertNotNil(result[0].column("value"), "Altered view should include value column")
        XCTAssertEqual(result[0].column("value")?.int, 100)
        XCTAssertEqual(result[1].column("value")?.int, 200)
    }

    func testDropView() async throws {
        let tableName = "test_drop_view_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_drop_view_\(UUID().uuidString.prefix(8))"

        // Create test table
        try await createTestTable(name: tableName)

        // Create view
        let query = "SELECT * FROM [\(tableName)]"
        try await viewClient.createView(name: viewName, query: query)

        // Verify it exists
        var exists = try await viewClient.viewExists(name: viewName)
        XCTAssertTrue(exists, "View should exist after creation")

        // Drop the view
        try await viewClient.dropView(name: viewName)

        // Verify it's gone
        exists = try await viewClient.viewExists(name: viewName)
        XCTAssertFalse(exists, "View should not exist after being dropped")
    }

    func testGetViewDefinition() async throws {
        let tableName = "test_definition_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_definition_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create view
        let query = "SELECT id, name FROM [\(tableName)] WHERE value > 100"
        try await viewClient.createView(name: viewName, query: query)

        // Get view definition
        let definition = try await viewClient.getViewDefinition(name: viewName)
        XCTAssertNotNil(definition, "View definition should be retrievable")
        XCTAssertTrue(definition?.contains("SELECT") == true, "Definition should contain SELECT")
        XCTAssertTrue(definition?.contains(tableName) == true, "Definition should contain table name")
    }

    // MARK: - Indexed View Tests

    func testCreateIndexedView() async throws {
        let tableName = "test_indexed_view_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_indexed_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create indexed view (requires specific query format for SQL Server)
        let query = """
        SELECT id, name, value, COUNT_BIG(*) AS row_count
        FROM dbo.[\(tableName)]
        GROUP BY id, name, value
        """
        
        try await viewClient.createIndexedView(
            name: viewName,
            query: query,
            indexName: "IX_\(viewName)_id",
            indexColumns: ["id"]
        )

        // Verify the view exists
        let exists = try await viewClient.viewExists(name: viewName)
        XCTAssertTrue(exists, "Indexed view should exist after creation")

        // Verify it's indexed
        let isIndexed = try await viewClient.isIndexedView(name: viewName)
        XCTAssertTrue(isIndexed, "View should be indexed")

        // Test querying the indexed view
        let result = try await client.query("SELECT * FROM [\(viewName)] ORDER BY id")
        XCTAssertEqual(result.count, 3)
        XCTAssertEqual(result[0].column("row_count")?.int, 1)
    }

    func testRefreshIndexedView() async throws {
        let tableName = "test_refresh_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_refresh_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create indexed view
        let query = """
        SELECT id, name, COUNT_BIG(*) AS row_count
        FROM dbo.[\(tableName)]
        GROUP BY id, name
        """
        
        try await viewClient.createIndexedView(
            name: viewName,
            query: query,
            indexName: "IX_\(viewName)_id",
            indexColumns: ["id"]
        )

        // Refresh the indexed view (update statistics)
        try await viewClient.refreshIndexedView(name: viewName)

        // Verify the view still works
        let result = try await client.query("SELECT COUNT(*) as count FROM [\(viewName)]")
        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("count")?.int, 3)
    }

    // MARK: - Complex View Tests

    func testCreateViewWithJoins() async throws {
        let table1Name = "test_join_table1_\(UUID().uuidString.prefix(8))"
        let table2Name = "test_join_table2_\(UUID().uuidString.prefix(8))"
        let viewName = "test_join_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create first test table
        let columns1 = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]
        try await adminClient.createTable(name: table1Name, columns: columns1)
        tablesToDrop.append(table1Name)

        // Create second test table
        let columns2 = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "table1_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(200)))))
        ]
        try await adminClient.createTable(name: table2Name, columns: columns2)
        tablesToDrop.append(table2Name)

        // Insert test data
        let insertSql1 = "INSERT INTO [\(table1Name)] (id, name) VALUES (1, N'First'), (2, N'Second')"
        let insertSql2 = "INSERT INTO [\(table2Name)] (id, table1_id, description) VALUES (1, 1, N'Desc1'), (2, 2, N'Desc2')"
        _ = try await client.execute(insertSql1)
        _ = try await client.execute(insertSql2)

        // Create view with join
        let query = """
        SELECT t1.id, t1.name, t2.description
        FROM [\(table1Name)] t1
        INNER JOIN [\(table2Name)] t2 ON t1.id = t2.table1_id
        """
        try await viewClient.createView(name: viewName, query: query)

        // Test the view
        let result = try await client.query("SELECT * FROM [\(viewName)] ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("name")?.string, "First")
        XCTAssertEqual(result[0].column("description")?.string, "Desc1")
        XCTAssertEqual(result[1].column("name")?.string, "Second")
        XCTAssertEqual(result[1].column("description")?.string, "Desc2")
    }

    func testCreateViewWithAggregation() async throws {
        let tableName = "test_agg_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_agg_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table with category
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
            SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        tablesToDrop.append(tableName)

        // Insert test data
        let insertSql = """
        INSERT INTO [\(tableName)] (id, category, amount) VALUES
        (1, N'A', 100.50),
        (2, N'A', 200.25),
        (3, N'B', 150.75),
        (4, N'B', 300.00),
        (5, N'C', 75.25)
        """
        _ = try await client.execute(insertSql)

        // Create aggregation view
        let query = """
        SELECT category, 
               COUNT(*) as item_count,
               SUM(amount) as total_amount,
               AVG(amount) as avg_amount
        FROM [\(tableName)]
        GROUP BY category
        """
        try await viewClient.createView(name: viewName, query: query)

        // Test the aggregation view
        let result = try await client.query("SELECT * FROM [\(viewName)] ORDER BY category")
        XCTAssertEqual(result.count, 3)
        
        // Check category A
        XCTAssertEqual(result[0].column("category")?.string, "A")
        XCTAssertEqual(result[0].column("item_count")?.int, 2)
        guard
            let totalAmountA = result[0].column("total_amount")?.double,
            let averageAmountA = result[0].column("avg_amount")?.double
        else {
            XCTFail("Failed to decode aggregated metrics for category A")
            return
        }
        XCTAssertEqual(totalAmountA, 300.75, accuracy: 0.0001)
        XCTAssertEqual(averageAmountA, 150.375, accuracy: 0.0001)
        
        // Check category B
        XCTAssertEqual(result[1].column("category")?.string, "B")
        XCTAssertEqual(result[1].column("item_count")?.int, 2)
        guard
            let totalAmountB = result[1].column("total_amount")?.double,
            let averageAmountB = result[1].column("avg_amount")?.double
        else {
            XCTFail("Failed to decode aggregated metrics for category B")
            return
        }
        XCTAssertEqual(totalAmountB, 450.75, accuracy: 0.0001)
        XCTAssertEqual(averageAmountB, 225.375, accuracy: 0.0001)
        
        // Check category C single row
        XCTAssertEqual(result[2].column("category")?.string, "C")
        XCTAssertEqual(result[2].column("item_count")?.int, 1)
        guard let totalAmountC = result[2].column("total_amount")?.double else {
            XCTFail("Failed to decode aggregated metrics for category C")
            return
        }
        XCTAssertEqual(totalAmountC, 75.25, accuracy: 0.0001)
    }

    // MARK: - Error Handling Tests

    func testCreateDuplicateView() async throws {
        let tableName = "test_duplicate_table_\(UUID().uuidString.prefix(8))"
        let viewName = "test_duplicate_view_\(UUID().uuidString.prefix(8))"
        viewsToDrop.append((name: viewName, schema: "dbo"))

        // Create test table
        try await createTestTable(name: tableName)

        // Create the first view
        let query = "SELECT * FROM [\(tableName)]"
        try await viewClient.createView(name: viewName, query: query)

        // Attempt to create duplicate should fail
        do {
            try await viewClient.createView(name: viewName, query: query)
            XCTFail("Creating duplicate view should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testDropNonExistentView() async throws {
        let viewName = "non_existent_view_\(UUID().uuidString.prefix(8))"

        // Attempt to drop non-existent view should fail
        do {
            try await viewClient.dropView(name: viewName)
            XCTFail("Dropping non-existent view should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }

    func testCreateViewWithInvalidQuery() async throws {
        let viewName = "test_invalid_view_\(UUID().uuidString.prefix(8))"

        let invalidQuery = "SELECT * FROM non_existent_table"

        // Attempt to create view with invalid query should fail
        do {
            try await viewClient.createView(name: viewName, query: invalidQuery)
            XCTFail("Creating view with invalid query should have failed")
        } catch {
            // Expected to fail
            XCTAssertTrue(error is SQLServerError)
        }
    }
}
