import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Comprehensive TDS Object Type Tests
/// Tests all SQL Server object types using SQLServerClient against live database
final class TDSObjectTypeTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private let logger = Logger(label: "TDSObjectTypeTests")

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: config,
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        await client?.shutdownGracefully()
        try await group?.shutdownGracefully()
    }

    // MARK: - Table Object Tests

    func testTableObjects() async throws {
        logger.info("🔧 Testing table object operations...")

        let tempTableName = "temp_test_table_\(Int.random(in: 1000...9999))"

        // Create table
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                created_at DATETIME DEFAULT GETDATE(),
                price DECIMAL(10,2),
                is_active BIT DEFAULT 1
            )
        """)

        // Insert data
        try await client.query("""
            INSERT INTO \(tempTableName) (name, description, price, is_active)
            VALUES
                ('Test Product 1', 'Test Description 1', 19.99, 1),
                ('Test Product 2', 'Test Description 2', 29.99, 0),
                ('Test Product 3', 'Test Description 3', 39.99, 1)
        """)

        // Query data
        let result = try await client.query("SELECT * FROM \(tempTableName) ORDER BY id")

        XCTAssertEqual(result.count, 3)
        let row = result.first!
        XCTAssertNotNil(row.column("id"))
        XCTAssertNotNil(row.column("name"))
        XCTAssertNotNil(row.column("description"))
        XCTAssertNotNil(row.column("created_at"))
        XCTAssertNotNil(row.column("price"))
        XCTAssertNotNil(row.column("is_active"))

        // Update data
        try await client.query("UPDATE \(tempTableName) SET name = 'Updated Name' WHERE id = 1")

        // Verify update
        let updateResult = try await client.query("SELECT name FROM \(tempTableName) WHERE id = 1")
        XCTAssertEqual(updateResult.count, 1)
        XCTAssertEqual(updateResult.first?.column("name")?.string, "Updated Name")

        // Delete data
        try await client.query("DELETE FROM \(tempTableName) WHERE id = 3")

        // Verify deletion
        let deleteResult = try await client.query("SELECT COUNT(*) as count FROM \(tempTableName)")
        XCTAssertEqual(deleteResult.count, 1)
        XCTAssertEqual(deleteResult.first?.column("count")?.string, "2")

        // Drop table
        try await client.query("DROP TABLE \(tempTableName)")

        logger.info("✅ Table object operations test completed")
    }

    // MARK: - View Object Tests

    func testViewObjects() async throws {
        logger.info("🔧 Testing view object operations...")

        let tempViewName = "temp_test_view_\(Int.random(in: 1000..9999))"

        // Create base table
        let tempTableName = "temp_view_base_\(Int.random(in: 1000..9999))"
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                id INT PRIMARY KEY,
                category VARCHAR(50),
                value DECIMAL(10,2),
                created_date DATE
            )
        """)

        try await client.query("""
            INSERT INTO \(tempTableName) (id, category, value, created_date) VALUES
            (1, 'Category A', 100.50, '2023-01-01'),
            (2, 'Category B', 200.75, '2023-01-02'),
            (3, 'Category A', 150.25, '2023-01-03')
        """)

        // Create view
        try await client.query("""
            CREATE VIEW \(tempViewName) AS
            SELECT
                category,
                COUNT(*) as count,
                SUM(value) as total_value,
                AVG(value) as avg_value,
                MIN(created_date) as earliest_date
            FROM \(tempTableName)
            GROUP BY category
        """)

        // Query view
        let result = try await client.query("SELECT * FROM \(tempViewName) ORDER BY category")

        XCTAssertEqual(result.count, 2)  // Two categories: A and B

        // Clean up
        try await client.query("DROP VIEW \(tempViewName)")
        try await client.query("DROP TABLE \(tempTableName)")

        logger.info("✅ View object operations test completed")
    }

    // MARK: - Stored Procedure Tests

    func testStoredProcedureObjects() async throws {
        logger.info("🔧 Testing stored procedure object operations...")

        let tempProcName = "temp_test_proc_\(Int.random(in: 1000..9999))"

        // Create procedure
        try await client.query("""
            CREATE PROCEDURE \(tempProcName)
                @param1 INT = 1,
                @param2 VARCHAR(50) = 'default'
            AS
            BEGIN
                SELECT
                    @param1 as param1_val,
                    @param2 as param2_val,
                    GETDATE() as current_time,
                    (SELECT COUNT(*) FROM sys.objects) as object_count
            END
        """)

        // Execute with default parameters
        let result1 = try await client.query("EXEC \(tempProcName)")

        XCTAssertEqual(result1.count, 1)
        let row1 = result1.first!
        XCTAssertNotNil(row1.column("param1_val"))
        XCTAssertNotNil(row1.column("param2_val"))
        XCTAssertNotNil(row1.column("current_time"))
        XCTAssertNotNil(row1.column("object_count"))

        // Execute with custom parameters
        let result2 = try await client.query("EXEC \(tempProcName) @param1 = 999, @param2 = 'custom'")

        XCTAssertEqual(result2.count, 1)
        let row2 = result2.first!
        XCTAssertNotNil(row2.column("param1_val"))
        XCTAssertNotNil(row2.column("param2_val"))

        // Drop procedure
        try await client.query("DROP PROCEDURE \(tempProcName)")

        logger.info("✅ Stored procedure object operations test completed")
    }

    // MARK: - Function Object Tests

    func testFunctionObjects() async throws {
        logger.info("🔧 Testing function object operations...")

        let tempFuncName = "temp_test_func_\(Int.random(in: 1000..9999))"

        // Create scalar function
        try await client.query("""
            CREATE FUNCTION \(tempFuncName) (@input INT)
            RETURNS INT
            AS
            BEGIN
                RETURN @input * 2
            END
        """)

        // Test scalar function
        let result1 = try await client.query("SELECT dbo.\(tempFuncName)(21) as doubled_value")

        XCTAssertEqual(result1.count, 1)
        XCTAssertEqual(result1.first?.column("doubled_value")?.string, "42")

        // Create table-valued function
        let tempTVFName = "temp_test_tvf_\(Int.random(in: 1000..9999))"
        try await client.query("""
            CREATE FUNCTION \(tempTVFName) (@max_rows INT)
            RETURNS TABLE
            (
                id INT,
                value VARCHAR(50)
            )
            AS
            BEGIN
                RETURN
                SELECT
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as id,
                    'Value ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as varchar) as value
                FROM sys.objects
                WHERE ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) <= @max_rows
            END
        """)

        // Test table-valued function
        let result2 = try await client.query("SELECT * FROM dbo.\(tempTVFName)(5)")

        XCTAssertEqual(result2.count, 5)

        // Drop functions
        try await client.query("DROP FUNCTION \(tempFuncName)")
        try await client.query("DROP FUNCTION \(tempTVFName)")

        logger.info("✅ Function object operations test completed")
    }

    // MARK: - Index Object Tests

    func testIndexObjects() async throws {
        logger.info("🔧 Testing index object operations...")

        let tempTableName = "temp_index_test_\(Int.random(in: 1000..9999))"
        let tempIndexName = "idx_temp_index_test_\(Int.random(in: 1000..9999))"

        // Create table
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                category VARCHAR(50),
                value DECIMAL(10,2),
                created_at DATETIME DEFAULT GETDATE()
            )
        """)

        // Insert data
        try await client.query("""
            INSERT INTO \(tempTableName) (name, category, value) VALUES
            ('Product A', 'Electronics', 199.99),
            ('Product B', 'Electronics', 299.99),
            ('Product C', 'Books', 49.99),
            ('Product D', 'Electronics', 399.99),
            ('Product E', 'Books', 29.99)
        """)

        // Create index
        try await client.query("""
            CREATE INDEX \(tempIndexName) ON \(tempTableName) (category)
        """)

        // Verify index usage
        let result = try await client.query("""
            SELECT name, value FROM \(tempTableName) WHERE category = 'Electronics'
        """)

        XCTAssertEqual(result.count, 3)

        // Create composite index
        try await client.query("""
            CREATE INDEX idx_temp_composite ON \(tempTableName) (category, value DESC)
        """)

        // Drop index
        try await client.query("DROP INDEX \(tempIndexName) ON \(tempTableName)")
        try await client.query("DROP INDEX idx_temp_composite ON \(tempTableName)")

        // Drop table
        try await client.query("DROP TABLE \(tempTableName)")

        logger.info("✅ Index object operations test completed")
    }

    // MARK: - Trigger Object Tests

    func testTriggerObjects() async throws {
        logger.info("🔧 Testing trigger object operations...")

        let tempTableName = "temp_trigger_test_\(Int.random(in: 1000..9999))"
        let auditTableName = "temp_audit_\(Int.random(in: 1000..9999))"

        // Create tables
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name VARCHAR(100),
                modified_by VARCHAR(100),
                modified_at DATETIME DEFAULT GETDATE()
            )
        """)

        try await client.query("""
            CREATE TABLE \(auditTableName) (
                audit_id INT IDENTITY(1,1) PRIMARY KEY,
                table_name VARCHAR(100),
                operation VARCHAR(10),
                record_id INT,
                old_name VARCHAR(100),
                new_name VARCHAR(100),
                audit_timestamp DATETIME DEFAULT GETDATE()
            )
        """)

        // Create trigger
        let tempTriggerName = "trg_temp_update_audit_\(Int.random(in: 1000..9999))"
        try await client.query("""
            CREATE TRIGGER \(tempTriggerName)
            ON \(tempTableName)
            AFTER UPDATE
            AS
            BEGIN
                INSERT INTO \(auditTableName) (table_name, operation, record_id, old_name, new_name)
                SELECT 'temp_trigger_test', 'UPDATE', i.id, d.name, i.name
                FROM inserted i
                JOIN deleted d ON i.id = d.id
            END
        """)

        // Insert initial data
        try await client.query("INSERT INTO \(tempTableName) (name, modified_by) VALUES ('Initial', 'system')")

        // Update data to trigger trigger
        try await client.query("UPDATE \(tempTableName) SET name = 'Updated', modified_by = 'user' WHERE id = 1")

        // Check audit table
        let auditResult = try await client.query("SELECT * FROM \(auditTableName)")

        XCTAssertEqual(auditResult.count, 1)
        let auditRow = auditResult.first!
        XCTAssertEqual(auditRow.column("operation")?.string, "UPDATE")
        XCTAssertEqual(auditRow.column("old_name")?.string, "Initial")
        XCTAssertEqual(auditRow.column("new_name")?.string, "Updated")

        // Clean up
        try await client.query("DROP TRIGGER \(tempTriggerName) ON \(tempTableName)")
        try await client.query("DROP TABLE \(tempTableName)")
        try await client.query("DROP TABLE \(auditTableName)")

        logger.info("✅ Trigger object operations test completed")
    }

    // MARK: - Constraint Object Tests

    func testConstraintObjects() async throws {
        logger.info("🔧 Testing constraint object operations...")

        let tempTableName = "temp_constraint_test_\(Int.random(in: 1000..9999))"

        // Create table with various constraints
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                id INT NOT NULL PRIMARY KEY,
                email VARCHAR(100) UNIQUE NOT NULL,
                name VARCHAR(50) NOT NULL,
                age INT CHECK (age >= 0 AND age <= 150),
                category VARCHAR(50) DEFAULT 'General',
                created_at DATETIME DEFAULT GETDATE()
            )
        """)

        // Test unique constraint
        try await client.query("INSERT INTO \(tempTableName) (email, name, age) VALUES ('test@example.com', 'Test User', 25)")

        do {
            try await client.query("INSERT INTO \(tempTableName) (email, name, age) VALUES ('test@example.com', 'Test User 2', 30)")
            XCTFail("Should have failed due to unique constraint")
        } catch {
            logger.info("✅ Unique constraint working correctly")
        }

        // Test check constraint
        try await client.query("INSERT INTO \(tempTableName) (email, name, age) VALUES ('test2@example.com', 'Test User 2', 25)")

        do {
            try await client.query("INSERT INTO \(tempTableName) (email, name, age) VALUES ('test3@example.com', 'Test User 3', 200)")
            XCTFail("Should have failed due to check constraint")
        } catch {
            logger.info("✅ Check constraint working correctly")
        }

        // Test NOT NULL constraint
        do {
            try await client.query("INSERT INTO \(tempTableName) (email, age) VALUES ('test4@example.com', 35)")
            XCTFail("Should have failed due to NOT NULL constraint")
        } catch {
            logger.info("✅ NOT NULL constraint working correctly")
        }

        // Verify successful insert
        let result = try await client.query("SELECT COUNT(*) as count FROM \(tempTableName)")
        XCTAssertEqual(result.first?.column("count")?.string, "2")

        // Drop table
        try await client.query("DROP TABLE \(tempTableName)")

        logger.info("✅ Constraint object operations test completed")
    }

    // MARK: - User-Defined Type Tests

    func testUserDefinedTypes() async throws {
        logger.info("🔧 Testing user-defined type operations...")

        let tempTypeName = "temp_test_type_\(Int.random(in: 1000..9999))"

        // Create user-defined type
        try await client.query("""
            CREATE TYPE \(tempTypeName) FROM TABLE (
                id INT,
                product_name VARCHAR(100),
                price DECIMAL(10,2),
                is_available BIT
            )
        """)

        // Create table using the type
        let tempTableName = "temp_udt_table_\(Int.random(in: 1000..9999))"
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                order_id INT PRIMARY KEY,
                order_details \(tempTypeName) READONLY
            )
        """)

        // Insert data
        try await client.query("""
            INSERT INTO \(tempTableName) (order_id, order_details) VALUES
            (1, (1, 'Product 1', 99.99, 1)),
            (2, (2, 'Product 2', 149.99, 0))
        """)

        // Query data
        let result = try await client.query("""
            SELECT
                o.order_id,
                od.product_name,
                od.price,
                od.is_available
            FROM \(tempTableName) o
        """)

        XCTAssertEqual(result.count, 2)
        let row = result.first!
        XCTAssertEqual(row.column("product_name")?.string, "Product 1")
        XCTAssertEqual(row.column("price")?.string, "99.99")

        // Clean up
        try await client.query("DROP TABLE \(tempTableName)")
        try await client.query("DROP TYPE \(tempTypeName)")

        logger.info("✅ User-defined type operations test completed")
    }

    // MARK: - Schema Object Tests

    func testSchemaObjects() async throws {
        logger.info("🔧 Testing schema object operations...")

        let tempSchemaName = "temp_test_schema_\(Int.random(in: 1000..9999))"

        // Create schema
        try await client.query("CREATE SCHEMA \(tempSchemaName)")

        // Create table in schema
        let tempTableName = "\(tempSchemaName).temp_table"
        try await client.query("""
            CREATE TABLE \(tempTableName) (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        """)

        // Insert data
        try await client.query("INSERT INTO \(tempTableName) (id, data) VALUES (1, 'Schema Test Data')")

        // Query data
        let result = try await client.query("SELECT * FROM \(tempTableName)")

        XCTAssertEqual(result.count, 1)
        XCTAssertEqual(result.first?.column("data")?.string, "Schema Test Data")

        // List schemas
        let schemasResult = try await client.query("SELECT name FROM sys.schemas WHERE name LIKE 'temp_%'")

        XCTAssertGreaterThan(schemasResult.count, 0)

        // Clean up
        try await client.query("DROP TABLE \(tempTableName)")
        try await client.query("DROP SCHEMA \(tempSchemaName)")

        logger.info("✅ Schema object operations test completed")
    }

    // MARK: - Database Object Tests

    func testDatabaseObjects() async throws {
        logger.info("🔧 Testing database object operations...")

        // Get current database
        let currentDbResult = try await client.query("SELECT DB_NAME() as current_db")

        XCTAssertGreaterThan(currentDbResult.count, 0)
        let currentDb = currentDbResult.first?.column("current_db")?.string
        XCTAssertNotNil(currentDb)

        // List databases
        let dbListResult = try await client.query("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model') ORDER BY name")

        XCTAssertGreaterThan(dbListResult.count, 0)

        logger.info("✅ Database object operations test completed - Current DB: \(currentDb ?? "unknown")")
    }

    // MARK: - Comprehensive Object Integration Test

    func testAllObjectTypesIntegration() async throws {
        logger.info("🔧 Testing comprehensive object type integration...")

        let uniqueId = Int.random(in: 1000...9999)

        // Create schema
        let schemaName = "integration_test_schema_\(uniqueId)"
        try await client.query("CREATE SCHEMA \(schemaName)")

        // Create user-defined types
        let productTypeName = "\(schemaName).ProductType"
        let orderTypeName = "\(schemaName).OrderType"

        try await client.query("""
            CREATE TYPE \(productTypeName) FROM TABLE (
                id INT,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50)
            )
        """)

        try await client.query("""
            CREATE TYPE \(orderTypeName) FROM TABLE (
                order_id INT,
                customer_name VARCHAR(100),
                order_date DATE,
                status VARCHAR(20)
            )
        """)

        // Create tables
        let productsTable = "\(schemaName).Products"
        let ordersTable = "\(schemaName).Orders"
        let orderDetailsTable = "\(schemaName).OrderDetails"

        try await client.query("""
            CREATE TABLE \(productsTable) (
                id INT IDENTITY(1,1) PRIMARY KEY,
                product_info \(productTypeName),
                created_at DATETIME DEFAULT GETDATE()
            )
        """)

        try await client.query("""
            CREATE TABLE \(ordersTable) (
                order_id INT IDENTITY(1,1) PRIMARY KEY,
                order_info \(orderTypeName),
                total_amount DECIMAL(10,2) DEFAULT 0
            )
        """)

        try await client.query("""
            CREATE TABLE \(orderDetailsTable) (
                detail_id INT IDENTITY(1,1) PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                unit_price DECIMAL(10,2),
                FOREIGN KEY (order_id) REFERENCES \(ordersTable)(order_id)
            )
        """)

        // Create view
        let orderSummaryView = "\(schemaName).OrderSummary"
        try await client.query("""
            CREATE VIEW \(orderSummaryView) AS
            SELECT
                o.order_id,
                o.order_info,
                o.total_amount,
                COUNT(od.detail_id) as item_count,
                SUM(od.quantity * od.unit_price) as calculated_total
            FROM \(ordersTable) o
            JOIN \(orderDetailsTable) od ON o.order_id = od.order_id
            GROUP BY o.order_id, o.order_info, o.total_amount
        """)

        // Create stored procedure
        let procName = "\(schemaName).CreateOrder"
        try await client.query("""
            CREATE PROCEDURE \(procName)
                @customer_name VARCHAR(100),
                @product_info \(productTypeName) READONLY,
                @quantity INT
            AS
            BEGIN
                DECLARE @order_id INT
                DECLARE @total_amount DECIMAL(10,2)

                INSERT INTO \(ordersTable) (order_info, total_amount)
                VALUES (CAST(@customer_name as \(orderTypeName)), 0)

                SET @order_id = SCOPE_IDENTITY()
                SET @total_amount = @product_info.price * @quantity

                UPDATE \(ordersTable)
                SET total_amount = @total_amount
                WHERE order_id = @order_id

                INSERT INTO \(orderDetailsTable) (order_id, product_id, quantity, unit_price)
                VALUES (@order_id, @product_info.id, @quantity, @product_info.price)

                SELECT @order_id as order_id, @total_amount as total_amount
            END
        """)

        // Create function
        let funcName = "\(schemaName).GetTotalOrderValue"
        try await client.query("""
            CREATE FUNCTION \(funcName) (@order_id INT)
            RETURNS DECIMAL(10,2)
            AS
            BEGIN
                DECLARE @total DECIMAL(10,2)
                SELECT @total = SUM(od.quantity * od.unit_price)
                FROM \(orderDetailsTable) od
                WHERE od.order_id = @order_id

                RETURN ISNULL(@total, 0)
            END
        """)

        // Test the complete integration
        // 1. Insert product
        try await client.query("""
            INSERT INTO \(productsTable) (product_info)
            VALUES (1, 'Integration Test Product', 29.99, 'Testing')
        """)

        // 2. Create order using stored procedure
        let procResult = try await client.query("""
            EXEC \(procName)
                @customer_name = 'Test Customer',
                @product_info = (SELECT * FROM \(productsTable) WHERE product_info.id = 1),
                @quantity = 3
        """)

        XCTAssertEqual(procResult.count, 1)

        // 3. Verify using function
        let funcResult = try await client.query("""
            SELECT dbo.\(funcName)(SCOPE_IDENTITY()) as total_value
        """)

        XCTAssertEqual(funcResult.count, 1)
        XCTAssertEqual(funcResult.first?.column("total_value")?.string, "89.97")

        // 4. Query view
        let viewResult = try await client.query("SELECT * FROM \(orderSummaryView)")

        XCTAssertEqual(viewResult.count, 1)

        // 5. Clean up all objects in reverse order of dependencies
        try await client.query("DROP PROCEDURE \(procName)")
        try await client.query("DROP FUNCTION \(funcName)")
        try await client.query("DROP VIEW \(orderSummaryView)")
        try await client.query("DROP TABLE \(orderDetailsTable)")
        try await client.query("DROP TABLE \(ordersTable)")
        try await client.query("DROP TABLE \(productsTable)")
        try await client.query("DROP TYPE \(productTypeName)")
        try await client.query("DROP TYPE \(orderTypeName)")
        try await client.query("DROP SCHEMA \(schemaName)")

        logger.info("✅ Comprehensive object type integration test completed successfully!")
    }
}