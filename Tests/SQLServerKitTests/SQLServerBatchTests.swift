@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerBatchTests: XCTestCase {
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
    
    func testExecuteScriptWithGOSeparators() async throws {
        let tableName = "test_go_batch_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let script = """
        CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))
        GO
        INSERT INTO [\(tableName)] (id, name) VALUES (1, N'First')
        GO
        INSERT INTO [\(tableName)] (id, name) VALUES (2, N'Second')
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 3, "Should execute 3 batches")
        
        // Verify data was inserted
        let result = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 2)
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testExecuteSeparateBatches() async throws {
        let tableName = "test_separate_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let statements = [
            "CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(100))",
            "INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Batch1')",
            "INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Batch2')",
            "UPDATE [\(tableName)] SET value = N'Updated' WHERE id = 1"
        ]
        
        let results = try await client.executeSeparateBatches(statements)
        XCTAssertEqual(results.count, 4, "Should execute 4 batches")
        
        // Verify final state
        let result = try await client.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        XCTAssertEqual(result.first?.column("value")?.string, "Updated")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testBatchWithComments() async throws {
        let tableName = "test_batch_comments_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let script = """
        -- Create a test table
        CREATE TABLE [\(tableName)] (
            id INT PRIMARY KEY,
            description NVARCHAR(100)
        )
        GO
        -- Add a comment to the table
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'Test table for batch comments',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(tableName)'
        GO
        -- Insert test data
        INSERT INTO [\(tableName)] (id, description) VALUES (1, N'Test record')
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 3, "Should execute 3 batches")
        
        // Verify table comment
        let commentResult = try await client.query("""
        SELECT p.value
        FROM sys.extended_properties p
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0
        """).get()
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Test table for batch comments")
        
        // Verify data
        let dataResult = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(dataResult.first?.column("count")?.int, 1)
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testBatchErrorHandling() async throws {
        let tableName = "test_batch_error_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let script = """
        CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))
        GO
        INSERT INTO [\(tableName)] (id, name) VALUES (1, N'Valid')
        GO
        INSERT INTO [\(tableName)] (id, name) VALUES (1, N'Duplicate') -- This should fail
        GO
        """
        
        do {
            _ = try await client.executeScript(script)
            XCTFail("Should have thrown an error due to duplicate key")
        } catch {
            // Expected to fail
            XCTAssertTrue(error.localizedDescription.contains("duplicate") || 
                         error.localizedDescription.contains("PRIMARY KEY") ||
                         error.localizedDescription.contains("violation") ||
                         error.localizedDescription.contains("already an object named"))
        }
        
        // Verify table was created and first insert succeeded
        let result = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 1)
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testComplexBatchWithTransaction() async throws {
        let tableName = "test_complex_batch_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let script = """
        BEGIN TRANSACTION
        GO
        CREATE TABLE [\(tableName)] (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            created_date DATETIME2 DEFAULT GETDATE()
        )
        GO
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'Complex batch test table',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(tableName)'
        GO
        EXEC sp_addextendedproperty 
            N'MS_Description',
            N'Primary key for the table',
            N'SCHEMA',
            N'dbo',
            N'TABLE',
            N'\(tableName)',
            N'COLUMN',
            N'id'
        GO
        INSERT INTO [\(tableName)] (name) VALUES (N'Test Record 1')
        INSERT INTO [\(tableName)] (name) VALUES (N'Test Record 2')
        GO
        COMMIT
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 6, "Should execute 6 batches")
        
        // Verify table exists and has data
        let dataResult = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(dataResult.first?.column("count")?.int, 2)
        
        // Verify table comment
        let tableCommentResult = try await client.query("""
        SELECT p.value
        FROM sys.extended_properties p
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0
        """).get()
        XCTAssertEqual(tableCommentResult.first?.column("value")?.string, "Complex batch test table")
        
        // Verify column comment
        let columnCommentResult = try await client.query("""
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'id'
        """).get()
        XCTAssertEqual(columnCommentResult.first?.column("value")?.string, "Primary key for the table")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testBatchWithStoredProcedure() async throws {
        let tableName = "test_batch_proc_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let procName = "usp_test_batch_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        let script = """
        CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))
        GO
        CREATE PROCEDURE [\(procName)]
            @id INT,
            @name NVARCHAR(50)
        AS
        BEGIN
            INSERT INTO [\(tableName)] (id, name) VALUES (@id, @name)
        END
        GO
        EXEC [\(procName)] @id = 1, @name = N'From Procedure'
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 3, "Should execute 3 batches")
        
        // Verify data was inserted by procedure
        let result = try await client.query("SELECT name FROM [\(tableName)] WHERE id = 1").get()
        XCTAssertEqual(result.first?.column("name")?.string, "From Procedure")
        
        // Cleanup
        _ = try await client.execute("DROP PROCEDURE [\(procName)]").get()
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testBatchWithView() async throws {
        let tableName = "test_batch_view_table_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let viewName = "test_batch_view_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        let script = """
        CREATE TABLE [\(tableName)] (
            id INT PRIMARY KEY,
            name NVARCHAR(50),
            active BIT DEFAULT 1
        )
        GO
        INSERT INTO [\(tableName)] (id, name, active) VALUES (1, N'Active Record', 1)
        INSERT INTO [\(tableName)] (id, name, active) VALUES (2, N'Inactive Record', 0)
        GO
        CREATE VIEW [\(viewName)] AS
        SELECT id, name FROM [\(tableName)] WHERE active = 1
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 3, "Should execute 3 batches")
        
        // Verify view returns only active records
        let result = try await client.query("SELECT COUNT(*) as count FROM [\(viewName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 1)
        
        let nameResult = try await client.query("SELECT name FROM [\(viewName)]").get()
        XCTAssertEqual(nameResult.first?.column("name")?.string, "Active Record")
        
        // Cleanup
        _ = try await client.execute("DROP VIEW [\(viewName)]").get()
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testBatchWithTrigger() async throws {
        let tableName = "test_batch_trigger_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let auditTableName = "test_batch_audit_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let triggerName = "tr_test_batch_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        let script = """
        CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))
        GO
        CREATE TABLE [\(auditTableName)] (audit_id INT IDENTITY(1,1), table_id INT, action NVARCHAR(10))
        GO
        CREATE TRIGGER [\(triggerName)]
        ON [\(tableName)]
        AFTER INSERT
        AS
        BEGIN
            INSERT INTO [\(auditTableName)] (table_id, action)
            SELECT id, 'INSERT' FROM inserted
        END
        GO
        INSERT INTO [\(tableName)] (id, name) VALUES (1, N'Test Record')
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 4, "Should execute 4 batches")
        
        // Verify trigger fired and audit record was created
        let auditResult = try await client.query("SELECT COUNT(*) as count FROM [\(auditTableName)]").get()
        XCTAssertEqual(auditResult.first?.column("count")?.int, 1)
        
        let actionResult = try await client.query("SELECT action FROM [\(auditTableName)] WHERE table_id = 1").get()
        XCTAssertEqual(actionResult.first?.column("action")?.string, "INSERT")
        
        // Cleanup
        _ = try await client.execute("DROP TRIGGER [\(triggerName)]").get()
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
        _ = try await client.execute("DROP TABLE [\(auditTableName)]").get()
    }
    
    func testBatchWithIndex() async throws {
        let tableName = "test_batch_index_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let indexName = "ix_test_batch_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        let script = """
        CREATE TABLE [\(tableName)] (
            id INT PRIMARY KEY,
            name NVARCHAR(50),
            category NVARCHAR(20),
            created_date DATETIME2 DEFAULT GETDATE()
        )
        GO
        CREATE INDEX [\(indexName)] ON [\(tableName)] (category, name)
        GO
        INSERT INTO [\(tableName)] (id, name, category) VALUES (1, N'Test1', N'CategoryA')
        INSERT INTO [\(tableName)] (id, name, category) VALUES (2, N'Test2', N'CategoryB')
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 3, "Should execute 3 batches")
        
        // Verify index was created
        let indexResult = try await client.query("""
        SELECT COUNT(*) as count 
        FROM sys.indexes 
        WHERE object_id = OBJECT_ID('[\(tableName)]') AND name = '\(indexName)'
        """).get()
        XCTAssertEqual(indexResult.first?.column("count")?.int, 1)
        
        // Verify data was inserted
        let dataResult = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(dataResult.first?.column("count")?.int, 2)
        
        // Cleanup
        _ = try await client.execute("DROP INDEX [\(indexName)] ON [\(tableName)]").get()
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testBatchWithConstraints() async throws {
        let parentTableName = "test_batch_parent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let childTableName = "test_batch_child_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let fkName = "fk_test_batch_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        let script = """
        CREATE TABLE [\(parentTableName)] (
            id INT PRIMARY KEY,
            name NVARCHAR(50) UNIQUE
        )
        GO
        CREATE TABLE [\(childTableName)] (
            id INT PRIMARY KEY,
            parent_id INT,
            description NVARCHAR(100),
            CONSTRAINT [\(fkName)] FOREIGN KEY (parent_id) REFERENCES [\(parentTableName)](id)
        )
        GO
        INSERT INTO [\(parentTableName)] (id, name) VALUES (1, N'Parent Record')
        GO
        INSERT INTO [\(childTableName)] (id, parent_id, description) VALUES (1, 1, N'Child Record')
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 4, "Should execute 4 batches")
        
        // Verify foreign key constraint exists
        let fkResult = try await client.query("""
        SELECT COUNT(*) as count 
        FROM sys.foreign_keys 
        WHERE name = '\(fkName)'
        """).get()
        XCTAssertEqual(fkResult.first?.column("count")?.int, 1)
        
        // Verify data integrity
        let childResult = try await client.query("""
        SELECT c.description, p.name as parent_name
        FROM [\(childTableName)] c
        JOIN [\(parentTableName)] p ON c.parent_id = p.id
        """).get()
        XCTAssertEqual(childResult.first?.column("description")?.string, "Child Record")
        XCTAssertEqual(childResult.first?.column("parent_name")?.string, "Parent Record")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(childTableName)]").get()
        _ = try await client.execute("DROP TABLE [\(parentTableName)]").get()
    }
    
    func testBatchWithVariables() async throws {
        let tableName = "test_batch_vars_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        let script = """
        DECLARE @TableName NVARCHAR(128) = N'\(tableName)'
        DECLARE @RecordCount INT = 5
        GO
        CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))
        GO
        DECLARE @i INT = 1
        WHILE @i <= 5
        BEGIN
            INSERT INTO [\(tableName)] (id, name) VALUES (@i, N'Record ' + CAST(@i AS NVARCHAR(10)))
            SET @i = @i + 1
        END
        GO
        """
        
        let results = try await client.executeScript(script)
        XCTAssertEqual(results.count, 3, "Should execute 3 batches")
        
        // Verify all records were inserted
        let result = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(result.first?.column("count")?.int, 5)
        
        // Verify record names
        let nameResult = try await client.query("SELECT name FROM [\(tableName)] WHERE id = 3").get()
        XCTAssertEqual(nameResult.first?.column("name")?.string, "Record 3")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testEmptyBatchHandling() async throws {
        let script = """
        -- This is just a comment
        GO
        
        GO
        SELECT 1 as test_value
        GO
        -- Another comment
        GO
        """
        
        let results = try await client.executeScript(script)
        // Should only execute the SELECT statement, empty batches should be filtered out
        XCTAssertEqual(results.count, 1, "Should execute only 1 non-empty batch")
        
        // Verify the SELECT worked
        if let result = results.first {
            XCTAssertEqual(result.rows.count, 1)
        }
    }
}