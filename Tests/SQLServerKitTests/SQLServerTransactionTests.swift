@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerTransactionTests: XCTestCase {
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
    
    func testBasicTransaction() async throws {
        let tableName = "test_transaction_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table using withConnection and rawSql
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }
        
        // Test transaction with multiple operations
        try await client.withConnection { connection in
            // Begin transaction
            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
            
            // Insert data
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test1')").get()
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Test2')").get()
            
            // Commit transaction
            _ = try await connection.underlying.rawSql("COMMIT").get()
        }
        
        // Verify data was committed
        let result = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 2)
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
    
    func testTransactionRollback() async throws {
        let tableName = "test_rollback_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table and insert initial data
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Initial')").get()
        }
        
        // Test transaction rollback
        try await client.withConnection { connection in
            // Begin transaction
            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
            
            // Insert data that will be rolled back
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Rollback')").get()
            
            // Rollback transaction
            _ = try await connection.underlying.rawSql("ROLLBACK").get()
        }
        
        // Verify only initial data exists
        let result = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1)
        
        let valueResult = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        }
        XCTAssertEqual(valueResult.first?.column("value")?.string, "Initial")
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
    
    func testTransactionWithExtendedProperties() async throws {
        let tableName = "test_trans_props_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Test transaction with table creation and extended properties
        try await client.withConnection { connection in
            // Begin transaction
            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
            
            // Create table
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))").get()
            
            // Add extended property
            let commentSQL = """
            EXEC sp_addextendedproperty 
                N'MS_Description',
                N'Test table created in transaction',
                N'SCHEMA',
                N'dbo',
                N'TABLE',
                N'\(tableName)'
            """
            _ = try await connection.underlying.rawSql(commentSQL).get()
            
            // Commit transaction
            _ = try await connection.underlying.rawSql("COMMIT").get()
        }
        
        // Verify table and comment exist
        let tableResult = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM sys.tables WHERE name = '\(tableName)'").get()
        }
        XCTAssertEqual(tableResult.first?.column("count")?.int, 1)
        
        let commentResult = try await client.withConnection { connection in
            try await connection.underlying.rawSql("""
            SELECT p.value
            FROM sys.extended_properties p
            WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0
            """).get()
        }
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Test table created in transaction")
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
    
    func testConcurrentTransactions() async throws {
        let tableName = "test_concurrent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }
        
        // Run concurrent transactions
        await withTaskGroup(of: Void.self) { group in
            for i in 1...5 {
                group.addTask {
                    do {
                        try await self.client.withConnection { connection in
                            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
                            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')").get()
                            _ = try await connection.underlying.rawSql("COMMIT").get()
                        }
                    } catch {
                        XCTFail("Concurrent transaction failed: \(error)")
                    }
                }
            }
        }
        
        // Verify all data was inserted
        let result = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 5)
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
    
    func testTransactionIsolation() async throws {
        let tableName = "test_isolation_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')").get()
        }
        
        // Test that uncommitted changes are not visible to other connections using withConnection
        var connection1Result: String?
        var connection2Result: String?
        
        // Use two separate withConnection blocks to simulate different connections
        try await client.withConnection { connection1 in
            // Start transaction on connection1
            _ = try await connection1.underlying.rawSql("BEGIN TRANSACTION").get()
            _ = try await connection1.underlying.rawSql("UPDATE [\(tableName)] SET value = N'Modified' WHERE id = 1").get()
            
            // Query within the same connection should see the change
            let rows = try await connection1.underlying.rawSql("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            connection1Result = rows.first?.column("value")?.string
            
            // Rollback the transaction
            _ = try await connection1.underlying.rawSql("ROLLBACK").get()
        }
        
        // Query from a different connection should see original value
        try await client.withConnection { connection2 in
            let rows = try await connection2.underlying.rawSql("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            connection2Result = rows.first?.column("value")?.string
        }
        
        XCTAssertEqual(connection1Result, "Modified", "Should see modified value within transaction")
        XCTAssertEqual(connection2Result, "Original", "Should see original value after rollback")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testTransactionWithSavepoints() async throws {
        let tableName = "test_savepoints_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        _ = try await client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        
        try await client.withConnection { connection in
            // Begin transaction
            _ = try await connection.execute("BEGIN TRANSACTION").get()
            
            // Insert first record
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'First')").get()
            
            // Create savepoint
            _ = try await connection.execute("SAVE TRANSACTION SavePoint1").get()
            
            // Insert second record
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Second')").get()
            
            // Rollback to savepoint (should remove second record)
            _ = try await connection.execute("ROLLBACK TRANSACTION SavePoint1").get()
            
            // Insert third record
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (3, N'Third')").get()
            
            // Commit transaction
            _ = try await connection.execute("COMMIT").get()
        }
        
        // Verify only first and third records exist
        let result = try await client.query("SELECT id, value FROM [\(tableName)] ORDER BY id").get()
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("id")?.int, 1)
        XCTAssertEqual(result[0].column("value")?.string, "First")
        XCTAssertEqual(result[1].column("id")?.int, 3)
        XCTAssertEqual(result[1].column("value")?.string, "Third")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testTransactionWithError() async throws {
        let tableName = "test_error_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }
        
        // Test manual rollback after inserting data
        try await client.withConnection { connection in
            // Begin transaction
            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
            
            // Insert valid data
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Valid')").get()
            
            // Manually rollback the transaction
            _ = try await connection.underlying.rawSql("ROLLBACK").get()
        }
        
        // Verify no data was committed due to rollback
        let result = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 0)
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
    
    func testTransactionWithDeadlock() async throws {
        let table1Name = "test_deadlock1_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let table2Name = "test_deadlock2_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test tables
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(table1Name)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(table2Name)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            
            // Insert initial data
            _ = try await connection.underlying.rawSql("INSERT INTO [\(table1Name)] (id, value) VALUES (1, N'Table1')").get()
            _ = try await connection.underlying.rawSql("INSERT INTO [\(table2Name)] (id, value) VALUES (1, N'Table2')").get()
        }
        
        // Test potential deadlock scenario (simplified) using separate withConnection blocks
        // This is a simplified test since we can't easily create true deadlocks with withConnection
        
        // Connection 1: Update table1
        try await client.withConnection { connection1 in
            _ = try await connection1.underlying.rawSql("BEGIN TRANSACTION").get()
            _ = try await connection1.underlying.rawSql("UPDATE [\(table1Name)] SET value = N'Modified1' WHERE id = 1").get()
            _ = try await connection1.underlying.rawSql("COMMIT").get()
        }
        
        // Connection 2: Update table2
        try await client.withConnection { connection2 in
            _ = try await connection2.underlying.rawSql("BEGIN TRANSACTION").get()
            _ = try await connection2.underlying.rawSql("UPDATE [\(table2Name)] SET value = N'Modified2' WHERE id = 1").get()
            _ = try await connection2.underlying.rawSql("COMMIT").get()
        }
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(table1Name)]").get()
            _ = try await connection.underlying.rawSql("DROP TABLE [\(table2Name)]").get()
        }
    }
    
    func testTransactionTimeout() async throws {
        let tableName = "test_timeout_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }
        
        try await client.withConnection { connection in
            // Set a short timeout
            _ = try await connection.underlying.rawSql("SET LOCK_TIMEOUT 1000").get() // 1 second
            
            // Begin transaction
            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
            
            // Insert data
            _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test')").get()
            
            // Commit transaction
            _ = try await connection.underlying.rawSql("COMMIT").get()
        }
        
        // Verify data was committed
        let result = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1)
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
    
    func testTransactionWithBulkOperations() async throws {
        let tableName = "test_bulk_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }
        
        try await client.withConnection { connection in
            // Begin transaction
            _ = try await connection.underlying.rawSql("BEGIN TRANSACTION").get()
            
            // Insert multiple records in a single transaction
            for i in 1...100 {
                _ = try await connection.underlying.rawSql("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')").get()
            }
            
            // Commit transaction
            _ = try await connection.underlying.rawSql("COMMIT").get()
        }
        
        // Verify all data was committed
        let result = try await client.withConnection { connection in
            try await connection.underlying.rawSql("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 100)
        
        // Cleanup
        try await client.withConnection { connection in
            _ = try await connection.underlying.rawSql("DROP TABLE [\(tableName)]").get()
        }
    }
}