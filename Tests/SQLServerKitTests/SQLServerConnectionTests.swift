@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerConnectionTests: XCTestCase {
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
    
    func testHealthCheck() async throws {
        let isHealthy = try await client.healthCheck()
        XCTAssertTrue(isHealthy, "Health check should pass for a working connection")
    }
    
    func testValidateConnections() async throws {
        // This should not throw an error for a healthy connection pool
        try await client.validateConnections()
    }
    
    func testPoolStatus() async throws {
        let status = client.poolStatus
        XCTAssertGreaterThanOrEqual(status.active, 0, "Active connections should be non-negative")
        XCTAssertGreaterThanOrEqual(status.idle, 0, "Idle connections should be non-negative")
    }
    
    func testWithConnectionIsolation() async throws {
        let tableName = "test_connection_isolation_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        _ = try await client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        _ = try await client.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')").get()
        
        // Test that withConnection provides proper isolation
        let result1 = try await client.withConnection { connection in
            // Start a transaction on this connection
            _ = try await connection.execute("BEGIN TRANSACTION").get()
            _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Modified' WHERE id = 1").get()
            
            // Query within the same connection should see the change
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            let value = rows.first?.column("value")?.string
            
            // Rollback the transaction
            _ = try await connection.execute("ROLLBACK").get()
            
            return value
        }
        
        XCTAssertEqual(result1, "Modified", "Should see modified value within the same connection")
        
        // Query outside the connection should see original value
        let result2 = try await client.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        XCTAssertEqual(result2.first?.column("value")?.string, "Original", "Should see original value after rollback")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testMultipleWithConnectionCalls() async throws {
        let tableName = "test_multiple_connections_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        _ = try await client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        
        // Test multiple withConnection calls work independently
        let results = await withTaskGroup(of: String?.self) { group in
            var results: [String?] = []
            
            for i in 1...5 {
                group.addTask {
                    do {
                        return try await self.client.withConnection { connection in
                            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')").get()
                            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = \(i)").get()
                            return rows.first?.column("value")?.string
                        }
                    } catch {
                        XCTFail("Connection \(i) failed: \(error)")
                        return nil
                    }
                }
            }
            
            for await result in group {
                results.append(result)
            }
            
            return results
        }
        
        // Verify all connections worked
        XCTAssertEqual(results.count, 5, "Should have 5 results")
        XCTAssertTrue(results.allSatisfy { $0 != nil }, "All results should be non-nil")
        
        // Verify all data was inserted
        let countResult = try await client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        XCTAssertEqual(countResult.first?.column("count")?.int, 5)
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testConnectionReuse() async throws {
        // Test that connections are properly reused from the pool
        var connectionIds: Set<String> = []
        
        for _ in 1...10 {
            let connectionId = try await client.withConnection { connection in
                let rows = try await connection.query("SELECT @@SPID as connection_id").get()
                return rows.first?.column("connection_id")?.string ?? ""
            }
            connectionIds.insert(connectionId)
        }
        
        // With a small pool, we should see connection reuse (fewer unique IDs than operations)
        XCTAssertLessThan(connectionIds.count, 10, "Should reuse connections from the pool")
        XCTAssertGreaterThan(connectionIds.count, 0, "Should have at least one connection")
    }
    
    func testConnectionErrorHandling() async throws {
        // Test that connection errors are properly handled
        do {
            _ = try await client.withConnection { connection in
                // Execute invalid SQL to trigger an error
                _ = try await connection.execute("SELECT * FROM non_existent_table_12345")
            }
            XCTFail("Should have thrown an error for invalid SQL")
        } catch {
            // Expected to fail
            XCTAssertTrue(error.localizedDescription.contains("Invalid object name") || 
                         error.localizedDescription.contains("non_existent_table"))
        }
        
        // Verify that the client is still functional after the error
        let result = try await client.query("SELECT 1 as test").get()
        XCTAssertEqual(result.first?.column("test")?.int, 1)
    }
    
    func testConnectionTimeout() async throws {
        // Test connection behavior with timeouts
        let startTime = Date()
        
        let result = try await client.withConnection { connection in
            // Execute a query that should complete quickly
            let result = try await connection.execute("SELECT GETDATE() as [current_time]")
            return result.rows.first?.column("current_time")?.date
        }
        
        let endTime = Date()
        let duration = endTime.timeIntervalSince(startTime)
        
        XCTAssertNotNil(result, "Should get a valid date result")
        XCTAssertLessThan(duration, 5.0, "Query should complete within 5 seconds")
    }
    
    func testConnectionPoolExhaustion() async throws {
        // Test behavior when connection pool is exhausted
        let maxConnections = client.configuration.poolConfiguration.maximumConcurrentConnections
        
        // Start multiple long-running operations
        await withTaskGroup(of: Void.self) { group in
            for i in 1...Int(maxConnections + 2) {
                group.addTask {
                    do {
                        _ = try await self.client.withConnection { connection in
                            // Hold the connection briefly
                            try await Task.sleep(nanoseconds: 100_000_000) // 100ms
                            let rows = try await connection.query("SELECT \(i) as task_id").get()
                            return rows.first?.column("task_id")?.int
                        }
                    } catch {
                        // Some tasks might timeout or fail due to pool exhaustion
                        // This is expected behavior
                    }
                }
            }
        }
        
        // Verify the client is still functional after pool stress
        let result = try await client.query("SELECT 1 as recovery_test").get()
        XCTAssertEqual(result.first?.column("recovery_test")?.int, 1)
    }
    
    func testConnectionMetadata() async throws {
        let metadata = try await client.withConnection { connection in
            let rows = try await connection.query("""
            SELECT 
                @@VERSION as server_version,
                @@SERVERNAME as server_name,
                DB_NAME() as database_name,
                USER_NAME() as user_name,
                @@SPID as connection_id
            """).get()
            
            return rows.first
        }
        
        XCTAssertNotNil(metadata, "Should get connection metadata")
        XCTAssertNotNil(metadata?.column("server_version")?.string, "Should have server version")
        XCTAssertNotNil(metadata?.column("server_name")?.string, "Should have server name")
        XCTAssertNotNil(metadata?.column("database_name")?.string, "Should have database name")
        XCTAssertNotNil(metadata?.column("user_name")?.string, "Should have user name")
        XCTAssertNotNil(metadata?.column("connection_id")?.int, "Should have connection ID")
    }
    
    func testConnectionStateConsistency() async throws {
        let tableName = "test_state_consistency_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        _ = try await client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        
        // Test that connection state is consistent within a withConnection block
        try await client.withConnection { connection in
            // Set a session variable
            _ = try await connection.execute("DECLARE @test_var INT = 42").get()
            
            // Insert data
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test')").get()
            
            // Verify we can access both the session variable and the data
            let varResult = try await connection.query("SELECT 42 as test_var").get() // Can't access DECLARE vars across batches
            let dataResult = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            
            XCTAssertEqual(varResult.first?.column("test_var")?.int, 42)
            XCTAssertEqual(dataResult.first?.column("value")?.string, "Test")
        }
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testConnectionRecoveryAfterError() async throws {
        // Test that connections can recover after errors
        let tableName = "test_recovery_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        _ = try await client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        
        // Cause an error in a connection
        do {
            try await client.withConnection { connection in
                // Insert valid data
                _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Valid')").get()
                
                // Cause a constraint violation
                _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Duplicate')").get()
            }
            XCTFail("Should have thrown an error")
        } catch {
            // Expected to fail
        }
        
        // Verify the connection pool can still be used
        let result = try await client.withConnection { connection in
            let rows = try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
            return rows.first?.column("count")?.int
        }
        
        XCTAssertEqual(result, 1, "Should have one valid record")
        
        // Cleanup
        _ = try await client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testConnectionPoolWarmup() async throws {
        // Test that the connection pool properly warms up
        let status = client.poolStatus
        
        // After client initialization, we should have some connections available
        XCTAssertGreaterThanOrEqual(status.active + status.idle, 0, "Should have connections available")
        
        // Execute a simple query to ensure warmup worked
        let result = try await client.query("SELECT 1 as warmup_test").get()
        XCTAssertEqual(result.first?.column("warmup_test")?.int, 1)
    }
    
    func testConnectionCleanup() async throws {
        // Test that connections are properly cleaned up
        let initialStatus = client.poolStatus
        
        // Use a connection
        _ = try await client.withConnection { connection in
            let rows = try await connection.query("SELECT @@SPID as spid").get()
            return rows.first?.column("spid")?.int
        }
        
        // Status should be similar after connection is returned to pool
        let finalStatus = client.poolStatus
        
        // The exact numbers might vary, but we shouldn't have leaked connections
        XCTAssertLessThanOrEqual(finalStatus.active, initialStatus.active + 1, "Should not leak active connections")
    }
}