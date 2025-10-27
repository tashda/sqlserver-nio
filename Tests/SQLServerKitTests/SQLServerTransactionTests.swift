@testable import SQLServerKit
import XCTest
import NIO
import Logging
import Dispatch

final class SQLServerTransactionTests: XCTestCase {
    var group: EventLoopGroup!
    var baseClient: SQLServerClient!
    var client: SQLServerClient!
    private var snapshotIsolationChecked = false
    
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
    }
    
    private func ensureSnapshotIsolationEnabled() async throws {
        guard !snapshotIsolationChecked else { return }
        snapshotIsolationChecked = true
        // In per-test ephemeral DBs ensure snapshot isolation is enabled when needed
        let databaseRows = try await self.client.query("SELECT DB_NAME() AS db").get()
        let database = databaseRows.first?.column("db")?.string ?? ""
        try await withReliableConnection(client: client) { connection in
            let stateRows = try await connection.query("""
            SELECT snapshot_isolation_state 
            FROM sys.databases 
            WHERE name = N'\(database.replacingOccurrences(of: "'", with: "''"))'
            """)
            let state = stateRows.first?.column("snapshot_isolation_state")?.int ?? 0
            if state != 1 {
                _ = try await connection.execute("ALTER DATABASE [\(database)] SET ALLOW_SNAPSHOT_ISOLATION ON")
            }
        }
    }

    // Helper to run an individual transaction test in an ephemeral DB with DB-scoped client
    private func inTempDb(_ body: @escaping () async throws -> Void) async throws {
        try await withTemporaryDatabase(client: self.baseClient, prefix: "tx") { db in
            let dbClient = try await makeClient(forDatabase: db, using: self.group)
            let prev = self.client; self.client = dbClient
            defer { Task { _ = try? await dbClient.shutdownGracefully().get(); self.client = prev } }
            try await body()
        }
    }
    
    func testBasicTransaction() async throws {
        try await withTemporaryDatabase(client: self.baseClient, prefix: "tx") { db in
            let dbClient = try await makeClient(forDatabase: db, using: self.group)
            let prev = self.client; self.client = dbClient
            defer { Task { _ = try? await dbClient.shutdownGracefully().get(); self.client = prev } }
            let tableName = "test_transaction_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
            _ = try await self.client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            // Transaction
            _ = try await self.client.execute("BEGIN TRANSACTION").get()
            _ = try await self.client.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test1')").get()
            _ = try await self.client.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Test2')").get()
            _ = try await self.client.execute("COMMIT").get()
            let result = try await self.client.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
            XCTAssertEqual(result.first?.column("count")?.int, 2)
        }
    }
    
    func testTransactionRollback() async throws {
        let tableName = "test_rollback_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table and insert initial data
        try await withReliableConnection(client: self.client) { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Initial')")
        }
        
        // Test transaction rollback
        try await withReliableConnection(client: self.client) { connection in
            // Begin transaction
            _ = try await connection.execute("BEGIN TRANSACTION")
            
            // Insert data that will be rolled back
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'Rollback')")
            
            // Rollback transaction
            _ = try await connection.execute("ROLLBACK")
        }
        
        // Verify only initial data exists
        let result = try await withReliableConnection(client: self.client) { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1)
        
        let valueResult = try await withReliableConnection(client: self.client) { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
        }
        XCTAssertEqual(valueResult.first?.column("value")?.string, "Initial")
        
        // Cleanup
        try await withReliableConnection(client: self.client) { connection in
            _ = try await connection.execute("DROP TABLE [\(tableName)]")
        }
    }
    
    func testTransactionWithExtendedProperties() async throws {
        let tableName = "test_trans_props_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Test transaction with table creation and extended properties
        try await withReliableConnection(client: self.client) { connection in
            // Begin transaction
            _ = try await connection.execute("BEGIN TRANSACTION")
            
            // Create table
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, name NVARCHAR(50))")
            
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
            _ = try await connection.execute(commentSQL)
            
            // Commit transaction
            _ = try await connection.execute("COMMIT")
        }
        
        // Verify table and comment exist
        let tableResult = try await withReliableConnection(client: self.client) { connection in
            try await connection.query("SELECT COUNT(*) as count FROM sys.tables WHERE name = '\(tableName)'")
        }
        XCTAssertEqual(tableResult.first?.column("count")?.int, 1)
        
        let commentResult = try await withReliableConnection(client: self.client) { connection in
            try await connection.query("""
            SELECT CAST(p.value AS NVARCHAR(4000)) AS value
            FROM sys.extended_properties p
            WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0
            """)
        }
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Test table created in transaction")
        
        // Cleanup
        try await withReliableConnection(client: self.client) { connection in
            _ = try await connection.execute("DROP TABLE [\(tableName)]")
        }
    }
    
    func testConcurrentTransactions() async throws {
        let tableName = "test_concurrent_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await withReliableConnection(client: self.client) { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
        }
        
        // Run concurrent transactions
        await withTaskGroup(of: Void.self) { group in
            for i in 1...5 {
                group.addTask {
                    do {
                        try await withReliableConnection(client: self.client) { connection in
                            _ = try await connection.execute("BEGIN TRANSACTION")
                            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')")
                            _ = try await connection.execute("COMMIT")
                        }
                    } catch {
                        XCTFail("Concurrent transaction failed: \(error)")
                    }
                }
            }
        }
        
        // Verify all data was inserted
        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]")
        }
        XCTAssertEqual(result.first?.column("count")?.int, 5)
        
        // Cleanup
        try await self.client.withConnection { connection in
            _ = try await connection.execute("DROP TABLE [\(tableName)]")
        }
    }
    
    func testTransactionIsolation() async throws {
        let tableName = "test_isolation_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }
        
        // Test that uncommitted changes are not visible to other connections using withConnection
        var connection1Result: String?
        var connection2Result: String?
        
        // Use two separate withConnection blocks to simulate different connections
        try await withReliableConnection(client: self.client) { connection1 in
            // Start transaction on connection1
            _ = try await connection1.execute("BEGIN TRANSACTION")
            _ = try await connection1.execute("UPDATE [\(tableName)] SET value = N'Modified' WHERE id = 1")
            
            // Query within the same connection should see the change
            let rows = try await connection1.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            connection1Result = rows.first?.column("value")?.string
            
            // Rollback the transaction
            _ = try await connection1.execute("ROLLBACK")
        }
        
        // Query from a different connection should see original value
        try await withReliableConnection(client: self.client) { connection2 in
            let rows = try await connection2.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            connection2Result = rows.first?.column("value")?.string
        }
        
        XCTAssertEqual(connection1Result, "Modified", "Should see modified value within transaction")
        XCTAssertEqual(connection2Result, "Original", "Should see original value after rollback")
        
        // Cleanup
        _ = try await self.client.execute("DROP TABLE [\(tableName)]").get()
    }
    
    func testTransactionWithSavepoints() async throws {
        let tableName = "test_savepoints_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        
        // Create test table
        _ = try await client.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        
        try await self.client.withConnection { connection in
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
        try await inTempDb {
            let tableName = "test_error_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

            // Create test table
            try await withReliableConnection(client: self.client) { connection in
                _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            }

            // Test manual rollback after inserting data
            try await withReliableConnection(client: self.client) { connection in
                // Begin transaction
                _ = try await connection.execute("BEGIN TRANSACTION")

                // Insert valid data
                _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Valid')")

                // Manually rollback the transaction
                _ = try await connection.execute("ROLLBACK")
            }

            // Verify no data was committed due to rollback
            let result = try await withReliableConnection(client: self.client) { connection in
                try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]")
            }
            XCTAssertEqual(result.first?.column("count")?.int, 0)

            // Cleanup
            try await withReliableConnection(client: self.client) { connection in
                _ = try await connection.execute("DROP TABLE [\(tableName)]")
            }
        }
    }
    
    func testTransactionWithDeadlock() async throws {
        let table1Name = "test_deadlock1_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        let table2Name = "test_deadlock2_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

        // Create test tables
        try await inTempDb {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("CREATE TABLE [\(table1Name)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
                _ = try await connection.execute("CREATE TABLE [\(table2Name)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()

                // Insert initial data
                _ = try await connection.execute("INSERT INTO [\(table1Name)] (id, value) VALUES (1, N'Table1')").get()
                _ = try await connection.execute("INSERT INTO [\(table2Name)] (id, value) VALUES (1, N'Table2')").get()
            }

            // Test potential deadlock scenario (simplified) using separate withConnection blocks
            // This is a simplified test since we can't easily create true deadlocks with withConnection

            // Connection 1: Update table1
            try await self.client.withConnection { connection1 in
                _ = try await connection1.execute("BEGIN TRANSACTION").get()
                _ = try await connection1.execute("UPDATE [\(table1Name)] SET value = N'Modified1' WHERE id = 1").get()
                _ = try await connection1.execute("COMMIT").get()
            }

            // Connection 2: Update table2
            try await self.client.withConnection { connection2 in
                _ = try await connection2.execute("BEGIN TRANSACTION").get()
                _ = try await connection2.execute("UPDATE [\(table2Name)] SET value = N'Modified2' WHERE id = 1").get()
                _ = try await connection2.execute("COMMIT").get()
            }

            // Cleanup
            try await self.client.withConnection { connection in
                _ = try await connection.execute("DROP TABLE [\(table1Name)]").get()
                _ = try await connection.execute("DROP TABLE [\(table2Name)]").get()
            }
        }
    }
    
    func testTransactionTimeout() async throws {
        try await inTempDb {
            let tableName = "test_timeout_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

            // Create test table
            try await self.client.withConnection { connection in
                _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            }

            try await self.client.withConnection { connection in
                // Set a short timeout
                _ = try await connection.execute("SET LOCK_TIMEOUT 1000").get() // 1 second

                // Begin transaction
                _ = try await connection.execute("BEGIN TRANSACTION").get()

                // Insert data
                _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Test')").get()

                // Commit transaction
                _ = try await connection.execute("COMMIT").get()
            }

            // Verify data was committed
            let result = try await self.client.withConnection { connection in
                try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
            }
            XCTAssertEqual(result.first?.column("count")?.int, 1)

            // Cleanup
            try await self.client.withConnection { connection in
                _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
            }
        }
    }
    
    func testTransactionWithBulkOperations() async throws {
        try await inTempDb {
            let tableName = "test_bulk_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"

            // Create test table
            try await self.client.withConnection { connection in
                _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            }

            try await self.client.withConnection { connection in
                // Begin transaction
                _ = try await connection.execute("BEGIN TRANSACTION").get()

                // Insert multiple records in a single transaction
                for i in 1...100 {
                    _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (\(i), N'Value\(i)')").get()
                }

                // Commit transaction
                _ = try await connection.execute("COMMIT").get()
            }

            // Verify all data was committed
            let result = try await self.client.withConnection { connection in
                try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
            }
            XCTAssertEqual(result.first?.column("count")?.int, 100)

            // Cleanup
            try await self.client.withConnection { connection in
                _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
            }
        }
    }

    func testReadCommittedPreventsDirtyReads() async throws {
        let tableName = "test_read_committed_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]")
                }
            }
        }
        
        let writer = Task {
            try await withReliableConnection(client: self.client) { connection in
                _ = try await connection.execute("BEGIN TRANSACTION")
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Dirty' WHERE id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                _ = try await connection.execute("ROLLBACK")
            }
        }
        
        try await Task.sleep(nanoseconds: 100_000_000)
        
        let readValue = try await withReliableConnection(client: self.client) { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }
        XCTAssertEqual(readValue, "Original", "READ COMMITTED should block until dirty update rolls back")
        
        _ = try await withTimeout(5) { try await writer.value }
    }
    
    func testReadUncommittedAllowsDirtyReads() async throws {
        let tableName = "test_read_uncommitted_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]")
                }
            }
        }
        
        let writer = Task {
            try await withReliableConnection(client: self.client) { connection in
                _ = try await connection.execute("BEGIN TRANSACTION")
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Dirty' WHERE id = 1")
                try await Task.sleep(nanoseconds: 500_000_000)
                _ = try await connection.execute("ROLLBACK")
            }
        }
        
        try await Task.sleep(nanoseconds: 100_000_000)
        
        let dirtyRead = try await withReliableConnection(client: client) { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            let rows = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            return rows.first?.column("value")?.string
        }
        XCTAssertEqual(dirtyRead, "Dirty", "READ UNCOMMITTED should see uncommitted changes")
        
        _ = try await withTimeout(5) { try await writer.value }
    }
    
    func testRepeatableReadPreventsNonRepeatableReads() async throws {
        let tableName = "test_repeatable_\(UUID().uuidString.prefix(8))"
        try await withReliableConnection(client: client) { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')")
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
                }
            }
        }
        
        let blocker = Task {
            try await withReliableConnection(client: self.client) { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            _ = try await connection.execute("BEGIN TRANSACTION")
            let first = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            try await Task.sleep(nanoseconds: 600_000_000)
            let second = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
            _ = try await connection.execute("COMMIT")
            return (first.first?.column("value")?.string, second.first?.column("value")?.string)
        }
        }
        
        try await Task.sleep(nanoseconds: 100_000_000)
        
        let updateDelay = try await withReliableConnection(client: client) { connection in
            let start = DispatchTime.now()
            _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Updated' WHERE id = 1")
            let elapsed = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            return elapsed
        }
        
        let (firstRead, secondRead) = try await withTimeout(5) { try await blocker.value }
        XCTAssertEqual(firstRead, "Original")
        XCTAssertEqual(secondRead, "Original")
        XCTAssertGreaterThan(updateDelay, 400_000_000 as UInt64, "Update should wait for repeatable read to finish")
        
        let finalValue = try await withReliableConnection(client: client) { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1")
        }
        XCTAssertEqual(finalValue.first?.column("value")?.string, "Updated")
    }
    
    func testSerializablePreventsPhantomInserts() async throws {
        let tableName = "test_serializable_\(UUID().uuidString.prefix(8))"
        try await withReliableConnection(client: client) { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, category NVARCHAR(10))")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, category) VALUES (1, N'A'), (2, N'A')")
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
                }
            }
        }

        let rangeLockTask = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").get()
                _ = try await connection.execute("BEGIN TRANSACTION").get()
                _ = try await connection.query("SELECT COUNT(*) FROM [\(tableName)] WHERE category = N'A'").get()
                try await Task.sleep(nanoseconds: 700_000_000)
                _ = try await connection.execute("COMMIT").get()
            }
        }

        try await Task.sleep(nanoseconds: 200_000_000)

        let insertDelay = try await self.client.withConnection { connection in
            let start = DispatchTime.now()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, category) VALUES (3, N'A')").get()
            let elapsed = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            return elapsed
        }

        _ = try await withTimeout(5) { try await rangeLockTask.value }
        XCTAssertGreaterThan(insertDelay, 300_000_000 as UInt64, "INSERT should have been blocked until SERIALIZABLE transaction finished")

        let countResult = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)] WHERE category = N'A'").get()
        }
        XCTAssertEqual(countResult.first?.column("count")?.int, 3)
    }

    func testConcurrentUpdateBlocksUntilCommit() async throws {
        let tableName = "test_concurrent_update_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')").get()
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
                }
            }
        }

        // Session 1: hold an X lock for ~600ms
        let holder = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("BEGIN TRANSACTION").get()
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Locked' WHERE id = 1").get()
                try await Task.sleep(nanoseconds: 600_000_000)
                _ = try await connection.execute("COMMIT").get()
            }
        }

        // Give the holder a head start
        try await Task.sleep(nanoseconds: 100_000_000)

        // Session 2: this update should block until commit
        let elapsed = try await self.client.withConnection { connection in
            let start = DispatchTime.now()
            _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'Updated' WHERE id = 1").get()
            let end = DispatchTime.now()
            return end.uptimeNanoseconds - start.uptimeNanoseconds
        }

        _ = try await withTimeout(5) { try await holder.value }
        XCTAssertGreaterThan(elapsed, 400_000_000 as UInt64, "Second update should wait for first transaction to commit")

        let final = try await self.client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        }
        XCTAssertEqual(final.first?.column("value")?.string, "Updated")
    }
    
    func testSavepointRollbackWithinTransaction() async throws {
        let tableName = "test_savepoint_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
                }
            }
        }

        try await self.client.withConnection { connection in
            _ = try await connection.execute("BEGIN TRANSACTION")
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Committed')").get()
            _ = try await connection.execute("SAVE TRANSACTION before_temp").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (2, N'ShouldRollback')").get()
            _ = try await connection.execute("ROLLBACK TRANSACTION before_temp").get()
            _ = try await connection.execute("COMMIT").get()
        }

        let result = try await self.client.withConnection { connection in
            try await connection.query("SELECT COUNT(*) as count FROM [\(tableName)]").get()
        }
        XCTAssertEqual(result.first?.column("count")?.int, 1, "Savepoint rollback should remove second row")
    }
    
    func testSnapshotIsolationProvidesStableView() async throws {
        try await ensureSnapshotIsolationEnabled()
        let tableName = "test_snapshot_\(UUID().uuidString.prefix(8))"
        try await self.client.withConnection { connection in
            _ = try await connection.execute("CREATE TABLE [\(tableName)] (id INT PRIMARY KEY, value NVARCHAR(50))").get()
            _ = try await connection.execute("INSERT INTO [\(tableName)] (id, value) VALUES (1, N'Original')").get()
        }
        defer {
            Task {
                try? await client.withConnection { connection in
                    _ = try await connection.execute("DROP TABLE [\(tableName)]").get()
                }
            }
        }
        
        let writer = Task {
            try await self.client.withConnection { connection in
                _ = try await connection.execute("BEGIN TRANSACTION").get()
                _ = try await connection.execute("UPDATE [\(tableName)] SET value = N'NewValue' WHERE id = 1").get()
                try await Task.sleep(nanoseconds: 500_000_000)
                _ = try await connection.execute("COMMIT").get()
            }
        }
        
        try await Task.sleep(nanoseconds: 100_000_000)
        
        let (firstRead, secondRead) = try await self.client.withConnection { connection in
            _ = try await connection.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT").get()
            _ = try await connection.execute("BEGIN TRANSACTION").get()
            let first = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            try await Task.sleep(nanoseconds: 400_000_000)
            let second = try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
            _ = try await connection.execute("COMMIT").get()
            return (first.first?.column("value")?.string, second.first?.column("value")?.string)
        }
        
        _ = try await withTimeout(5) { try await writer.value }
        
        XCTAssertEqual(firstRead, "Original")
        XCTAssertEqual(secondRead, "Original")
        
        let committedValue = try await self.client.withConnection { connection in
            try await connection.query("SELECT value FROM [\(tableName)] WHERE id = 1").get()
        }
        XCTAssertEqual(committedValue.first?.column("value")?.string, "NewValue")
    }
}
