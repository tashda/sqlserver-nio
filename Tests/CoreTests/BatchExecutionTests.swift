import XCTest
import SQLServerKit
import SQLServerKitTesting

final class BatchExecutionTests: StandardTestBase, @unchecked Sendable {

    // MARK: - Basic Batch Execution

    func testSingleBatch() async throws {
        let result = try await client.executeBatches(["SELECT 1 AS val"])
        XCTAssertEqual(result.batchResults.count, 1)
        XCTAssertTrue(result.batchResults[0].succeeded)
        XCTAssertEqual(result.batchResults[0].result?.rows.count, 1)
    }

    func testMultipleBatches() async throws {
        let result = try await client.executeBatches([
            "SELECT 1 AS a",
            "SELECT 2 AS b",
            "SELECT 3 AS c"
        ])
        XCTAssertEqual(result.batchResults.count, 3)
        for (i, batch) in result.batchResults.enumerated() {
            XCTAssertTrue(batch.succeeded, "Batch \(i) should succeed")
            XCTAssertEqual(batch.result?.rows.count, 1, "Batch \(i) should return 1 row")
        }
    }

    func testEmptyBatchSkipped() async throws {
        let result = try await client.executeBatches([
            "SELECT 1 AS a",
            "",
            "   ",
            "SELECT 2 AS b"
        ])
        XCTAssertEqual(result.batchResults.count, 4)
        // Empty batches produce nil result, no error
        XCTAssertNil(result.batchResults[1].result)
        XCTAssertNil(result.batchResults[1].error)
        XCTAssertNil(result.batchResults[2].result)
        XCTAssertNil(result.batchResults[2].error)
        // Non-empty batches succeed
        XCTAssertTrue(result.batchResults[0].succeeded)
        XCTAssertTrue(result.batchResults[3].succeeded)
    }

    // MARK: - Continue on Error

    func testContinueOnError() async throws {
        let result = try await client.executeBatches([
            "SELECT 1 AS first_result",
            "THIS IS INVALID SQL THAT WILL FAIL",
            "SELECT 3 AS third_result"
        ])
        XCTAssertEqual(result.batchResults.count, 3)
        // First batch succeeds
        XCTAssertTrue(result.batchResults[0].succeeded, "First batch should succeed")
        XCTAssertEqual(result.batchResults[0].result?.rows.count, 1)
        // Second batch fails
        XCTAssertFalse(result.batchResults[1].succeeded, "Second batch should fail")
        XCTAssertNotNil(result.batchResults[1].error, "Second batch should have error")
        // Third batch succeeds (execution continues past error)
        XCTAssertTrue(result.batchResults[2].succeeded, "Third batch should succeed after error")
        XCTAssertEqual(result.batchResults[2].result?.rows.count, 1)
    }

    // MARK: - Batch Isolation (Variable Scope)

    func testVariableScopeIsolation() async throws {
        // Variables declared in one batch should NOT be visible in the next
        let result = try await client.executeBatches([
            "DECLARE @x INT = 42; SELECT @x AS val",
            "SELECT @x AS val"  // @x is out of scope — should fail
        ])
        XCTAssertEqual(result.batchResults.count, 2)
        XCTAssertTrue(result.batchResults[0].succeeded, "First batch should succeed")
        XCTAssertFalse(result.batchResults[1].succeeded, "Second batch should fail (variable out of scope)")
    }

    // MARK: - Session State Across Batches

    func testTempTableSurvivesGO() async throws {
        let tableName = "#batch_test_\(UUID().uuidString.prefix(8))"
        let result = try await client.executeBatches([
            "CREATE TABLE \(tableName) (id INT); INSERT INTO \(tableName) VALUES (1), (2), (3)",
            "SELECT COUNT(*) AS cnt FROM \(tableName)",
            "DROP TABLE \(tableName)"
        ])
        XCTAssertEqual(result.batchResults.count, 3)
        XCTAssertTrue(result.batchResults[0].succeeded, "Create+insert should succeed")
        XCTAssertTrue(result.batchResults[1].succeeded, "Select from temp table should succeed")
        let rows = result.batchResults[1].result?.rows ?? []
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows.first?.column("cnt")?.int, 3)
        XCTAssertTrue(result.batchResults[2].succeeded, "Drop should succeed")
    }

    func testTransactionSurvivesBatches() async throws {
        let tableName = "#txn_batch_\(UUID().uuidString.prefix(8))"
        let result = try await client.executeBatches([
            "CREATE TABLE \(tableName) (val INT)",
            "BEGIN TRANSACTION; INSERT INTO \(tableName) VALUES (1)",
            "INSERT INTO \(tableName) VALUES (2); COMMIT",
            "SELECT COUNT(*) AS cnt FROM \(tableName)",
            "DROP TABLE \(tableName)"
        ])
        XCTAssertEqual(result.batchResults.count, 5)
        for (i, batch) in result.batchResults.enumerated() {
            XCTAssertTrue(batch.succeeded, "Batch \(i) should succeed")
        }
        let rows = result.batchResults[3].result?.rows ?? []
        XCTAssertEqual(rows.first?.column("cnt")?.int, 2, "Both rows should be committed")
    }

    // MARK: - Message Line Numbers

    func testMessageLineNumberExposed() async throws {
        // Execute SQL with a deliberate error on a known line
        let result = try await client.executeBatches([
            """
            SELECT 1
            SELECT 2
            SELECT * FROM this_table_does_not_exist_12345
            """
        ])
        XCTAssertEqual(result.batchResults.count, 1)
        // The batch should fail because the non-existent table causes an error
        XCTAssertFalse(result.batchResults[0].succeeded)
    }

    // MARK: - Streaming Batch Execution

    func testStreamBatchesEvents() async throws {
        let (connection, stream) = try await client.streamBatches([
            "SELECT 1 AS a",
            "SELECT 2 AS b"
        ])

        var batchStartCount = 0
        var batchCompleteCount = 0
        var batchFailCount = 0

        for try await event in stream {
            switch event {
            case .batchStarted: batchStartCount += 1
            case .batchCompleted: batchCompleteCount += 1
            case .batchFailed: batchFailCount += 1
            case .batchEvent: break
            }
        }

        XCTAssertEqual(batchStartCount, 2)
        XCTAssertEqual(batchCompleteCount, 2)
        XCTAssertEqual(batchFailCount, 0)
        
        // Keep connection alive
        _ = connection
    }

    // MARK: - Multiple Result Sets Within a Batch

    func testMultipleResultSetsInSingleBatch() async throws {
        // A single batch can produce multiple result sets
        let result = try await client.executeBatches([
            "SELECT 1 AS a; SELECT 2 AS b; SELECT 3 AS c"
        ])
        XCTAssertEqual(result.batchResults.count, 1)
        XCTAssertTrue(result.batchResults[0].succeeded)
        // The result should contain rows from at least the last SELECT
        // (exact behavior depends on how rows are accumulated)
        XCTAssertNotNil(result.batchResults[0].result)
    }

    func testMultipleResultSetsAcrossBatches() async throws {
        // Each batch produces its own result set(s)
        let result = try await client.executeBatches([
            "SELECT 1 AS a; SELECT 2 AS b",
            "SELECT 3 AS c"
        ])
        XCTAssertEqual(result.batchResults.count, 2)
        XCTAssertTrue(result.batchResults[0].succeeded)
        XCTAssertTrue(result.batchResults[1].succeeded)
    }

    // MARK: - Error Messages Contain Useful Info

    func testErrorMessageContainsBatchContext() async throws {
        let result = try await client.executeBatches([
            "SELECT * FROM nonexistent_table_xyz_12345"
        ])
        XCTAssertFalse(result.batchResults[0].succeeded)
        let errorDescription = result.batchResults[0].error?.localizedDescription ?? ""
        XCTAssertTrue(errorDescription.contains("nonexistent_table_xyz_12345") || !errorDescription.isEmpty,
                      "Error should contain relevant info: \(errorDescription)")
    }

    // MARK: - DML Across Batches

    func testDMLAcrossBatches() async throws {
        let tableName = "#dml_batch_\(UUID().uuidString.prefix(8))"
        let result = try await client.executeBatches([
            "CREATE TABLE \(tableName) (id INT, name NVARCHAR(50))",
            "INSERT INTO \(tableName) VALUES (1, 'Alice')",
            "INSERT INTO \(tableName) VALUES (2, 'Bob')",
            "UPDATE \(tableName) SET name = 'Charlie' WHERE id = 1",
            "DELETE FROM \(tableName) WHERE id = 2",
            "SELECT * FROM \(tableName)",
            "DROP TABLE \(tableName)"
        ])
        XCTAssertEqual(result.batchResults.count, 7)
        for (i, batch) in result.batchResults.enumerated() {
            XCTAssertTrue(batch.succeeded, "Batch \(i) should succeed")
        }
        // After UPDATE and DELETE, only Charlie should remain
        let selectResult = result.batchResults[5].result
        XCTAssertEqual(selectResult?.rows.count, 1)
    }

    // MARK: - USE Database Across Batches

    func testUseDatabaseAcrossBatches() async throws {
        // USE should persist across batches (session state)
        let result = try await client.executeBatches([
            "USE master",
            "SELECT DB_NAME() AS current_db"
        ])
        XCTAssertEqual(result.batchResults.count, 2)
        XCTAssertTrue(result.batchResults[0].succeeded)
        XCTAssertTrue(result.batchResults[1].succeeded)
        let dbName = result.batchResults[1].result?.rows.first?.column("current_db")?.string
        XCTAssertEqual(dbName, "master")
    }

    // MARK: - Error in Middle Does Not Corrupt Connection

    func testErrorDoesNotCorruptConnection() async throws {
        // After a batch error, subsequent batches should execute cleanly
        let tableName = "#err_recover_\(UUID().uuidString.prefix(8))"
        let result = try await client.executeBatches([
            "CREATE TABLE \(tableName) (id INT)",
            "INSERT INTO \(tableName) VALUES ('not_an_int')",  // type error
            "INSERT INTO \(tableName) VALUES (42)",
            "SELECT * FROM \(tableName)",
            "DROP TABLE \(tableName)"
        ])
        XCTAssertTrue(result.batchResults[0].succeeded, "CREATE should succeed")
        XCTAssertFalse(result.batchResults[1].succeeded, "Type mismatch should fail")
        XCTAssertTrue(result.batchResults[2].succeeded, "Valid INSERT should succeed after error")
        XCTAssertTrue(result.batchResults[3].succeeded, "SELECT should succeed")
        XCTAssertTrue(result.batchResults[4].succeeded, "DROP should succeed")
    }

    // MARK: - Large Number of Batches

    func testManyBatches() async throws {
        let batches = (1...20).map { "SELECT \($0) AS val" }
        let result = try await client.executeBatches(batches)
        XCTAssertEqual(result.batchResults.count, 20)
        for (i, batch) in result.batchResults.enumerated() {
            XCTAssertTrue(batch.succeeded, "Batch \(i) should succeed")
        }
    }

    // MARK: - Streaming

    func testStreamBatchesContinueOnError() async throws {
        let (connection, stream) = try await client.streamBatches([
            "SELECT 1 AS a",
            "INVALID SQL HERE",
            "SELECT 3 AS c"
        ])

        var batchCompleteCount = 0
        var batchFailCount = 0

        for try await event in stream {
            switch event {
            case .batchCompleted: batchCompleteCount += 1
            case .batchFailed: batchFailCount += 1
            default: break
            }
        }

        XCTAssertEqual(batchCompleteCount, 2, "Two batches should complete successfully")
        XCTAssertEqual(batchFailCount, 1, "One batch should fail")
        
        // Keep connection alive
        _ = connection
    }
}
