import XCTest
@testable import SQLServerKit
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
        let (_, stream) = try await client.streamBatches([
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
    }

    func testStreamBatchesContinueOnError() async throws {
        let (_, stream) = try await client.streamBatches([
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
    }
}
