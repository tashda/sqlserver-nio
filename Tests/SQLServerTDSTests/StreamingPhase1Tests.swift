import XCTest
import NIOCore
@testable import SQLServerTDS

/// Isolated test suite for Step 1.1 - Protocol Extension and Basic Streaming Infrastructure
/// These tests validate the foundation work without depending on complex TDS parsing or networking

final class StreamingPhase1Tests: XCTestCase {

    // MARK: - Step 1.1: TDSRequest Protocol Extension Tests

    func testTDSRequestProtocolHasStreamingProperties() throws {
        // Test that all TDSRequest implementations support the new streaming properties
        let rawSqlRequest = RawSqlRequest(sql: "SELECT 1")
        
        // Verify protocol compliance

        
        // Verify new properties exist and have correct defaults
        XCTAssertFalse(rawSqlRequest.stream, "stream should default to false for backward compatibility")
        XCTAssertNil(rawSqlRequest.onData, "onData should default to nil")
        
        // Verify existing properties still work (but are nil by default)
        XCTAssertNil(rawSqlRequest.onRow, "onRow should be nil by default")
        XCTAssertNil(rawSqlRequest.onMetadata, "onMetadata should be nil by default")
        XCTAssertNil(rawSqlRequest.onDone, "onDone should be nil by default")
    }

    func testRawSqlRequestStreamingModeEnabled() throws {
        var receivedData: [TDSData] = []
        let onDataCallback: (TDSData) -> Void = { data in
            receivedData.append(data)
        }

        let streamingRequest = RawSqlRequest(
            sql: "SELECT * FROM large_table",
            stream: true,
            onData: onDataCallback
        )

        // Verify streaming mode is properly enabled
        XCTAssertTrue(streamingRequest.stream, "stream should be true when explicitly set")
        XCTAssertNotNil(streamingRequest.onData, "onData should be set when provided")
        
        // Verify SQL is accessible
        XCTAssertEqual(streamingRequest.sql, "SELECT * FROM large_table")
        
        // Test that callback is stored (we can't test execution without full TDS stack)
        XCTAssertNotNil(streamingRequest.onData, "onData callback should be stored")
    }

    func testRawSqlRequestBatchModeUnchanged() throws {
        let batchRequest = RawSqlRequest(
            sql: "SELECT COUNT(*) FROM table",
            stream: false,  // Explicitly false
            onRow: { row in
                // Traditional batch processing callback
            }
        )

        // Verify batch mode behavior
        XCTAssertFalse(batchRequest.stream, "stream should be false in batch mode")
        XCTAssertNil(batchRequest.onData, "onData should be nil in batch mode")
        XCTAssertNotNil(batchRequest.onRow, "onRow should work in batch mode")
        XCTAssertEqual(batchRequest.sql, "SELECT COUNT(*) FROM table")
    }

    func testAllTDSRequestImplementationsSupportStreaming() throws {
        // Test all TDSRequest implementations have the new properties
        
        // RawSqlRequest - our main target
        let rawSqlRequest = RawSqlRequest(sql: "SELECT 1", stream: true)
        XCTAssertTrue(rawSqlRequest.stream, "RawSqlRequest should support streaming")
        
        // RpcRequest - should have streaming properties (always false for RPC)
        let rpcMessage = TDSMessages.RpcRequestMessage(
            procedureName: "test_proc",
            parameters: []
        )
        let rpcRequest = RpcRequest(rpcMessage: rpcMessage)
        XCTAssertFalse(rpcRequest.stream, "RpcRequest should have stream=false")
        XCTAssertNil(rpcRequest.onData, "RpcRequest should have onData=nil")
        
        // LoginRequest - should have streaming properties (not applicable for login)
        let loginPayload = TDSMessages.Login7Message(
            username: "test",
            password: "test",
            serverName: "test",
            database: "test"
        )
        let loginRequest = LoginRequest(payload: loginPayload)
        XCTAssertFalse(loginRequest.stream, "LoginRequest should have stream=false")
        XCTAssertNil(loginRequest.onData, "LoginRequest should have onData=nil")
    }

    // MARK: - Node-mssql Compatibility Tests

    func testNodeMssqlStreamingAPICompatibility() throws {
        // This test verifies our API matches node-mssql patterns
        
        // node-mssql equivalent: const request = new Request("SELECT 1", { stream: true })
        let streamingRequest = RawSqlRequest(
            sql: "SELECT * FROM large_table",
            stream: true,
            onData: { data in
                // Process individual data items immediately (like node-mssql row events)
            }
        )
        
        // node-mssql equivalent: const request = new Request("SELECT 1")  // defaults to batch
        let batchRequest = RawSqlRequest(sql: "SELECT * FROM small_table")
        
        // Verify the core difference: streaming vs batch mode
        XCTAssertTrue(streamingRequest.stream, "Should match node-mssql: req.stream = true")
        XCTAssertFalse(batchRequest.stream, "Should match node-mssql: req.stream = false (default)")
        
        // Verify our onData callback provides the same immediate processing as node-mssql events
        XCTAssertNotNil(streamingRequest.onData, "Should match node-mssql: row events for streaming")
        XCTAssertNil(batchRequest.onData, "Batch mode doesn't need immediate processing")
    }

    func testBackwardCompatibility() throws {
        // Test that existing code continues to work unchanged
        
        // Existing pattern - should still work
        let existingRequest = RawSqlRequest(
            sql: "SELECT 1",
            onRow: { row in
                // Existing row processing logic
            }
        )
        
        // Verify existing behavior is preserved
        XCTAssertFalse(existingRequest.stream, "Default should be false")
        XCTAssertNil(existingRequest.onData, "No onData callback in existing pattern")
        XCTAssertNotNil(existingRequest.onRow, "Existing onRow should still work")
        XCTAssertEqual(existingRequest.sql, "SELECT 1", "SQL should be accessible")
    }

    // MARK: - Parameter Validation Tests

    func testRawSqlRequestParameterValidation() throws {
        // Test various parameter combinations
        
        // All parameters
        let fullRequest = RawSqlRequest(
            sql: "SELECT 1",
            stream: true,
            onRow: { _ in },
            onMetadata: { _ in },
            onData: { _ in },
            onDone: { _ in },
            onMessage: { _, _ in },
            onReturnValue: { _ in },
            resultPromise: nil
        )
        
        XCTAssertTrue(fullRequest.stream, "All parameters should work")
        XCTAssertNotNil(fullRequest.onData, "onData should be set")
        XCTAssertNotNil(fullRequest.onRow, "onRow should be set")
        
        // Minimal parameters
        let minimalRequest = RawSqlRequest(sql: "SELECT 1")
        XCTAssertFalse(minimalRequest.stream, "Default should be false")
        XCTAssertNil(minimalRequest.onData, "Default should be nil")
    }

    // MARK: - Memory and Performance Tests

    func testStreamingMemoryEfficiencyDesign() throws {
        // This test verifies the design intent for memory efficiency
        // We can't test actual memory usage without full TDS stack, but we can verify the design
        
        let streamingRequest = RawSqlRequest(
            sql: "SELECT * FROM huge_table",
            stream: true,
            onData: { data in
                // In real implementation, this would process data immediately
                // without accumulating in memory (like node-mssql)
            }
        )
        
        // Verify the design supports immediate processing
        XCTAssertTrue(streamingRequest.stream, "Streaming mode should be enabled")
        XCTAssertNotNil(streamingRequest.onData, "Immediate data processing callback should be available")
        
        // In streaming mode, we expect not to accumulate data in the request context
        // This will be verified in Phase 2 when we implement the actual streaming logic
    }
}

// MARK: - Test Infrastructure

extension StreamingPhase1Tests {
    
    /// Helper to create test metadata for potential future use
    private func createTestColumnData() -> TDSTokens.ColMetadataToken.ColumnData {
        return TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .nvarchar,
            length: 100,
            collation: [],
            tableName: nil,
            colName: "test_column",
            precision: nil,
            scale: nil
        )
    }
}