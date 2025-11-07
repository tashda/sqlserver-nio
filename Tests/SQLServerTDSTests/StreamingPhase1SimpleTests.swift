import XCTest
import NIOCore
@testable import SQLServerTDS

/// Simple working tests for Step 1.1 - Protocol Extension and Basic Streaming Infrastructure
/// Focus on testing what we can validate without complex dependencies

final class StreamingPhase1SimpleTests: XCTestCase {

    // MARK: - Step 1.1: TDSRequest Protocol Extension Tests

    func testRawSqlRequestStreamingProperties() throws {
        // Test that RawSqlRequest supports the new streaming properties
        let batchRequest = RawSqlRequest(sql: "SELECT 1")

        // Verify defaults
        XCTAssertFalse(batchRequest.stream, "stream should default to false")
        XCTAssertNil(batchRequest.onData, "onData should default to nil")
        XCTAssertEqual(batchRequest.sql, "SELECT 1")
    }

    func testRawSqlRequestStreamingModeEnabled() throws {
        var callbackCount = 0
        let onDataCallback: (TDSData) -> Void = { _ in
            callbackCount += 1
        }

        let streamingRequest = RawSqlRequest(
            sql: "SELECT * FROM table",
            stream: true,
            onData: onDataCallback
        )

        // Verify streaming mode
        XCTAssertTrue(streamingRequest.stream, "stream should be true when set")
        XCTAssertNotNil(streamingRequest.onData, "onData should be set")
        XCTAssertEqual(streamingRequest.sql, "SELECT * FROM table")
    }

    func testRpcRequestStreamingProperties() throws {
        let rpcMessage = TDSMessages.RpcRequestMessage(
            procedureName: "test_proc",
            parameters: []
        )
        let rpcRequest = RpcRequest(rpcMessage: rpcMessage)

        // RPC requests should always be non-streaming
        XCTAssertFalse(rpcRequest.stream, "RpcRequest should have stream=false")
        XCTAssertNil(rpcRequest.onData, "RpcRequest should have onData=nil")
    }

    func testBackwardCompatibility() throws {
        // Test existing code patterns still work
        let existingRequest = RawSqlRequest(
            sql: "SELECT COUNT(*) FROM users",
            onRow: { row in
                // Existing row processing logic
            }
        )

        // Should work exactly as before
        XCTAssertFalse(existingRequest.stream, "Default behavior preserved")
        XCTAssertNil(existingRequest.onData, "No new features used")
        XCTAssertNotNil(existingRequest.onRow, "Existing callbacks work")
        XCTAssertEqual(existingRequest.sql, "SELECT COUNT(*) FROM users")
    }

    func testNodeMssqlAPICompatibility() throws {
        // Verify our API matches node-mssql patterns

        // node-mssql: request.stream = true
        let streamingRequest = RawSqlRequest(
            sql: "SELECT * FROM large_table",
            stream: true
        )

        // node-mssql: request.stream = false (default)
        let batchRequest = RawSqlRequest(sql: "SELECT * FROM small_table")

        // Verify the core difference exists
        XCTAssertTrue(streamingRequest.stream, "Streaming mode enabled")
        XCTAssertFalse(batchRequest.stream, "Batch mode (default)")
    }

    func testAllParametersWork() throws {
        let fullRequest = RawSqlRequest(
            sql: "SELECT 1",
            stream: true,
            onRow: { _ in },
            onMetadata: { _ in },
            onData: { _ in },
            onDone: { _ in }
        )

        // All parameters should be accepted
        XCTAssertTrue(fullRequest.stream, "Stream parameter works")
        XCTAssertNotNil(fullRequest.onData, "onData parameter works")
        XCTAssertNotNil(fullRequest.onRow, "onRow parameter works")
    }

    func testProtocolCompliance() throws {
        // Verify our main implementation conforms to the extended protocol
        let request = RawSqlRequest(sql: "SELECT 1", stream: true)

        // This should compile and run without issues
        XCTAssertTrue(request.stream, "Protocol extension works")

        // Test we can treat it as TDSRequest
        let tdsRequest: TDSRequest = request
        XCTAssertTrue(tdsRequest.stream, "Protocol compliance verified")
    }
}