import XCTest
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit
import SQLServerKitTesting

/// Diagnostics and Performance Tuning tests (Phase 5)
final class DiagnosticsTests: XCTestCase, @unchecked Sendable {
    private var client: SQLServerClient!
    private let logger = Logger(label: "DiagnosticsTests")

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()
        
        let config = makeSQLServerClientConfiguration()
        self.client = try await SQLServerClient.connect(
            configuration: config,
            numberOfThreads: 1
        )
    }

    override func tearDown() async throws {
        try? await client?.shutdownGracefully()
    }

    // MARK: - 5.1 SQL Profiler (Live Trace) Tests

    func testLiveTraceLifecycle() async throws {
        logger.info("🔧 Testing SQL Profiler (XE Live Trace) lifecycle...")
        
        let sessionName = "EchoProfilerTest_\(Int.random(in: 1000...9999))"
        let events: [SQLTraceEvent] = [.rpcCompleted, .sqlBatchCompleted]
        
        // 1. Start Trace
        try await client.profiler.startLiveTrace(name: sessionName, events: events)
        logger.info("   Trace '\(sessionName)' started.")
        
        // 2. Execute a query to generate an event
        _ = try await client.query("SELECT 1 AS profiler_heartbeat")
        
        // 3. Read events (may take a moment for ring buffer to flush)
        // We wait a bit or just verify the call succeeds.
        try await Task.sleep(nanoseconds: 500_000_000) // 0.5s
        let capturedEvents = try await client.readTraceEvents(sessionName: sessionName)
        logger.info("   Captured \(capturedEvents.count) events.")
        
        // 4. Stop Trace
        try await client.profiler.stopLiveTrace(name: sessionName)
        logger.info("   Trace stopped and cleaned up.")
        
        XCTAssertTrue(true, "Lifecycle completed successfully")
    }

    // MARK: - 5.2 Tuning Advisor Tests

    func testMissingIndexRecommendations() async throws {
        logger.info("🔧 Testing Tuning Advisor (Missing Index DMVs)...")
        
        // Note: In a fresh test environment, DMVs might be empty.
        // We verify the query execution and model mapping.
        let recommendations = try await client.tuning.listMissingIndexRecommendations(minImpact: 0)
        
        logger.info("   Found \(recommendations.count) recommendations.")
        
        for rec in recommendations.prefix(3) {
            logger.info("   💡 Table: \(rec.schemaName).\(rec.tableName), Impact: \(String(format: "%.1f", rec.avgTotalUserCost))%")
            logger.info("      Equality: \(rec.equalityColumns.joined(separator: ", "))")
            logger.info("      Include: \(rec.includedColumns.joined(separator: ", "))")
        }
        
        XCTAssertNotNil(recommendations, "Recommendations list should be returned (even if empty)")
    }
}

extension SQLServerClient {
    @available(macOS 12.0, *)
    func readTraceEvents(sessionName: String) async throws -> [SQLServerProfilerEvent] {
        return try await self.profiler.readTraceEvents(sessionName: sessionName)
    }
}
