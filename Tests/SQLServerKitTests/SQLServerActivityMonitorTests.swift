import XCTest
import NIO
@testable import SQLServerKit
import SQLServerTDS

final class SQLServerActivityMonitorTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!

    override func setUpWithError() throws {
        try super.setUpWithError()
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_ACTIVITY_MONITOR_TESTS", description: "activity monitor integration tests")
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDownWithError() throws {
        try? client?.shutdownGracefully().wait()
        client = nil
        try group?.syncShutdownGracefully()
        group = nil
        try super.tearDownWithError()
    }

    func testSnapshotBasic() throws {
        let monitor = SQLServerActivityMonitor(client: client)
        let snapshot = try waitForResult(monitor.snapshot(options: .init()), timeout: 30, description: "activity snapshot")
        // Structural assertions only; environment content varies
        XCTAssertNotNil(snapshot.capturedAt)
        XCTAssertNotNil(snapshot.waits)
        XCTAssertNotNil(snapshot.fileIO)
        XCTAssertNotNil(snapshot.expensiveQueries)
    }

    func testKillSessionCancelsQueryWhenEnabled() throws {
        guard envFlagEnabled("TDS_ENABLE_ACTIVITY_MONITOR_KILL") else {
            throw XCTSkip("Skipping kill test; set TDS_ENABLE_ACTIVITY_MONITOR_KILL=1 to enable")
        }

        let monitor = SQLServerActivityMonitor(client: client)
        let loop = group.next()

        // Open a dedicated connection to run a long operation
        let victimConn = try waitForResult(connectSQLServer(on: loop), timeout: 30, description: "connect victim")
        defer { _ = try? waitForResult(victimConn.close(), timeout: 15, description: "close victim") }

        // Determine victim spid
        let spidRows = try waitForResult(victimConn.query("SELECT @@SPID AS spid;"), timeout: 30, description: "read spid")
        let spid = spidRows.first?.column("spid")?.int ?? -1
        XCTAssertGreaterThan(spid, 0)

        // Start a long WAITFOR in the background
        let longOp = victimConn.execute("WAITFOR DELAY '00:00:30';")

        // Give it a moment to register
        usleep(300_000)

        // Kill the victim session from the monitor
        let _: Void = try waitForResult(monitor.killSession(sessionId: spid), timeout: 10, description: "kill session")

        // The long operation should fail
        do {
            _ = try waitForResult(longOp, timeout: 10, description: "await killed long op")
            XCTFail("Expected killed operation to fail")
        } catch {
            // Success path: killed sessions return protocol/connection closed errors
            XCTAssertTrue(true)
        }
    }
}

