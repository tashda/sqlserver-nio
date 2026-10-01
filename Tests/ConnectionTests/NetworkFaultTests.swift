import XCTest
import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import SQLServerKit
import SQLServerKitTesting
import SQLServerKitXCTestSupport

/// Network faults injected with Toxiproxy between the driver and SQL Server
/// (`SQLSERVER_TEST_PROXY_URL` is the server through the proxy, `SQLSERVER_TEST_PROXY_CONTROL` the
/// proxy's HTTP API). Each test states the failure it injects and what the driver must do: keep
/// working, fail with a lost-connection error, or recover on the next operation. None may hang.
final class NetworkFaultTests: XCTestCase, @unchecked Sendable {
    private var server: TestServer!
    private var api: URL!
    private var proxy: String!
    private var watchdog: DispatchWorkItem?

    override func setUp() async throws {
        _ = isLoggingConfigured
        server = try requireSQLServerTestServer(TestServer.proxyVariable)
        guard let control = TestServer.proxyControl else {
            if TestServer.isRequired { throw TestServer.Unavailable(TestServer.missingMessage(TestServer.proxyControlVariable)) }
            throw XCTSkip(TestServer.missingMessage(TestServer.proxyControlVariable))
        }
        api = control
        proxy = try await proxyName()
        let name = self.name
        let watchdog = DispatchWorkItem { fatalError("\(name) did not finish within 300 seconds") }
        self.watchdog = watchdog
        DispatchQueue.global().asyncAfter(deadline: .now() + 300, execute: watchdog)
        try await resetProxy()
    }

    override func tearDown() async throws {
        if proxy != nil { try? await resetProxy() }
        watchdog?.cancel()
    }

    // MARK: - Toxiproxy API

    private func request(_ method: String, _ path: String, body: [String: Any]? = nil) async throws -> Data {
        var request = URLRequest(url: api.appendingPathComponent(path))
        request.httpMethod = method
        if let body {
            request.httpBody = try JSONSerialization.data(withJSONObject: body)
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        let (data, response) = try await URLSession.shared.data(for: request)
        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        guard (200..<300).contains(status) else {
            throw SQLServerError.invalidArgument("Toxiproxy \(method) \(path) returned \(status)")
        }
        return data
    }

    /// The proxy in front of the server: the only one, or the one listening on the URL's port.
    private func proxyName() async throws -> String {
        let data = try await request("GET", "proxies")
        let proxies = (try JSONSerialization.jsonObject(with: data) as? [String: [String: Any]]) ?? [:]
        if proxies.count == 1, let name = proxies.keys.first { return name }
        if let match = proxies.first(where: { ($0.value["listen"] as? String)?.hasSuffix(":\(server.port)") == true }) {
            return match.key
        }
        throw XCTSkip("Toxiproxy at \(api!) has no proxy listening on port \(server.port)")
    }

    private func resetProxy() async throws {
        _ = try await request("POST", "reset")
    }

    private func addToxic(_ name: String, type: String, stream: String = "downstream", attributes: [String: Any]) async throws {
        _ = try await request("POST", "proxies/\(proxy!)/toxics", body: [
            "name": name, "type": type, "stream": stream, "toxicity": 1.0, "attributes": attributes,
        ])
    }

    private func setProxyEnabled(_ enabled: Bool) async throws {
        _ = try await request("POST", "proxies/\(proxy!)", body: ["enabled": enabled])
    }

    // MARK: - Connections

    private func connectionConfiguration() -> SQLServerConnection.Configuration {
        var configuration = server.configuration
        configuration.connectTimeoutSeconds = 20
        return configuration
    }

    private func connect() async throws -> SQLServerConnection {
        try await SQLServerConnection.connect(configuration: connectionConfiguration())
    }

    private static func largeResultSQL(rows: Int) -> String {
        """
        WITH n AS (
            SELECT TOP (\(rows)) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
            FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
        )
        SELECT i, REPLICATE(N'r', 200) AS payload FROM n;
        """
    }

    // MARK: - Tests

    func testHighLatencyLinkStillQueriesAndCancels() async throws {
        let connection = try await connect()
        defer { Task { try? await connection.close() } }
        try await addToxic("latency-down", type: "latency", attributes: ["latency": 150, "jitter": 50])
        try await addToxic("latency-up", type: "latency", stream: "upstream", attributes: ["latency": 150, "jitter": 50])

        let rows = try await connection.query("SELECT 42 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 42)

        let task = Task { try await connection.execute("WAITFOR DELAY '00:00:30';") }
        try await Task.sleep(for: .milliseconds(500))
        task.cancel()
        _ = try? await task.value
        let after = try await connection.query("SELECT 43 AS v;")
        XCTAssertEqual(after.first?.column("v")?.int, 43)
    }

    func testCancellingLargeResultOnSlowLinkDrainsAndKeepsConnection() async throws {
        let connection = try await connect()
        defer { Task { try? await connection.close() } }
        // ~200 KB/s: the rows SQL Server already sent take longer to drain
        // than the cancellation acknowledgement timeout, while data keeps
        // arriving. The connection must survive.
        try await addToxic("slow", type: "bandwidth", attributes: ["rate": 200])

        var seen = 0
        for try await event in connection.streamQuery(Self.largeResultSQL(rows: 400_000)) {
            if case .row = event {
                seen += 1
                if seen == 50 { break }
            }
        }
        let rows = try await connection.query("SELECT 7 AS v;")
        XCTAssertEqual(rows.first?.column("v")?.int, 7)
    }

    func testResetDuringResultIsReportedAsLostConnection() async throws {
        let connection = try await connect()
        defer { Task { try? await connection.close() } }
        // Reset the TCP connection after ~64 KB of the result.
        try await addToxic("reset", type: "limit_data", attributes: ["bytes": 64 * 1024])
        do {
            _ = try await connection.query(Self.largeResultSQL(rows: 50_000))
            XCTFail("The query should fail when the connection is reset")
        } catch let error as SQLServerError {
            XCTAssertTrue(error.isConnectionLost, "Expected a lost connection, got \(error)")
        }
        XCTAssertTrue(connection.isClosed)
    }

    func testSilentNetworkDuringCancellationClosesConnectionWithinDeadline() async throws {
        let connection = try await connect()
        defer { Task { try? await connection.close() } }
        let task = Task { try await connection.execute("WAITFOR DELAY '00:01:00';") }
        try await Task.sleep(for: .milliseconds(500))
        // Everything after this point is swallowed: the ATTENTION never
        // reaches SQL Server and no acknowledgement can come back.
        try await addToxic("blackhole-up", type: "timeout", stream: "upstream", attributes: ["timeout": 0])
        try await addToxic("blackhole-down", type: "timeout", attributes: ["timeout": 0])
        let started = ContinuousClock.now
        task.cancel()
        _ = try? await task.value
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(25), "The acknowledgement deadline must bound the wait")
        XCTAssertTrue(connection.isClosed, "A connection with an unacknowledged cancellation must be closed")
    }

    func testPoolRecoversAfterEveryConnectionIsDropped() async throws {
        var configuration = SQLServerClient.Configuration(connection: connectionConfiguration())
        configuration.poolConfiguration.maximumConcurrentConnections = 3
        let client = try await SQLServerClient.connect(configuration: configuration, numberOfThreads: 1)
        defer { Task { try? await client.shutdownGracefully() } }
        // Warm several sessions.
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<3 { group.addTask { _ = try await client.query("SELECT 1;") } }
            try await group.waitForAll()
        }
        // Drop every connection, as a failover or server restart would.
        try await setProxyEnabled(false)
        try await Task.sleep(for: .milliseconds(300))
        try await setProxyEnabled(true)

        for value in 1...5 {
            let rows = try await client.query("SELECT \(value) AS v;")
            XCTAssertEqual(rows.first?.column("v")?.int, value, "Idle sessions that were dropped must be replaced, not handed out")
        }
    }

    func testServerUnreachableDuringLoginFailsWithinConnectTimeout() async throws {
        try await addToxic("blackhole-up", type: "timeout", stream: "upstream", attributes: ["timeout": 0])
        var configuration = connectionConfiguration()
        configuration.connectTimeoutSeconds = 3
        configuration.retryConfiguration = .init(maximumAttempts: 1)
        let started = ContinuousClock.now
        do {
            let connection = try await SQLServerConnection.connect(configuration: configuration)
            try? await connection.close()
            XCTFail("Login through a silent network must fail")
        } catch let error as SQLServerError {
            guard case .timeout = error else { return XCTFail("Expected a login timeout, got \(error)") }
        }
        XCTAssertLessThan(ContinuousClock.now - started, .seconds(8))
    }
}
