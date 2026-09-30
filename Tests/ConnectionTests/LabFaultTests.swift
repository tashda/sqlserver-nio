import XCTest
import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import SQLServerKit
import SQLServerKitTesting

/// Network faults injected with Toxiproxy between the driver and SQL Server
/// (`testlab/testlab.sh faults`). Each test states the failure it injects and
/// what the driver must do: keep working, fail with a lost-connection error,
/// or recover on the next operation. None may hang.
final class LabFaultTests: XCTestCase, @unchecked Sendable {
    private struct Lab {
        let host: String
        let port: Int
        let api: URL
        let proxy: String
        let username: String
        let password: String
    }

    private var lab: Lab!
    private var watchdog: DispatchWorkItem?

    override func setUp() async throws {
        _ = isLoggingConfigured
        guard let host = env("NIO_LAB_FAULT_HOST"),
              let port = env("NIO_LAB_FAULT_PORT").flatMap(Int.init),
              let api = env("NIO_LAB_TOXIPROXY").flatMap(URL.init(string:)),
              let proxy = env("NIO_LAB_FAULT_PROXY"),
              let username = env("NIO_LAB_FAULT_USERNAME"),
              let password = env("NIO_LAB_FAULT_PASSWORD") else {
            if envFlagEnabled("NIO_LAB_REQUIRE") {
                XCTFail("Fault lab is required but NIO_LAB_FAULT_* is not set; run testlab/testlab.sh faults")
            }
            throw XCTSkip("Fault lab not configured (testlab/testlab.sh faults)")
        }
        lab = Lab(host: host, port: port, api: api, proxy: proxy, username: username, password: password)
        let name = self.name
        let watchdog = DispatchWorkItem { fatalError("\(name) did not finish within 300 seconds") }
        self.watchdog = watchdog
        DispatchQueue.global().asyncAfter(deadline: .now() + 300, execute: watchdog)
        try await resetProxy()
    }

    override func tearDown() async throws {
        if lab != nil { try? await resetProxy() }
        watchdog?.cancel()
    }

    // MARK: - Toxiproxy API

    private func request(_ method: String, _ path: String, body: [String: Any]? = nil) async throws {
        var request = URLRequest(url: lab.api.appendingPathComponent(path))
        request.httpMethod = method
        if let body {
            request.httpBody = try JSONSerialization.data(withJSONObject: body)
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        let (_, response) = try await URLSession.shared.data(for: request)
        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        guard (200..<300).contains(status) else {
            throw SQLServerError.invalidArgument("Toxiproxy \(method) \(path) returned \(status)")
        }
    }

    private func resetProxy() async throws {
        try await request("POST", "reset")
    }

    private func addToxic(_ name: String, type: String, stream: String = "downstream", attributes: [String: Any]) async throws {
        try await request("POST", "proxies/\(lab.proxy)/toxics", body: [
            "name": name, "type": type, "stream": stream, "toxicity": 1.0, "attributes": attributes,
        ])
    }

    private func setProxyEnabled(_ enabled: Bool) async throws {
        try await request("POST", "proxies/\(lab.proxy)", body: ["enabled": enabled])
    }

    // MARK: - Connections

    private func connectionConfiguration() -> SQLServerConnection.Configuration {
        var configuration = SQLServerConnection.Configuration(
            hostname: lab.host,
            port: lab.port,
            login: .init(database: "master", authentication: .sqlPassword(username: lab.username, password: lab.password)),
            tlsConfiguration: .trustingServerCertificate
        )
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
