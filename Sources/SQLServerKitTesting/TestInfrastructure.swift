import Foundation
import Logging
import NIO
import NIOPosix
import SQLServerTDS
import SQLServerKit

/// Centralized test infrastructure for all SQL Server tests
/// This is the single source of truth for all test utilities and eliminates duplication

// MARK: - Environment Configuration

public enum TestEnvironment: String, CaseIterable {
    case production = "production"
    case staging = "staging"
    case development = "development"
    case local = "local"
    case sql2025 = "sql2025"

    public var displayName: String {
        switch self {
        case .production: return "Production Server"
        case .staging: return "Staging Server"
        case .development: return "Development Server"
        case .local: return "Local Server"
        case .sql2025: return "SQL Server 2025"
        }
    }

    public var configuration: TestEnvironmentConfig {
        switch self {
        case .production, .staging, .development, .local:
            return TestEnvironmentConfig(
                hostname: "localhost",
                port: 1433,
                database: "master",
                username: "sa",
                password: "YourPassword123!"
            )
        case .sql2025:
            return TestEnvironmentConfig(
                hostname: "localhost",
                port: 1433,
                database: "master",
                username: "sa",
                password: "YourPassword123!"
            )
        }
    }
}

public struct TestEnvironmentConfig {
    public let hostname: String
    public let port: Int
    public let database: String
    public let username: String
    public let password: String

    public init(hostname: String, port: Int, database: String, username: String, password: String) {
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username
        self.password = password
    }
}

public class TestEnvironmentManager {
    public static var currentEnvironment: TestEnvironment {
        let envName = ProcessInfo.processInfo.environment["TDS_ENV"] ?? "local"
        return TestEnvironment(rawValue: envName) ?? .local
    }

    public static var currentConfig: TestEnvironmentConfig {
        return currentEnvironment.configuration
    }

    public static func loadEnvironmentVariables() {
        // Use TDS_ENV to select config, but only set each var as a default
        // (overwrite=0) so that shell-provided values take precedence.
        // This allows: TDS_ENV=sql2025 swift test  OR  TDS_HOSTNAME=x swift test
        let config = currentConfig
        setenv("TDS_HOSTNAME", config.hostname, 0)
        setenv("TDS_PORT", String(config.port), 0)
        setenv("TDS_DATABASE", config.database, 0)
        setenv("TDS_USERNAME", config.username, 0)
        setenv("TDS_PASSWORD", config.password, 0)
    }
}

// MARK: - Logging Infrastructure

public let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()

// MARK: - Environment Utilities

public func env(_ name: String) -> String? {
    // First try process environment variables (Xcode test plans, command line)
    if let value = ProcessInfo.processInfo.environment[name] {
        return value
    }
    // Fallback to traditional getenv() (for compatibility)
    return getenv(name).flatMap { String(cString: $0) }
}

public func envFlagEnabled(_ key: String) -> Bool {
    guard let value = env(key) else { return false }
    return value == "1" || value.lowercased() == "true" || value.lowercased() == "yes"
}

// MARK: - Connection Configuration

public func makeSQLServerConnectionConfiguration() -> SQLServerConnection.Configuration {
    // Use centralized environment configuration
    let config = TestEnvironmentManager.currentConfig

    let hostname = env("TDS_HOSTNAME") ?? config.hostname
    let port = env("TDS_PORT").flatMap(Int.init) ?? config.port
    let username = env("TDS_USERNAME") ?? config.username
    let password = env("TDS_PASSWORD") ?? config.password
    let database = env("TDS_DATABASE") ?? config.database

    var cfg = SQLServerConnection.Configuration(
        hostname: hostname,
        port: port,
        login: .init(
            database: database,
            authentication: .sqlPassword(username: username, password: password)
        ),
        tlsConfiguration: nil,
        metadataConfiguration: SQLServerMetadataClient.Configuration(
            includeSystemSchemas: false,
            enableColumnCache: true,
            includeRoutineDefinitions: true,
            includeTriggerDefinitions: true,
            commandTimeout: 10,
            extractParameterDefaults: false
        ),
        retryConfiguration: SQLServerRetryConfiguration(
            maximumAttempts: 5,
            backoffStrategy: { attempt in
                let base: Int64 = 250
                let delay = base << (attempt - 1)
                return .milliseconds(delay)
            },
            shouldRetry: { error in
                if let se = error as? SQLServerError {
                    switch se {
                    case .connectionClosed, .transient:
                        return true
                    case .timeout:
                        return false
                    default:
                        return false
                    }
                }
                if let tds = error as? TDSError {
                    if case .connectionClosed = tds { return true }
                    if case .protocolError(let message) = tds, message.localizedCaseInsensitiveContains("timeout") { return false }
                }
                if let ch = error as? ChannelError {
                    switch ch {
                    case .ioOnClosedChannel, .outputClosed, .eof, .alreadyClosed:
                        return true
                    default:
                        break
                    }
                }
                if error is NIOConnectionError { return true }
                return false
            }
        )
    )
    cfg.transparentNetworkIPResolution = false
    return cfg
}

public func makeSQLServerClientConfiguration() -> SQLServerClient.Configuration {
    let pool = SQLServerConnectionPool.Configuration(
        maximumConcurrentConnections: 8,
        minimumIdleConnections: 2,
        connectionIdleTimeout: nil,
        validationQuery: "SELECT 1;"
    )

    return SQLServerClient.Configuration(
        connection: makeSQLServerConnectionConfiguration(),
        poolConfiguration: pool
    )
}

// MARK: - Database Lifecycle

public func createTemporaryDatabase(client: SQLServerClient, prefix: String = "tmp") async throws -> String {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8)
    let dbName = "\(prefix)_\(token)"
    let createSql = "CREATE DATABASE [\(dbName)];"
    try await executeWithTransientRetry(client: client) { connection in
        connection.execute(createSql)
    }
    return String(dbName)
}

public func dropTemporaryDatabase(client: SQLServerClient, name: String) async throws {
    try await dropDatabaseIfExists(client: client, name: name)
}

public func withTemporaryDatabase<T>(
    client: SQLServerClient,
    prefix: String = "tmp",
    operation: (String) async throws -> T
) async throws -> T {
    let dbName = try await createTemporaryDatabase(client: client, prefix: prefix)

    do {
        let result = try await operation(dbName)
        try await dropTemporaryDatabase(client: client, name: dbName)
        return result
    } catch {
        try? await dropTemporaryDatabase(client: client, name: dbName)
        throw error
    }
}

public func withDbClient<T>(
    for database: String,
    using group: EventLoopGroup,
    maxConnections: Int = 4,
    operation: (SQLServerClient) async throws -> T
) async throws -> T {
    var config = makeSQLServerConnectionConfiguration()
    config.login.database = database
    // For isolated temporary databases used in tests, enable
    // parameter default extraction since the schema is small and
    // this metadata is explicitly validated in routine tests.
    config.metadataConfiguration.extractParameterDefaults = true

    let client = try await SQLServerClient.connect(
        configuration: SQLServerClient.Configuration(
            connection: config,
            poolConfiguration: SQLServerConnectionPool.Configuration(
                maximumConcurrentConnections: maxConnections,
                minimumIdleConnections: 0,
                connectionIdleTimeout: nil,
                validationQuery: nil
            )
        ),
        eventLoopGroupProvider: .shared(group)
    ).get()

    do {
        let result = try await operation(client)
        try await client.shutdownGracefully().get()
        return result
    } catch {
        // Ensure cleanup even if operation fails
        try? await client.shutdownGracefully().get()
        throw error
    }
}

// MARK: - Utilities

public func withTimeout<T>(_ timeout: TimeInterval, operation: @escaping () async throws -> T) async throws -> T {
    return try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            return try await operation()
        }

        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            throw AsyncTimeoutError.timeout(timeout)
        }

        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}

public func withRetry<T>(attempts: Int, operation: @escaping () async throws -> T) async throws -> T {
    var lastError: Error?

    for attempt in 1...attempts {
        do {
            return try await operation()
        } catch {
            lastError = error
            if attempt < attempts {
                // Simple exponential backoff
                let delay = TimeInterval(attempt) * 0.1
                try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            }
        }
    }

    throw lastError!
}

// MARK: - Performance Test Utilities

public func connectSQLServer(on eventLoop: EventLoop) -> EventLoopFuture<SQLServerConnection> {
    let config = makeSQLServerConnectionConfiguration()
    return SQLServerConnection.connect(configuration: config, on: eventLoop)
}

public func waitForResult<T>(_ future: EventLoopFuture<T>, timeout: TimeInterval, description: String) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    var result: Result<T, Error>?

    future.whenComplete { completion in
        result = completion
        semaphore.signal()
    }

    let timeoutResult = semaphore.wait(timeout: .now() + timeout)

    guard timeoutResult == .success else {
        throw TestError.timeout("Operation '\(description)' timed out after \(timeout) seconds")
    }

    switch result {
    case .success(let value):
        return value
    case .failure(let error):
        throw error
    case .none:
        throw TestError.timeout("No result received for operation '\(description)'")
    }
}

// MARK: - Error Types

public enum AsyncTimeoutError: Error {
    case timeout(TimeInterval)
}

public enum TestError: Error {
    case timeout(String)
}

// MARK: - Test Helper Functions

public func makeTempTableName(prefix: String = "tmp") -> String {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    return "#\(prefix)_\(token)"
}

public func makeSchemaQualifiedName(prefix: String, schema: String = "dbo") -> (bare: String, bracketed: String, nameOnly: String) {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let name = "\(prefix)_\(token)"
    let bare = "\(schema).\(name)"
    let bracketed = "[\(schema)].[\(name)]"
    return (bare, bracketed, name)
}

public func makeClient(
    forDatabase database: String,
    using group: EventLoopGroup,
    maxConnections: Int = 4
) async throws -> SQLServerClient {
    var config = makeSQLServerConnectionConfiguration()
    config.login.database = database

    let client = try await SQLServerClient.connect(
        configuration: SQLServerClient.Configuration(
            connection: config,
            poolConfiguration: SQLServerConnectionPool.Configuration(
                maximumConcurrentConnections: maxConnections,
                minimumIdleConnections: 0,
                connectionIdleTimeout: nil,
                validationQuery: nil
            )
        ),
        eventLoopGroupProvider: .shared(group)
    ).get()

    return client
}

@inline(__always)
private func isTransientConnectionClosureError(_ error: Error) -> Bool {
    if let sqlError = error as? SQLServerError {
        if case .connectionClosed = sqlError {
            return true
        }
    }
    if let channelError = error as? ChannelError, case .alreadyClosed = channelError {
        return true
    }
    if error.localizedDescription.contains("Already closed") {
        return true
    }
    return false
}

private func executeWithTransientRetry(
    client: SQLServerClient,
    attempts: Int = 3,
    operation: @escaping (SQLServerConnection) -> EventLoopFuture<SQLServerExecutionResult>
) async throws {
    var attempt = 0
    while true {
        attempt += 1
        do {
            let future: EventLoopFuture<SQLServerExecutionResult> = client.withConnection(on: nil, operation)
            _ = try await future.get()
            return
        } catch {
            if isTransientConnectionClosureError(error), attempt < attempts {
                try await Task.sleep(nanoseconds: 100_000_000)
                continue
            }
            throw error
        }
    }
}

private func queryWithTransientRetry<T>(
    client: SQLServerClient,
    attempts: Int = 3,
    operation: @escaping (SQLServerConnection) -> EventLoopFuture<T>
) async throws -> T {
    var attempt = 0
    while true {
        attempt += 1
        do {
            let future: EventLoopFuture<T> = client.withConnection(on: nil, operation)
            return try await future.get()
        } catch {
            if isTransientConnectionClosureError(error), attempt < attempts {
                try await Task.sleep(nanoseconds: 100_000_000)
                continue
            }
            throw error
        }
    }
}

private func databaseExists(client: SQLServerClient, name: String) async throws -> Bool {
    let sql = "SELECT DB_ID(N'\(name)') AS dbid;"
    let rows: [TDSRow] = try await queryWithTransientRetry(client: client) { connection in
        connection.query(sql)
    }
    if let dbValue = rows.first?.column("dbid")?.int, dbValue != 0 {
        return true
    }
    return false
}

public func dropDatabaseIfExists(client: SQLServerClient, name: String) async throws {
    let dropSql = """
    IF DB_ID(N'\(name)') IS NOT NULL
    BEGIN
        ALTER DATABASE [\(name)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE [\(name)];
    END
    """

    var attempt = 0
    while attempt < 5 {
        attempt += 1
        do {
            try await executeWithTransientRetry(client: client) { connection in
                connection.changeDatabase("master").flatMap { _ in
                    connection.execute(dropSql)
                }
            }
        } catch {
            if !isTransientConnectionClosureError(error) {
                throw error
            }
        }

        // Verify the database is gone before returning
        if try await databaseExists(client: client, name: name) {
            try await Task.sleep(nanoseconds: 100_000_000)
            continue
        } else {
            return
        }
    }

    throw SQLServerError.sqlExecutionError(message: "Unable to drop temporary database \(name) after multiple attempts")
}

public func executeInDb(client: SQLServerClient, database: String, _ sql: String) async throws {
    try await executeWithTransientRetry(client: client) { connection in
        connection.changeDatabase(database).flatMap { _ in
            connection.execute(sql)
        }
    }
}

public func withDbConnection<T>(
    client: SQLServerClient,
    database: String,
    operation: @escaping (SQLServerConnection) async throws -> T
) async throws -> T {
    let future: EventLoopFuture<T> = client.withConnection(on: nil) { connection in
        let promise = connection.eventLoop.makePromise(of: T.self)

        connection.changeDatabase(database).whenComplete { result in
            switch result {
            case .failure(let error):
                promise.fail(error)
            case .success:
                Task {
                    do {
                        let value = try await operation(connection)
                        promise.succeed(value)
                    } catch {
                        promise.fail(error)
                    }
                }
            }
        }

        return promise.futureResult
    }

    return try await future.get()
}

public func queryInDb(client: SQLServerClient, database: String, _ sql: String) async throws -> [TDSRow] {
    let future: EventLoopFuture<[TDSRow]> = client.withConnection(on: nil) { connection in
        connection.changeDatabase(database).flatMap { _ in
            connection.query(sql)
        }
    }
    return try await future.get()
}


// MARK: - Test Data Generators

public func generateUniqueTableName(prefix: String = "test") -> String {
    let token = UUID().uuidString.prefix(8)
    return "\(prefix)_\(token)"
}

public func generateUniqueColumnName(prefix: String = "col") -> String {
    let token = UUID().uuidString.prefix(6)
    return "\(prefix)_\(token)"
}

// MARK: - SQL Server Version Utilities

public struct SQLServerVersion {
    public let major: Int
    public let minor: Int
    public let build: Int
    public let revision: Int

    public init(major: Int, minor: Int, build: Int = 0, revision: Int = 0) {
        self.major = major
        self.minor = minor
        self.build = build
        self.revision = revision
    }

    public static func from(string: String) -> SQLServerVersion? {
        let parts = string.split(separator: ".").compactMap { Int($0) }
        guard parts.count >= 2 else { return nil }

        return SQLServerVersion(
            major: parts[0],
            minor: parts[1],
            build: parts.count > 2 ? parts[2] : 0,
            revision: parts.count > 3 ? parts[3] : 0
        )
    }
}

public func getSQLServerVersion(client: SQLServerClient) async throws -> SQLServerVersion? {
    let result = try await client.query("SELECT SERVERPROPERTY('ProductVersion') as version").get()
    guard let versionString = result.first?.column("version")?.string else { return nil }
    return SQLServerVersion.from(string: versionString)
}

public func supportsVersion(_ version: SQLServerVersion, minimumMajor: Int, minimumMinor: Int = 0) -> Bool {
    if version.major > minimumMajor { return true }
    if version.major == minimumMajor && version.minor >= minimumMinor { return true }
    return false
}

// MARK: - Feature Detection Utilities

public func supportsFeature(_ feature: String, client: SQLServerClient) async throws -> Bool {
    // Check if a feature is supported by querying sys.dm_os_server_properties
    // or using appropriate server properties
    let result = try await client.query("""
        SELECT CASE
            WHEN EXISTS (
                SELECT 1 FROM sys.all_objects
                WHERE name = '\(feature)'
            ) THEN 1
            ELSE 0
        END as supported
    """).get()

    return result.first?.column("supported")?.bool ?? false
}

public func requiresMinimumVersion(minimumMajor: Int, minimumMinor: Int = 0, client: SQLServerClient) async throws -> Bool {
    guard let version = try await getSQLServerVersion(client: client) else {
        return false
    }
    return supportsVersion(version, minimumMajor: minimumMajor, minimumMinor: minimumMinor)
}

// MARK: - Integration Test Base Class

#if canImport(XCTest)
import XCTest

/// Base class for all SQL Server integration tests.
/// Handles boilerplate setUp/tearDown and provides `requireServer()` skip helper.
open class SQLServerIntegrationTestCase: XCTestCase {
    public var group: EventLoopGroup!
    public var client: SQLServerClient!
    private var _skipDueToEnv = false

    open override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(
            configuration: makeSQLServerClientConfiguration(),
            eventLoopGroupProvider: .shared(group)
        ).get()
        do {
            _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() }
        } catch {
            _skipDueToEnv = true
        }
    }

    open override func tearDown() async throws {
        try? await client?.shutdownGracefully().get()
        try? await group?.shutdownGracefully()
        client = nil
        group = nil
    }

    /// Throws `XCTSkip` when the server was unavailable during setUp.
    public func requireServer() throws {
        if _skipDueToEnv { throw XCTSkip("Skipping: server unavailable during setUp") }
    }
}
#endif
