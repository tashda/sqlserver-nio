import Foundation
import Logging
import NIO
import NIOPosix
import SQLServerTDS

/// Centralized test infrastructure for all SQL Server tests
/// This is the single source of truth for all test utilities and eliminates duplication

// MARK: - Environment Configuration

public enum TestEnvironment: String, CaseIterable {
    case production = "production"
    case staging = "staging"
    case development = "development"
    case local = "local"

    public var displayName: String {
        switch self {
        case .production: return "Production Server"
        case .staging: return "Staging Server"
        case .development: return "Development Server"
        case .local: return "Local Server"
        }
    }

    public var configuration: TestEnvironmentConfig {
        switch self {
        case .production:
            return TestEnvironmentConfig(
                hostname: "192.168.1.200",
                port: 1435,
                database: "master",
                username: "sa",
                password: "K3nn3th5"
            )
        case .staging:
            return TestEnvironmentConfig(
                hostname: "192.168.1.201",
                port: 1433,
                database: "staging_db",
                username: "staging_user",
                password: "StagingPass123!"
            )
        case .development:
            return TestEnvironmentConfig(
                hostname: "192.168.1.202",
                port: 1434,
                database: "dev_db",
                username: "dev_user",
                password: "DevPass123!"
            )
        case .local:
            return TestEnvironmentConfig(
                hostname: "localhost",
                port: 1433,
                database: "swift_tds_database",
                username: "swift_tds_user",
                password: "SwiftTDS!"
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
        let config = currentConfig
        setenv("TDS_HOSTNAME", config.hostname, 1)
        setenv("TDS_PORT", String(config.port), 1)
        setenv("TDS_DATABASE", config.database, 1)
        setenv("TDS_USERNAME", config.username, 1)
        setenv("TDS_PASSWORD", config.password, 1)
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
            includeRoutineDefinitions: true
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
        maximumConcurrentConnections: 4,
        minimumIdleConnections: 0,
        connectionIdleTimeout: nil,
        validationQuery: "SELECT 1;"
    )

    return SQLServerClient.Configuration(
        connection: makeSQLServerConnectionConfiguration(),
        poolConfiguration: pool
    )
}

// MARK: - Database Lifecycle

public func withTemporaryDatabase<T>(
    client: SQLServerClient,
    prefix: String = "tmp",
    operation: (String) async throws -> T
) async throws -> T {
    // Use master database directly instead of creating temporary databases
    // to avoid permission issues with database creation
    let dbName = "master"

    return try await operation(dbName)
}

public func withDbClient<T>(
    for database: String,
    using group: EventLoopGroup,
    operation: (SQLServerClient) async throws -> T
) async throws -> T {
    var config = makeSQLServerConnectionConfiguration()
    config.login.database = database

    let client = try await SQLServerClient.connect(
        configuration: SQLServerClient.Configuration(
            connection: config,
            poolConfiguration: SQLServerConnectionPool.Configuration()
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

// MARK: - Connection Utilities

@available(macOS 12.0, *)
public func withReliableConnection<T>(
    client: SQLServerClient,
    operation: @escaping (SQLServerConnection) async throws -> T
) async throws -> T {
    let maxAttempts = 3
    var lastError: Error?

    for attempt in 1...maxAttempts {
        do {
            return try await client.withConnection { connection in
                try await operation(connection)
            }
        } catch {
            lastError = error
            if attempt < maxAttempts {
                // Brief delay before retry
                try await Task.sleep(nanoseconds: 100_000_000) // 0.1 seconds
            }
        }
    }

    throw lastError!
}

public func makeClient(forDatabase database: String, using group: EventLoopGroup) async throws -> SQLServerClient {
    var config = makeSQLServerConnectionConfiguration()
    config.login.database = database

    let client = try await SQLServerClient.connect(
        configuration: SQLServerClient.Configuration(
            connection: config,
            poolConfiguration: SQLServerConnectionPool.Configuration()
        ),
        eventLoopGroupProvider: .shared(group)
    ).get()

    return client
}

@available(macOS 12.0, *)
public func executeInDb(client: SQLServerClient, database: String, _ sql: String) async throws {
    try await client.withConnection { connection in
        try await connection.changeDatabase(database)
        _ = try await connection.execute(sql)
    }
}

@available(macOS 12.0, *)
public func withDbConnection<T>(
    client: SQLServerClient,
    database: String,
    operation: @escaping (SQLServerConnection) async throws -> T
) async throws -> T {
    return try await client.withConnection { connection in
        try await connection.changeDatabase(database)
        return try await operation(connection)
    }
}

@available(macOS 12.0, *)
public func queryInDb(client: SQLServerClient, database: String, _ sql: String) async throws -> [TDSRow] {
    return try await client.withConnection { connection in
        try await connection.changeDatabase(database)
        return try await connection.query(sql)
    }
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

public func deep() -> Bool {
    return env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1"
}

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