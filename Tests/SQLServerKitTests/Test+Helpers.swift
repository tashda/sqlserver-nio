import Foundation
import Logging
import NIO
import SQLServerKit
import XCTest

// MARK: - Logging

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()

// MARK: - Environment

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}

private var hasLoadedEnvironmentFile = false

private let packageRootPath: String = {
    var url = URL(fileURLWithPath: #filePath)
    // Test+Helpers.swift -> SQLServerKitTests -> Tests -> package root
    for _ in 0..<3 {
        url.deleteLastPathComponent()
    }
    return url.path
}()

func loadEnvFileIfPresent(path: String = ".env") {
    guard !hasLoadedEnvironmentFile else { return }
    hasLoadedEnvironmentFile = true
    
    let fileManager = FileManager.default
    let candidatePaths: [String] = {
        if path.hasPrefix("/") {
            return [path]
        }
        let workingDirectory = fileManager.currentDirectoryPath
        return [
            path,
            URL(fileURLWithPath: workingDirectory).appendingPathComponent(path).path,
            URL(fileURLWithPath: packageRootPath).appendingPathComponent(path).path
        ]
    }()
    
    guard let locatedPath = candidatePaths.first(where: { fileManager.fileExists(atPath: $0) }) else {
        return
    }
    
    guard let contents = try? String(contentsOfFile: locatedPath, encoding: .utf8) else {
        return
    }
    
    let newlineSet = CharacterSet.newlines
    let whitespaceSet = CharacterSet.whitespacesAndNewlines
    
    contents.components(separatedBy: newlineSet).forEach {
        let line = $0.trimmingCharacters(in: whitespaceSet)
        guard !line.isEmpty, !line.hasPrefix("#") else { return }
        
        guard let separatorIndex = line.firstIndex(of: "=") else { return }
        
        let keySubstring = line[..<separatorIndex]
        let valueSubstring = line[line.index(after: separatorIndex)...]
        
        let trimmedKey = keySubstring.trimmingCharacters(in: whitespaceSet)
        var trimmedValue = valueSubstring.trimmingCharacters(in: whitespaceSet)
        
        if trimmedValue.hasPrefix("\"") && trimmedValue.hasSuffix("\""), trimmedValue.count >= 2 {
            trimmedValue = String(trimmedValue.dropFirst().dropLast())
        } else if trimmedValue.hasPrefix("'" ) && trimmedValue.hasSuffix("'" ), trimmedValue.count >= 2 {
            trimmedValue = String(trimmedValue.dropFirst().dropLast())
        }
        
        let key = String(trimmedKey)
        let value = String(trimmedValue)
        
        guard !key.isEmpty else { return }
        setenv(key, value, 1)
    }
}

// MARK: - Test Configuration

func makeSQLServerConnectionConfiguration() -> SQLServerConnection.Configuration {
    let hostname = env("TDS_HOSTNAME") ?? "localhost"
    let port = env("TDS_PORT").flatMap(Int.init) ?? 1433
    let username = env("TDS_USERNAME") ?? "swift_tds_user"
    let password = env("TDS_PASSWORD") ?? "SwiftTDS!"
    let database = env("TDS_DATABASE") ?? "swift_tds_database"

    return SQLServerConnection.Configuration(
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
            maximumAttempts: 8,
            backoffStrategy: { attempt in
                let base: Int64 = 250 // ms
                let delay = base << (attempt - 1) // 250, 500, 1000, ...
                return .milliseconds(delay)
            }
        )
    )
}

func makeSQLServerClientConfiguration() -> SQLServerClient.Configuration {
    let pool = SQLServerConnectionPool.Configuration(
        maximumConcurrentConnections: 1,
        minimumIdleConnections: 1,
        connectionIdleTimeout: .seconds(60),
        validationQuery: "SELECT 1;"
    )

    return SQLServerClient.Configuration(
        connection: makeSQLServerConnectionConfiguration(),
        poolConfiguration: pool
    )
}

func connectSQLServer(on eventLoop: EventLoop) -> EventLoopFuture<SQLServerConnection> {
    SQLServerConnection.connect(configuration: makeSQLServerConnectionConfiguration(), on: eventLoop)
}


// MARK: - Test Helpers

func makeTempTableName(prefix: String = "tmp") -> String {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    return "#\(prefix)_\(token)"
}

func makeSchemaQualifiedName(prefix: String, schema: String = "dbo") -> (bare: String, bracketed: String, nameOnly: String) {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let name = "\(prefix)_\(token)"
    let bare = "\(schema).\(name)"
    let bracketed = "[\(schema)].[\(name)]"
    return (bare, bracketed, name)
}

func requireEnvFlag(_ key: String, description: String) throws {
    guard env(key) == "1" else {
        throw XCTSkip("Skipping \(description). Set \(key)=1 to enable.")
    }
}

enum TestTimeoutError: Error, LocalizedError {
    case timedOut(timeout: TimeInterval, description: String)

    var errorDescription: String? {
        switch self {
        case .timedOut(let timeout, let description):
            return "Operation '\(description)' timed out after \(timeout) seconds"
        }
    }
}

extension XCTestCase {
    func waitForResult<T>(
        _ future: EventLoopFuture<T>,
        timeout: TimeInterval,
        description: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> T {
        let expectation = expectation(description: description)
        var result: Result<T, Error>?

        future.whenComplete { value in
            result = value
            expectation.fulfill()
        }

        let waiterResult = XCTWaiter.wait(for: [expectation], timeout: timeout)
        guard waiterResult == .completed else {
            XCTFail("Operation '\(description)' did not complete within \(timeout) seconds", file: file, line: line)
            throw TestTimeoutError.timedOut(timeout: timeout, description: description)
        }

        guard let resolved = result else {
            XCTFail("Operation '\(description)' completed without result", file: file, line: line)
            throw TestTimeoutError.timedOut(timeout: timeout, description: description)
        }

        switch resolved {
        case .success(let value):
            return value
        case .failure(let error):
            throw error
        }
    }
}

extension NSRegularExpression {
    convenience init(_ pattern: String) {
        do {
            try self.init(pattern: pattern)
        } catch {
            preconditionFailure("Illegal regular expression: \(pattern).")
        }
    }
    
    func matches(_ string: String?) -> Bool {
        guard let str = string else { return false }
        let range = NSRange(location: 0, length: str.utf16.count)
        return firstMatch(in: str, options: [], range: range) != nil
    }
}

let sqlServerVersionPattern = "[0-9]{2}\\.{1}[0-9]{1}\\.{1}[0-9]{4}\\.{1}[0-9]{1}"
// MARK: - DDL Serialization & Retry

actor DDLGuard {
    static let shared = DDLGuard()
    func withLock<T>(_ operation: () async throws -> T) async rethrows -> T {
        try await operation()
    }
}

@discardableResult
func runWithRetry(_ client: SQLServerClient, _ sql: String, attempts: Int = 3, delayNs: UInt64 = 200_000_000) async -> Bool {
    for i in 1...attempts {
        do {
            _ = try await client.execute(sql).get()
            return true
        } catch {
            if i == attempts { return false }
            try? await Task.sleep(nanoseconds: delayNs)
        }
    }
    return false
}

// MARK: - Ephemeral database helpers

/// Creates an ephemeral database, runs the body, and drops the database.
/// DDL is serialized and each step has a timeout + retry for robustness.
func withTemporaryDatabase(
    client: SQLServerClient,
    prefix: String = "tmpdb",
    body: @escaping (_ database: String) async throws -> Void
) async throws {
    let db = "\(prefix)_\(UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(12))"
    try await DDLGuard.shared.withLock {
        _ = try await withTimeout(15) { try await client.execute("CREATE DATABASE [\(db)]").get() }
    }
    // Wait for the database to come ONLINE and accept queries
    do {
        _ = try await withTimeout(10) {
            try await withRetry(attempts: 8, delayNs: 250_000_000) {
                // Check ONLINE state
                let rows = try await client.query("SELECT state_desc FROM sys.databases WHERE name = N'\(db)' ").get()
                guard rows.first?.column("state_desc")?.string == "ONLINE" else {
                    throw SQLServerError.timeout(description: "database not ONLINE yet", underlying: nil)
                }
                // Also verify a simple query using a DB-scoped connection
                let _: [TDSRow] = try await withDbConnection(client: client, database: db) { conn in
                    try await conn.query("SELECT 1 AS ready").get()
                }
                return ()
            }
        }
    } catch {
        // best-effort; if readiness fails, attempt cleanup and rethrow
        await runWithRetry(client, "ALTER DATABASE [\(db)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [\(db)]")
        throw error
    }
    do {
        try await body(db)
    } catch {
        // attempt cleanup then rethrow
        await runWithRetry(client, "ALTER DATABASE [\(db)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [\(db)]")
        throw error
    }
    try await DDLGuard.shared.withLock {
        _ = try? await withTimeout(15) {
            try await client.execute("ALTER DATABASE [\(db)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [\(db)]").get()
        }
    }
}

// MARK: - Retry helper for flaky connections

@discardableResult
func withRetry<T>(
    attempts: Int = 3,
    delayNs: UInt64 = 200_000_000,
    _ operation: @escaping () async throws -> T
) async throws -> T {
    var lastError: Error?
    for i in 1...attempts {
        do {
            return try await operation()
        } catch {
            lastError = error
            if i == attempts { break }
            if let se = error as? SQLServerError {
                switch se {
                case .connectionClosed, .timeout, .transient:
                    try? await Task.sleep(nanoseconds: delayNs)
                    continue
                default:
                    throw error
                }
            } else {
                throw error
            }
        }
    }
    throw lastError ?? SQLServerError.unknown(NSError(domain: "withRetry", code: -1))
}

/// Runs an operation using a connection switched to the provided database for the duration of the operation.
func withDbConnection<T>(
    client: SQLServerClient,
    database: String,
    _ operation: @escaping (SQLServerConnection) async throws -> T
) async throws -> T {
    try await client.withConnection { connection in
        _ = try await connection.changeDatabase(database).get()
        return try await operation(connection)
    }
}

// Creates a connected client reusing the same group, but pointing to a specific database.
func makeClient(forDatabase database: String, using group: EventLoopGroup) async throws -> SQLServerClient {
    var cfg = makeSQLServerClientConfiguration()
    cfg.connection.login.database = database
    return try await SQLServerClient.connect(configuration: cfg, eventLoopGroupProvider: .shared(group)).get()
}

@discardableResult
func executeInDb(
    client: SQLServerClient,
    database: String,
    _ sql: String
) async throws -> SQLServerExecutionResult {
    try await withDbConnection(client: client, database: database) { conn in
        try await conn.execute(sql).get()
    }
}

func queryInDb(
    client: SQLServerClient,
    database: String,
    _ sql: String
) async throws -> [TDSRow] {
    try await withDbConnection(client: client, database: database) { conn in
        try await conn.query(sql).get()
    }
}
// MARK: - Async timeout helper

enum AsyncTimeoutError: Error, LocalizedError {
    case timedOut(seconds: TimeInterval)

    var errorDescription: String? {
        switch self {
        case .timedOut(let s):
            return "Operation timed out after \(s) seconds"
        }
    }
}

func withTimeout<T>(_ seconds: TimeInterval, _ operation: @escaping () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            throw AsyncTimeoutError.timedOut(seconds: seconds)
        }
        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}
