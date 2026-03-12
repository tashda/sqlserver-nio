import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerKit
import SQLServerTDS

// MARK: - Utilities

public func withTimeout<T: Sendable>(_ timeout: TimeInterval, operation: @escaping @Sendable () async throws -> T) async throws -> T {
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

public func withRetry<T: Sendable>(attempts: Int, operation: @escaping @Sendable () async throws -> T) async throws -> T {
    var lastError: Error?

    for attempt in 1...attempts {
        do {
            return try await operation()
        } catch {
            lastError = error
            if attempt < attempts {
                let delay = TimeInterval(attempt) * 0.1
                try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            }
        }
    }

    throw lastError!
}

public func waitForResult<T: Sendable>(_ future: EventLoopFuture<T>, timeout: TimeInterval, description: String) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    let resultBox = NIOLockedValueBox<Result<T, Error>?>(nil)

    future.whenComplete { completion in
        resultBox.withLockedValue { $0 = completion }
        semaphore.signal()
    }

    let timeoutResult = semaphore.wait(timeout: .now() + timeout)

    guard timeoutResult == .success else {
        throw TestError.timeout("Operation '\(description)' timed out after \(timeout) seconds")
    }

    switch resultBox.withLockedValue({ $0 }) {
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
    let connectionConfig = config

    return try await withRetry(attempts: 10) {
        try await SQLServerClient.connect(
            configuration: SQLServerClient.Configuration(
                connection: connectionConfig,
                poolConfiguration: SQLServerConnectionPool.Configuration(
                    maximumConcurrentConnections: maxConnections,
                    minimumIdleConnections: 0,
                    connectionIdleTimeout: nil,
                    validationQuery: nil
                )
            ),
            eventLoopGroupProvider: .shared(group)
        ).get()
    }
}

public func generateUniqueTableName(prefix: String = "test") -> String {
    let token = UUID().uuidString.prefix(8)
    return "\(prefix)_\(token)"
}

public func generateUniqueColumnName(prefix: String = "col") -> String {
    let token = UUID().uuidString.prefix(6)
    return "\(prefix)_\(token)"
}
