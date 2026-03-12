import Foundation
import NIO
import SQLServerKit
import SQLServerTDS

// MARK: - Database Lifecycle

public func createTemporaryDatabase(client: SQLServerClient, prefix: String = "tmp") async throws -> String {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8)
    let dbName = "\(prefix)_\(token)"
    let createSql = "CREATE DATABASE [\(dbName)];"
    try await executeWithTransientRetry(client: client) { connection in
        connection.execute(createSql)
    }
    try await waitForDatabaseOnline(client: client, name: String(dbName))
    try await waitForDatabaseConnectable(name: String(dbName))
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
    let connectionConfig = config

    let client = try await withRetry(attempts: 10) {
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
            connection.execute(sql).flatMap { result in
                resetDatabaseIfPossible(connection, to: connection.configuration.login.database).map { result }
            }.flatMapError { error in
                resetDatabaseIfPossible(connection, to: connection.configuration.login.database).flatMapThrowing { throw error }
            }
        }
    }
}

public func withDbConnection<T: Sendable>(
    client: SQLServerClient,
    database: String,
    operation: @escaping @Sendable (SQLServerConnection) async throws -> T
) async throws -> T {
    let future: EventLoopFuture<T> = client.withConnection(on: nil) { connection in
        let promise = connection.eventLoop.makePromise(of: T.self)
        let originalDatabase = connection.configuration.login.database

        connection.changeDatabase(database).whenComplete { result in
            switch result {
            case .failure(let error):
                promise.fail(error)
            case .success:
                Task {
                    do {
                        let value = try await operation(connection)
                        resetDatabaseIfPossible(connection, to: originalDatabase).whenComplete { resetResult in
                            switch resetResult {
                            case .success:
                                promise.succeed(value)
                            case .failure(let error):
                                promise.fail(error)
                            }
                        }
                    } catch {
                        resetDatabaseIfPossible(connection, to: originalDatabase).whenComplete { _ in
                            promise.fail(error)
                        }
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
            connection.query(sql).flatMap { rows in
                resetDatabaseIfPossible(connection, to: connection.configuration.login.database).map { rows }
            }.flatMapError { error in
                resetDatabaseIfPossible(connection, to: connection.configuration.login.database).flatMapThrowing { throw error }
            }
        }
    }
    return try await future.get()
}

// MARK: - Internal Helpers

@inline(__always)
internal func isTransientConnectionClosureError(_ error: Error) -> Bool {
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

internal func resetDatabaseIfPossible(_ connection: SQLServerConnection, to database: String) -> EventLoopFuture<Void> {
    connection.changeDatabase(database).flatMapError { error in
        if isTransientConnectionClosureError(error) {
            return connection.eventLoop.makeSucceededFuture(())
        }
        return connection.eventLoop.makeFailedFuture(error)
    }
}

internal func executeWithTransientRetry(
    client: SQLServerClient,
    attempts: Int = 3,
    operation: @escaping @Sendable (SQLServerConnection) -> EventLoopFuture<SQLServerExecutionResult>
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

internal func queryWithTransientRetry<T: Sendable>(
    client: SQLServerClient,
    attempts: Int = 3,
    operation: @escaping @Sendable (SQLServerConnection) -> EventLoopFuture<T>
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

internal func databaseExists(client: SQLServerClient, name: String) async throws -> Bool {
    let sql = "SELECT DB_ID(N'\(name)') AS dbid;"
    let rows: [TDSRow] = try await queryWithTransientRetry(client: client) { connection in
        connection.query(sql)
    }
    if let dbValue = rows.first?.column("dbid")?.int, dbValue != 0 {
        return true
    }
    return false
}

internal func waitForDatabaseOnline(client: SQLServerClient, name: String, attempts: Int = 20) async throws {
    let escaped = name.replacingOccurrences(of: "'", with: "''")
    let sql = """
    SELECT state, user_access
    FROM sys.databases
    WHERE name = N'\(escaped)'
    """

    for attempt in 1...attempts {
        let rows: [TDSRow] = try await queryWithTransientRetry(client: client) { connection in
            connection.query(sql)
        }
        if let row = rows.first,
           row.column("state")?.int == 0 {
            return
        }
        if attempt < attempts {
            try await Task.sleep(nanoseconds: 250_000_000)
        }
    }

    throw SQLServerError.sqlExecutionError(message: "Database \(name) did not reach ONLINE state")
}

internal func waitForDatabaseConnectable(name: String, attempts: Int = 20) async throws {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    var config = makeSQLServerConnectionConfiguration()
    config.login.database = name

    do {
        for attempt in 1...attempts {
            do {
                let client = try await SQLServerClient.connect(
                    configuration: SQLServerClient.Configuration(
                        connection: config,
                        poolConfiguration: SQLServerConnectionPool.Configuration(
                            maximumConcurrentConnections: 1,
                            minimumIdleConnections: 0,
                            connectionIdleTimeout: nil,
                            validationQuery: nil
                        )
                    ),
                    eventLoopGroupProvider: .shared(group)
                ).get()
                do {
                    let rows = try await client.query("SELECT DB_NAME() AS db_name").get()
                    try await client.shutdownGracefully().get()
                    if rows.first?.column("db_name")?.string?.caseInsensitiveCompare(name) == .orderedSame {
                        try await group.shutdownGracefully()
                        return
                    }
                } catch {
                    try? await client.shutdownGracefully().get()
                    throw error
                }
            } catch {
                if attempt == attempts {
                    try await group.shutdownGracefully()
                    throw error
                }
            }

            try await Task.sleep(nanoseconds: 250_000_000)
        }
        try await group.shutdownGracefully()
    } catch {
        try? await group.shutdownGracefully()
        throw error
    }
}
