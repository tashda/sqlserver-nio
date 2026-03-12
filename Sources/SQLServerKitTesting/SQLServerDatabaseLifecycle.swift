import Foundation
import NIO
import SQLServerKit
import SQLServerTDS

// MARK: - Database Lifecycle

@available(macOS 12.0, *)
public func createTemporaryDatabase(client: SQLServerClient, prefix: String = "tmp") async throws -> String {
    let token = UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8)
    let dbName = "\(prefix)_\(token)"
    let createSql = "CREATE DATABASE [\(dbName)];"
    try await executeWithTransientRetry(client: client) { connection in
        try await connection.execute(createSql)
    }
    try await waitForDatabaseOnline(client: client, name: String(dbName))
    try await waitForDatabaseConnectable(name: String(dbName))
    return String(dbName)
}

@available(macOS 12.0, *)
public func dropTemporaryDatabase(client: SQLServerClient, name: String) async throws {
    try await dropDatabaseIfExists(client: client, name: name)
}

@available(macOS 12.0, *)
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

@available(macOS 12.0, *)
public func withDbClient<T>(
    for database: String,
    using _: EventLoopGroup,
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
            numberOfThreads: 1
        )
    }

    do {
        let result = try await operation(client)
        try await client.shutdownGracefully()
        return result
    } catch {
        // Ensure cleanup even if operation fails
        try? await client.shutdownGracefully()
        throw error
    }
}

@available(macOS 12.0, *)
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
                try await connection.changeDatabase("master")
                return try await connection.execute(dropSql)
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

@available(macOS 12.0, *)
public func executeInDb(client: SQLServerClient, database: String, _ sql: String) async throws {
    try await executeWithTransientRetry(client: client) { connection in
        let originalDatabase = connection.currentDatabase
        try await connection.changeDatabase(database)
        do {
            let result = try await connection.execute(sql)
            try await connection.changeDatabase(originalDatabase)
            return result
        } catch {
            try? await connection.changeDatabase(originalDatabase)
            throw error
        }
    }
}

@available(macOS 12.0, *)
public func withDbConnection<T: Sendable>(
    client: SQLServerClient,
    database: String,
    operation: @escaping @Sendable (SQLServerConnection) async throws -> T
) async throws -> T {
    try await client.withConnection { connection in
        let originalDatabase = connection.currentDatabase
        try await connection.changeDatabase(database)
        do {
            let value = try await operation(connection)
            try await connection.changeDatabase(originalDatabase)
            return value
        } catch {
            try? await connection.changeDatabase(originalDatabase)
            throw error
        }
    }
}

@available(macOS 12.0, *)
public func queryInDb(client: SQLServerClient, database: String, _ sql: String) async throws -> [SQLServerRow] {
    try await client.withConnection { connection in
        let originalDatabase = connection.currentDatabase
        try await connection.changeDatabase(database)
        do {
            let rows = try await connection.query(sql)
            try await connection.changeDatabase(originalDatabase)
            return rows
        } catch {
            try? await connection.changeDatabase(originalDatabase)
            throw error
        }
    }
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

@available(macOS 12.0, *)
internal func executeWithTransientRetry(
    client: SQLServerClient,
    attempts: Int = 3,
    operation: @escaping @Sendable (SQLServerConnection) async throws -> SQLServerExecutionResult
) async throws {
    var attempt = 0
    while true {
        attempt += 1
        do {
            _ = try await client.withConnection(operation)
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

@available(macOS 12.0, *)
internal func queryWithTransientRetry<T: Sendable>(
    client: SQLServerClient,
    attempts: Int = 3,
    operation: @escaping @Sendable (SQLServerConnection) async throws -> T
) async throws -> T {
    var attempt = 0
    while true {
        attempt += 1
        do {
            return try await client.withConnection(operation)
        } catch {
            if isTransientConnectionClosureError(error), attempt < attempts {
                try await Task.sleep(nanoseconds: 100_000_000)
                continue
            }
            throw error
        }
    }
}

@available(macOS 12.0, *)
internal func databaseExists(client: SQLServerClient, name: String) async throws -> Bool {
    let sql = "SELECT DB_ID(N'\(name)') AS dbid;"
    let rows: [SQLServerRow] = try await queryWithTransientRetry(client: client) { connection in
        try await connection.query(sql)
    }
    if let dbValue = rows.first?.column("dbid")?.int, dbValue != 0 {
        return true
    }
    return false
}

@available(macOS 12.0, *)
internal func waitForDatabaseOnline(client: SQLServerClient, name: String, attempts: Int = 20) async throws {
    let escaped = name.replacingOccurrences(of: "'", with: "''")
    let sql = """
    SELECT state, user_access
    FROM sys.databases
    WHERE name = N'\(escaped)'
    """

    for attempt in 1...attempts {
        let rows: [SQLServerRow] = try await queryWithTransientRetry(client: client) { connection in
            try await connection.query(sql)
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

@available(macOS 12.0, *)
internal func waitForDatabaseConnectable(name: String, attempts: Int = 20) async throws {
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
                    numberOfThreads: 1
                )
                do {
                    let rows = try await client.query("SELECT DB_NAME() AS db_name")
                    try await client.shutdownGracefully()
                    if rows.first?.column("db_name")?.string?.caseInsensitiveCompare(name) == .orderedSame {
                        return
                    }
                } catch {
                    try? await client.shutdownGracefully()
                    throw error
                }
            } catch {
                if attempt == attempts {
                    throw error
                }
            }

            try await Task.sleep(nanoseconds: 250_000_000)
        }
    } catch {
        throw error
    }
}
