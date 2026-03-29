import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerClient {
    @available(macOS 12.0, *)
    public func withConnection<Result: Sendable>(
        _ operation: @escaping @Sendable (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        try await withConnection(on: nil, operation)
    }

    @available(macOS 12.0, *)
    public func withDatabase<Result: Sendable>(
        _ database: String,
        _ operation: @escaping @Sendable (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        try await withConnection { connection in
            let originalDatabase = connection.currentDatabase
            let needsReset = originalDatabase.caseInsensitiveCompare(database) != .orderedSame
            if needsReset {
                _ = try await connection.changeDatabase(database)
            }
            do {
                let result = try await operation(connection)
                if needsReset {
                    _ = try await connection.changeDatabase(originalDatabase)
                }
                return result
            } catch {
                if needsReset {
                    _ = try? await connection.changeDatabase(originalDatabase)
                }
                throw error
            }
        }
    }

    @available(macOS 12.0, *)
    internal func withConnection<Result: Sendable>(
        on eventLoop: EventLoop? = nil,
        _ operation: @escaping @Sendable (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        let future: EventLoopFuture<Result> = self.withConnection(on: eventLoop) { connection in
            let promise = connection.eventLoop.makePromise(of: Result.self)
            let completed = NIOLockedValueBox(false)

            let task = Task {
                do {
                    let result = try await withTaskCancellationHandler(operation: {
                        try await operation(connection)
                    }, onCancel: {
                        connection.cancelActiveRequest()
                    })

                    if !completed.withLockedValue({ $0 }) {
                        completed.withLockedValue { $0 = true }
                        promise.succeed(result)
                    }
                } catch {
                    if !completed.withLockedValue({ $0 }) {
                        completed.withLockedValue { $0 = true }
                        let errorToFail: Error
                        if let sqlError = error as? SQLServerError,
                           case .deadlockDetected = sqlError {
                            errorToFail = sqlError
                        } else {
                            errorToFail = error
                        }
                        promise.fail(errorToFail)
                    }
                }
            }

            connection.underlying.closeFuture.whenComplete { _ in
                if !completed.withLockedValue({ $0 }) {
                    completed.withLockedValue { $0 = true }
                    promise.fail(SQLServerError.connectionClosed)
                }
            }

            promise.futureResult.whenFailure { _ in
                task.cancel()
            }

            return promise.futureResult
        }

        do {
            return try await future.get()
        } catch {
            if let channelError = error as? ChannelError,
               case .alreadyClosed = channelError {
                throw SQLServerError.connectionClosed
            }
            throw error
        }
    }

    @available(macOS 12.0, *)
    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await withConnection(on: eventLoop) { connection in
            try await connection.execute(sql)
        }
    }

    @available(macOS 12.0, *)
    public func execute(_ sql: String) async throws -> SQLServerExecutionResult {
        try await execute(sql, on: nil)
    }

    @available(macOS 12.0, *)
    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SQLServerRow] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.query(sql)
        }
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [SQLServerRow] {
        try await query(sql, on: nil)
    }

    @available(macOS 12.0, *)
    public func queryPaged(
        _ sql: String,
        limit: Int,
        offset: Int = 0,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SQLServerRow] {
        try await withConnection(on: eventLoop) { connection in
            try await connection.queryPaged(sql, limit: limit, offset: offset)
        }
    }

    @available(macOS 12.0, *)
    public func queryPaged(
        _ sql: String,
        limit: Int,
        offset: Int = 0
    ) async throws -> [SQLServerRow] {
        try await queryPaged(sql, limit: limit, offset: offset, on: nil)
    }

    // MARK: - Database Context

    /// Returns the name of the current database for this client's connection, or `nil` if unavailable.
    @available(macOS 12.0, *)
    public func currentDatabaseName() async throws -> String? {
        let rows = try await query("SELECT DB_NAME() AS current_db")
        return rows.first?.column("current_db")?.string
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(
        _ sql: String,
        as type: T.Type = T.self,
        on eventLoop: EventLoop? = nil
    ) async throws -> T? {
        try await withConnection(on: eventLoop) { connection in
            try await connection.queryScalar(sql, as: type)
        }
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(
        _ sql: String,
        as type: T.Type = T.self
    ) async throws -> T? {
        try await queryScalar(sql, as: type, on: nil)
    }

    @available(macOS 12.0, *)
    public func call(
        procedure name: String,
        parameters: [SQLServerConnection.ProcedureParameter] = [],
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await withConnection(on: eventLoop) { connection in
            try await connection.call(procedure: name, parameters: parameters)
        }
    }

    @available(macOS 12.0, *)
    public func call(
        procedure name: String,
        parameters: [SQLServerConnection.ProcedureParameter] = []
    ) async throws -> SQLServerExecutionResult {
        try await call(procedure: name, parameters: parameters, on: nil)
    }

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await withConnection { connection in
            try await connection.serverVersion()
        }
    }

    @available(macOS 12.0, *)
    public func executeOnFreshConnection(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        let loop = eventLoop ?? eventLoopGroup.next()
        return try await withCheckedThrowingContinuation { continuation in
            self.executeOnFreshConnection(sql, on: loop).whenComplete { result in
                continuation.resume(with: result)
            }
        }
    }

    /// Executes pre-split SQL statements sequentially on a locked connection.
    @available(*, deprecated, message: "Use executeBatches(_:) instead")
    @available(macOS 12.0, *)
    public func executeSeparateBatches(_ sqlStatements: [String]) async throws -> [SQLServerExecutionResult] {
        let batchResult = try await executeBatches(sqlStatements)
        var results: [SQLServerExecutionResult] = []
        for single in batchResult.batchResults {
            if let error = single.error { throw error }
            if let result = single.result { results.append(result) }
        }
        return results
    }

    /// Splits SQL at GO boundaries and executes each batch sequentially.
    @available(*, deprecated, message: "Split batches client-side and use executeBatches(_:) instead")
    @available(macOS 12.0, *)
    public func executeScript(_ sql: String) async throws -> [SQLServerExecutionResult] {
        // Preserve backward compatibility: split on GO and delegate to executeBatches
        let batches = sql.components(separatedBy: "\n").reduce(into: (current: [String](), result: [String]())) { state, line in
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.range(of: #"^GO[\t\r ]*$"#, options: [.regularExpression, .caseInsensitive]) != nil {
                let batch = state.current.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines)
                if !batch.isEmpty { state.result.append(batch) }
                state.current.removeAll()
            } else {
                state.current.append(line)
            }
        }
        var finalBatches = batches.result
        let lastBatch = batches.current.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines)
        if !lastBatch.isEmpty { finalBatches.append(lastBatch) }

        let batchResult = try await executeBatches(finalBatches)
        var results: [SQLServerExecutionResult] = []
        for single in batchResult.batchResults {
            if let error = single.error { throw error }
            if let result = single.result { results.append(result) }
        }
        return results
    }

    @available(macOS 12.0, *)
    private func executeWithConnectionLock<T: Sendable>(_ operation: @escaping @Sendable (SQLServerConnection) async throws -> T) async throws -> T {
        let future: EventLoopFuture<T> = self.withConnection(on: nil) { connection in
            let promise = connection.eventLoop.makePromise(of: T.self)
            let didComplete = NIOLockedValueBox(false)
            promise.futureResult.whenComplete { _ in
                didComplete.withLockedValue { $0 = true }
            }
            connection.underlying.closeFuture.whenComplete { _ in
                if !didComplete.withLockedValue({ $0 }) {
                    promise.fail(SQLServerError.connectionClosed)
                }
            }
            let _ = Task {
                do {
                    let result = try await operation(connection)
                    if !didComplete.withLockedValue({ $0 }) {
                        promise.succeed(result)
                    }
                } catch {
                    if !didComplete.withLockedValue({ $0 }) {
                        if error.localizedDescription.contains("Already closed") {
                            promise.fail(SQLServerError.connectionClosed)
                        } else {
                            promise.fail(error)
                        }
                    }
                }
            }
            return promise.futureResult
        }
        return try await future.get()
    }

    @available(macOS 12.0, *)
    public func healthCheck() async throws -> Bool {
        do {
            let rows = try await query("SELECT 1 as health_check")
            return rows.count == 1 && rows.first?.column("health_check")?.int == 1
        } catch {
            logger.warning("Health check failed: \(error)")
            return false
        }
    }

    @available(macOS 12.0, *)
    public func validateConnections() async throws {
        _ = try await withConnection { _ in
            ()
        }
    }

    /// Streams query results row by row via a back-pressure-aware async sequence.
    /// The connection is held for the duration of the stream and returned to the pool on completion.
    @available(macOS 12.0, *)
    public func streamQuery(_ sql: String) async throws -> (connection: SQLServerConnection, stream: SQLServerStreamSequence) {
        let connection = try await self.connection()
        let stream = connection.streamQuery(sql)
        return (connection: connection, stream: stream)
    }

    // MARK: - Multi-batch execution

    /// Executes multiple pre-split batches sequentially on a single pooled connection.
    ///
    /// Continues on error — failed batches are captured in results, not thrown.
    @available(macOS 12.0, *)
    public func executeBatches(_ batches: [String]) async throws -> BatchExecutionResult {
        try await executeWithConnectionLock { connection in
            try await connection.executeBatches(batches)
        }
    }

    /// Streams events from multiple pre-split batches on a single pooled connection.
    ///
    /// The connection is held for the duration of all batches.
    @available(macOS 12.0, *)
    public func streamBatches(_ batches: [String]) async throws -> (connection: SQLServerConnection, stream: AsyncThrowingStream<BatchStreamEvent, any Error>) {
        let connection = try await self.connection()
        let stream = connection.streamBatches(batches)
        return (connection: connection, stream: stream)
    }
}
