import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerConnection {
    public func changeDatabase(_ database: String) -> EventLoopFuture<Void> {
        let current = stateLock.withLock { _currentDatabase }
        if Self.equalsIgnoreCase(current, database) {
            return eventLoop.makeSucceededFuture(())
        }
        let fut = executeWithRetry(operationName: "changeDatabase") {
            let sql = "USE \(Self.escapeIdentifier(database));"
            return self.runBatch(sql).map { _ in
                self.setCurrentDatabase(database)
            }
        }
        return fut.withTestTimeoutIfEnabled(on: self.eventLoop)
    }

    @available(macOS 12.0, *)
    public func changeDatabase(_ database: String) async throws {
        try await changeDatabase(database).get()
    }

    internal func bootstrapSession() -> EventLoopFuture<Void> {
        let statements = configuration.sessionOptions.buildStatements()
        guard !statements.isEmpty else {
            return self.eventLoop.makeSucceededFuture(())
        }
        let batch = statements.joined(separator: " ")
        return runBatch(batch).map { _ in () }
    }

    internal func runBatch(_ sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        struct Accumulator: Sendable {
            var rows: [TDSRow] = []
            var dones: [SQLServerStreamDone] = []
            var messages: [SQLServerStreamMessage] = []
        }

        let accumulator = NIOLockedValueBox(Accumulator())

        let request = RawSqlRequest(
            sql: sql,
            onRow: { row in
                accumulator.withLockedValue { $0.rows.append(row) }
            },
            onDone: { token in
                accumulator.withLockedValue {
                    $0.dones.append(SQLServerStreamDone(status: token.status, rowCount: token.doneRowCount))
                }
            },
            onMessage: { token, isError in
                accumulator.withLockedValue {
                    $0.messages.append(SQLServerStreamMessage(
                        kind: isError ? .error : .info,
                        number: Int32(token.number),
                        message: token.messageText,
                        state: token.state,
                        severity: token.classValue
                    ))
                }
            }
        )

        return self.base.send(request, logger: self.logger).flatMapThrowing { _ in
            let snapshot = accumulator.withLockedValue { $0 }
            let result = SQLServerExecutionResult(rows: snapshot.rows, done: snapshot.dones, messages: snapshot.messages)
            if let err = snapshot.messages.first(where: { $0.kind == .error }) {
                if err.number == 1205 {
                    throw SQLServerError.deadlockDetected(message: err.message)
                } else {
                    throw SQLServerError.sqlExecutionError(message: err.message)
                }
            }
            return result
        }
    }

    internal func markSessionPrimed() {
        stateLock.withLock { _isSessionPrimed = true }
    }

    internal func executeWithRetry<Result: Sendable>(
        operationName: String,
        operation: @Sendable @escaping () -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        @Sendable
        func attempt(_ currentAttempt: Int) -> EventLoopFuture<Result> {
            return operation().flatMapError { error in
                let normalized = SQLServerError.normalize(error)
                if currentAttempt < self.configuration.retryConfiguration.maximumAttempts && self.configuration.retryConfiguration.shouldRetry(normalized) {
                    self.logger.debug("Operation \(operationName) attempt \(currentAttempt) failed; retrying")
                    return attempt(currentAttempt + 1)
                }
                return self.eventLoop.makeFailedFuture(normalized)
            }
        }
        return attempt(1)
    }

    public func invalidate() -> EventLoopFuture<Void> {
        self.release(true).flatMap { self.shutdownGroupIfNeeded() }
    }

    internal func shutdownGroupIfNeeded() -> EventLoopFuture<Void> {
        guard let group = ownsEventLoopGroup else {
            return eventLoop.makeSucceededFuture(())
        }
        return SQLServerClient.shutdownEventLoopGroup(group)
    }

    internal func setCurrentDatabase(_ database: String) {
        stateLock.withLock { _currentDatabase = database }
        metadataClient.updateDefaultDatabase(database)
    }

    internal static func escapeIdentifier(_ identifier: String) -> String {
        return identifier.replacingOccurrences(of: "]", with: "]]")
    }

    internal static func equalsIgnoreCase(_ a: String, _ b: String) -> Bool {
        return a.caseInsensitiveCompare(b) == .orderedSame
    }

    internal func checkClosed() throws {
        if base.isClosed {
            throw SQLServerError.connectionClosed
        }
    }
}
