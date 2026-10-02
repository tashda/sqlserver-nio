import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerConnection {
    internal func changeDatabase(_ database: String) -> EventLoopFuture<Void> {
        let current = stateLock.withLock { _currentDatabase }
        if Self.equalsIgnoreCase(current, database) {
            return eventLoop.makeSucceededFuture(())
        }
        let sql = "USE \(SQLServerSQL.escapeIdentifier(database));"
        let fut = self.runBatch(sql).map { _ in
            self.setCurrentDatabase(database)
            self.logger.debug("Database context changed to \(database)")
        }
        return fut.withTestTimeoutIfEnabled(on: self.eventLoop)
    }

    @available(macOS 12.0, *)
    public func changeDatabase(_ database: String) async throws {
        try await changeDatabase(database).get()
    }

    @available(macOS 12.0, *)
    public func use(database: String) async throws {
        try await changeDatabase(database)
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
        startBatch(sql, timeout: nil).result
    }

    /// Sends a SQL batch and returns its cancellation handle and result.
    ///
    /// The result fails with the server's error when any statement raised
    /// one. Rows and messages produced before the error are available through
    /// `SQLServerError.serverDetails`.
    internal func startBatch(
        _ sql: String,
        timeout: TimeInterval?,
        resetConnection: Bool = false
    ) -> (handle: TDSRequestHandle, result: EventLoopFuture<SQLServerExecutionResult>) {
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
                    $0.dones.append(SQLServerStreamDone(
                        kind: .init(tokenType: token.type),
                        status: token.status,
                        curCmd: token.curCmd,
                        rowCount: token.doneRowCount
                    ))
                }
            },
            onMessage: { token, isError in
                accumulator.withLockedValue {
                    $0.messages.append(SQLServerStreamMessage(token: token, isError: isError))
                }
            }
        )
        request.resetConnection = resetConnection

        let handle = base.start(request, timeout: timeout.flatMap(Self.timeAmount))
        let result = handle.future.flatMapThrowing { _ in
            let snapshot = accumulator.withLockedValue { $0 }
            if let error = SQLServerError.fromServerMessages(snapshot.messages) {
                throw error
            }
            return SQLServerExecutionResult(rows: snapshot.rows, done: snapshot.dones, messages: snapshot.messages)
        }.always { _ in
            self.syncCurrentDatabaseFromServer()
        }
        return (handle, result)
    }

    internal static func timeAmount(_ seconds: TimeInterval) -> TimeAmount? {
        guard seconds.isFinite, seconds > 0 else { return nil }
        return .nanoseconds(Int64(seconds * 1_000_000_000))
    }

    /// Adopts the database the server reports as current. A batch can switch
    /// databases with USE, directly or inside a procedure.
    internal func syncCurrentDatabaseFromServer() {
        guard let serverDatabase = base.currentDatabase else { return }
        let changed = stateLock.withLock { () -> Bool in
            guard _currentDatabase != serverDatabase else { return false }
            return true
        }
        if changed { setCurrentDatabase(serverDatabase) }
    }

    internal func markSessionPrimed() {
        stateLock.withLock { _isSessionPrimed = true }
    }

    internal func invalidate() -> EventLoopFuture<Void> {
        self.release(true).map { self.fireAndForgetGroupShutdown() }
    }

    internal func fireAndForgetGroupShutdown() {
        // Fire-and-forget: we cannot return a future for group shutdown because NIO
        // would need to hop the result back to this event loop — which belongs to the
        // group being shut down — causing "Cannot schedule tasks on shut down EventLoop."
        guard let group = ownsEventLoopGroup else { return }
        group.shutdownGracefully { _ in }
    }

    internal func setCurrentDatabase(_ database: String) {
        stateLock.withLock { _currentDatabase = database }
        metadataClient.updateDefaultDatabase(database)
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
