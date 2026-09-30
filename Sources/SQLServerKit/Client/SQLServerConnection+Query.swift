import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerConnection {
    @available(macOS 12.0, *)
    public func queryPaged(_ sql: String, limit: Int, offset: Int = 0) async throws -> [SQLServerRow] {
        precondition(limit > 0, "limit must be positive")
        precondition(offset >= 0, "offset must be non-negative")

        let trimmedSQL = sql
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .trimmingCharacters(in: CharacterSet(charactersIn: ";"))

        let pagedSQL = """
        SELECT paged_result.*
        FROM (
            SELECT inner_query.*, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS __sqlserver_nio_rownum
            FROM (
                \(trimmedSQL)
            ) AS inner_query
        ) AS paged_result
        WHERE paged_result.__sqlserver_nio_rownum > \(offset)
          AND paged_result.__sqlserver_nio_rownum <= \(offset + limit)
        ORDER BY paged_result.__sqlserver_nio_rownum;
        """

        return try await execute(pagedSQL).rows.map { $0.droppingLastColumn() }
    }

    internal func execute(_ sql: String, applyDefaultTimeout: Bool = true) -> EventLoopFuture<SQLServerExecutionResult> {
        let timeout = applyDefaultTimeout ? configuration.sessionOptions.defaultQueryTimeout : nil
        return startExecute(sql, timeout: timeout).result
    }

    /// Sends a batch exactly once. A failed batch may already have committed
    /// on the server, so it is never replayed. On timeout the batch is
    /// cancelled on the server and the connection stays usable once SQL
    /// Server acknowledges the cancellation.
    internal func startExecute(
        _ sql: String,
        timeout: TimeInterval?
    ) -> (handle: TDSRequestHandle, result: EventLoopFuture<SQLServerExecutionResult>) {
        let (handle, future) = startBatch(sql, timeout: timeout)
        let result = future.flatMapError { error in
            let normalized = SQLServerError.normalize(error)
            switch normalized {
            case .timeout:
                self.logger.warning("SQL batch timed out and was cancelled", metadata: [
                    "db": .string(self.currentDatabase),
                    "snippet": .string(String(sql.prefix(120)))
                ])
            case .connectionClosed:
                self.logger.error("SQL batch failed because the connection closed", metadata: ["db": .string(self.currentDatabase)])
            default:
                break
            }
            return self.eventLoop.makeFailedFuture(normalized)
        }
        return (handle, result)
    }

    /// Executes a SQL batch.
    ///
    /// Cancelling the calling task cancels the batch on the server and throws
    /// `CancellationError` after SQL Server acknowledges the cancellation.
    /// Statements that completed before the cancellation are not rolled back
    /// unless they ran inside a transaction that is rolled back.
    @available(macOS 12.0, *)
    public func execute(_ sql: String) async throws -> SQLServerExecutionResult {
        try await execute(sql, timeout: configuration.sessionOptions.defaultQueryTimeout)
    }

    /// Executes a SQL batch with an explicit deadline. On expiry the batch is
    /// cancelled on the server and `SQLServerError.timeout` is thrown.
    @available(macOS 12.0, *)
    public func execute(_ sql: String, timeout: TimeInterval?) async throws -> SQLServerExecutionResult {
        try checkClosed()
        try Task.checkCancellation()
        let (handle, result) = startExecute(sql, timeout: timeout)
        return try await Self.awaitCancellable(handle: handle, result: result)
    }

    /// Awaits a request, cancelling it on the server if the task is cancelled.
    @available(macOS 12.0, *)
    internal static func awaitCancellable<T: Sendable>(
        handle: TDSRequestHandle,
        result: EventLoopFuture<T>
    ) async throws -> T {
        do {
            return try await withTaskCancellationHandler {
                try await result.get()
            } onCancel: {
                handle.cancel()
            }
        } catch {
            if Task.isCancelled, Self.isCancellation(error) {
                throw CancellationError()
            }
            throw error
        }
    }

    internal static func isCancellation(_ error: Error) -> Bool {
        if let tds = error as? TDSError { return tds == .cancelled }
        if case .protocolError(let tds) = error as? SQLServerError { return tds == .cancelled }
        return false
    }

    internal func query(_ sql: String) -> EventLoopFuture<[SQLServerRow]> {
        execute(sql).map(\.rows)
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [SQLServerRow] {
        try await execute(sql).rows
    }

    internal func execute(_ sql: String, timeout seconds: TimeInterval) -> EventLoopFuture<SQLServerExecutionResult> {
        startExecute(sql, timeout: seconds).result
    }

    /// `invalidateOnTimeout` is retained for source compatibility. A timed-out
    /// request is cancelled with ATTENTION and the connection is closed only
    /// when SQL Server does not acknowledge the cancellation.
    internal func execute(
        _ sql: String,
        timeout seconds: TimeInterval,
        invalidateOnTimeout: Bool
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        startExecute(sql, timeout: seconds).result
    }

    internal func queryScalar<T: SQLServerDataConvertible & Sendable>(_ sql: String, as type: T.Type = T.self) -> EventLoopFuture<T?> {
        execute(sql).map { result in
            guard
                let row = result.rows.first,
                let firstColumn = row.columns.first?.name,
                let valueData = row.column(firstColumn),
                let value = T(sqlServerValue: valueData)
            else {
                return nil
            }
            return value
        }
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(_ sql: String, as type: T.Type = T.self) async throws -> T? {
        try await queryScalar(sql, as: type).get()
    }

    internal func call(procedure name: String, parameters: [ProcedureParameter] = []) -> EventLoopFuture<SQLServerExecutionResult> {
        startCall(procedure: name, parameters: parameters).result
    }

    internal func startCall(
        procedure name: String,
        parameters: [ProcedureParameter]
    ) -> (handle: TDSRequestHandle, result: EventLoopFuture<SQLServerExecutionResult>) {
        struct Accumulator: Sendable {
            var rows: [TDSRow] = []
            var dones: [SQLServerStreamDone] = []
            var messages: [SQLServerStreamMessage] = []
            var returnValues: [SQLServerReturnValue] = []
        }

        let accumulator = NIOLockedValueBox(Accumulator())

        let tdsParams = parameters.map { p in
            TDSMessages.RpcParameter(name: p.name, data: p.value?.base, direction: {
                switch p.direction { case .in: return .in; case .out: return .out; case .inout: return .inout }
            }())
        }

        let request = RpcRequest(
            rpcMessage: TDSMessages.RpcRequestMessage(
                procedureName: name,
                parameters: tdsParams,
                transactionDescriptor: base.transactionDescriptor,
                outstandingRequestCount: base.requestCount
            ),
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
            },
            onReturnValue: { token in
                let tdsValue: TDSData? = token.value.map { TDSData(metadata: token.metadata, value: $0) }
                accumulator.withLockedValue {
                    $0.returnValues.append(SQLServerReturnValue(name: token.name, status: token.status, value: tdsValue.map(SQLServerValue.init(base:))))
                }
            }
        )

        let timeout = configuration.sessionOptions.defaultQueryTimeout.flatMap(Self.timeAmount)
        let handle = base.start(request, timeout: timeout)
        let result = handle.future.flatMapThrowing { _ -> SQLServerExecutionResult in
            let snapshot = accumulator.withLockedValue { $0 }
            if let error = SQLServerError.fromServerMessages(snapshot.messages) {
                throw error
            }
            return SQLServerExecutionResult(rows: snapshot.rows, done: snapshot.dones, messages: snapshot.messages, returnValues: snapshot.returnValues)
        }.flatMapErrorThrowing { error -> SQLServerExecutionResult in
            throw SQLServerError.normalize(error)
        }.always { _ in
            self.syncCurrentDatabaseFromServer()
        }
        return (handle, result)
    }

    @available(macOS 12.0, *)
    public func call(procedure name: String, parameters: [ProcedureParameter] = []) async throws -> SQLServerExecutionResult {
        try checkClosed()
        try Task.checkCancellation()
        let (handle, result) = startCall(procedure: name, parameters: parameters)
        return try await Self.awaitCancellable(handle: handle, result: result)
    }

    /// Streams the events of a SQL batch as they arrive.
    ///
    /// Rows are read from the socket only as fast as the sequence is
    /// consumed, so memory stays bounded for arbitrarily large results.
    /// Abandoning the sequence, or cancelling the consuming task, cancels the
    /// batch on the server; the connection is reusable once SQL Server
    /// acknowledges the cancellation. Server errors are delivered as
    /// `.message` events with `kind == .error` rather than thrown.
    @available(macOS 12.0, *)
    public func streamQuery(_ sql: String) -> SQLServerStreamSequence {
        let handleBox = NIOLockedValueBox<TDSRequestHandle?>(nil)
        let delegate = SQLServerStreamDelegate(handle: handleBox)
        let produced = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: SQLServerStreamEvent.self,
            failureType: (any Error).self,
            backPressureStrategy: AdaptiveRowBuffer(),
            finishOnDeinit: false,
            delegate: delegate
        )
        let source = produced.source

        // Row batching accumulator — all callbacks fire on the NIO event loop
        // so this is single-threaded and safe without locks.
        let batcher = StreamRowBatcher(source: source, capacity: 256) { result in
            switch result {
            case .stopProducing:
                handleBox.withLockedValue { $0 }?.pauseReading()
            case .produceMore, .dropped:
                break
            }
        }

        let request = RawSqlRequest(
            sql: sql,
            onRow: { row in batcher.addRow(SQLServerRow(base: row)) },
            onMetadata: { metadata in
                batcher.flush()
                let columns = metadata.map { column in
                    SQLServerColumnDescription(
                        name: column.colName,
                        type: SQLServerDataType(base: column.dataType),
                        typeName: column.udtInfo?.typeName ?? SQLServerDataType(base: column.dataType).name,
                        length: Int(column.length),
                        precision: Int(column.precision),
                        scale: Int(column.scale),
                        flags: column.flags,
                        cellType: SQLServerCellType(metadata: column)
                    )
                }
                batcher.yield(.metadata(columns))
            },
            onDone: { done in
                batcher.flush()
                batcher.yield(.done(SQLServerStreamDone(
                    kind: .init(tokenType: done.type),
                    status: done.status,
                    curCmd: done.curCmd,
                    rowCount: done.doneRowCount
                )))
            },
            onMessage: { token, isError in
                batcher.flush()
                batcher.yield(.message(SQLServerStreamMessage(token: token, isError: isError)))
            }
        )
        if base.isClosed {
            source.finish(SQLServerError.connectionClosed)
            return SQLServerStreamSequence(produced.sequence)
        }
        let handle = base.start(request, timeout: nil)
        handleBox.withLockedValue { $0 = handle }
        handle.future.whenComplete { result in
            batcher.flush()
            delegate.markFinished()
            self.syncCurrentDatabaseFromServer()
            switch result {
            case .success: source.finish()
            case .failure(let error): source.finish(SQLServerError.normalize(error))
            }
        }

        return SQLServerStreamSequence(produced.sequence)
    }

    // MARK: - Explicit transaction helpers (SSMS parity)
    internal func beginTransaction() -> EventLoopFuture<Void> {
        sendTransactionCommand(.begin())
    }

    internal func commit() -> EventLoopFuture<Void> {
        sendTransactionCommand(.commit)
    }

    internal func rollback() -> EventLoopFuture<Void> {
        sendTransactionCommand(.rollback)
    }

    /// Sends a TDS transaction manager request. Server errors are surfaced:
    /// SQL Server rejects a COMMIT of a doomed transaction (error 3930), and
    /// reporting that as success would lose the caller's writes silently.
    private func sendTransactionCommand(_ command: TransactionManagerRequest.Command) -> EventLoopFuture<Void> {
        let messages = NIOLockedValueBox<[SQLServerStreamMessage]>([])
        let request = TransactionManagerRequest(
            command: command,
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount,
            onMessage: { token, isError in
                messages.withLockedValue { $0.append(SQLServerStreamMessage(token: token, isError: isError)) }
            }
        )
        return base.send(request, logger: logger).flatMapThrowing {
            if let error = SQLServerError.fromServerMessages(messages.withLockedValue { $0 }) {
                throw error
            }
        }.flatMapErrorThrowing { error in
            throw SQLServerError.normalize(error)
        }
    }

    internal func createSavepoint(_ name: String) -> EventLoopFuture<Void> {
        execute("SAVE TRANSACTION \(savepointIdentifier(name))").map { _ in () }
    }

    internal func rollbackToSavepoint(_ name: String) -> EventLoopFuture<Void> {
        execute("ROLLBACK TRANSACTION \(savepointIdentifier(name))").map { _ in () }
    }

    internal func setIsolationLevel(_ level: IsolationLevel) -> EventLoopFuture<Void> {
        execute("SET TRANSACTION ISOLATION LEVEL \(level.sqlLiteral)").map { _ in () }
    }

    @available(macOS 12.0, *)
    public func beginTransaction() async throws {
        try checkClosed()
        _ = try await beginTransaction().get()
    }

    @available(macOS 12.0, *)
    public func commit() async throws {
        try checkClosed()
        _ = try await commit().get()
    }

    @available(macOS 12.0, *)
    public func rollback() async throws {
        try checkClosed()
        _ = try await rollback().get()
    }

    @available(macOS 12.0, *)
    public func createSavepoint(_ name: String) async throws {
        try checkClosed()
        _ = try await createSavepoint(name).get()
    }

    @available(macOS 12.0, *)
    public func rollbackToSavepoint(_ name: String) async throws {
        try checkClosed()
        _ = try await rollbackToSavepoint(name).get()
    }

    @available(macOS 12.0, *)
    public func setIsolationLevel(_ level: IsolationLevel) async throws {
        try checkClosed()
        _ = try await setIsolationLevel(level).get()
    }

    private func savepointIdentifier(_ name: String) -> String {
        if name.contains(" ") || name.contains("-") || name.contains(".") {
            return SQLServerSQL.escapeIdentifier(name)
        }
        return name
    }

    // MARK: - Multi-batch execution

    /// Executes multiple pre-split batches sequentially on this connection.
    ///
    /// Continues on error by default — if a batch fails, the error is captured in
    /// `SingleBatchResult.error` and execution proceeds to the next batch.
    /// Only throws if the connection itself is broken.
    @available(macOS 12.0, *)
    public func executeBatches(_ batches: [String]) async throws -> BatchExecutionResult {
        var results: [BatchExecutionResult.SingleBatchResult] = []
        results.reserveCapacity(batches.count)

        for (index, sql) in batches.enumerated() {
            try Task.checkCancellation()
            let trimmed = sql.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else {
                results.append(.init(batchIndex: index, result: nil))
                continue
            }
            do {
                let result = try await execute(trimmed)
                results.append(.init(batchIndex: index, result: result))
            } catch is CancellationError {
                throw CancellationError()
            } catch let error as SQLServerError where isConnectionBroken(error) {
                throw error
            } catch {
                results.append(.init(batchIndex: index, result: nil, error: error))
            }
        }

        return BatchExecutionResult(batchResults: results)
    }

    /// Streams events from multiple pre-split batches executed sequentially.
    ///
    /// Emits `batchStarted`, per-batch stream events, then `batchCompleted` or
    /// `batchFailed` for each batch, continuing after a batch that fails with
    /// a server error. Batches start only as the sequence is consumed: when
    /// the consumer stops or its task is cancelled, the running batch is
    /// cancelled on the server and later batches never run. After the
    /// connection is lost no further batch is attempted.
    @available(macOS 12.0, *)
    public func streamBatches(_ batches: [String]) -> AsyncThrowingStream<BatchStreamEvent, any Error> {
        let state = BatchStreamState(connection: self, batches: batches)
        return AsyncThrowingStream(unfolding: { try await state.next() })
    }

    private func isConnectionBroken(_ error: SQLServerError) -> Bool {
        error.isConnectionLost
    }
}

/// Pull-driven state for `streamBatches`. Only the consuming task touches it.
@available(macOS 12.0, *)
private final class BatchStreamState: @unchecked Sendable {
    private let connection: SQLServerConnection
    private let batches: [String]
    private var index = 0
    private var current: SQLServerStreamSequence.AsyncIterator?
    private var messages: [SQLServerStreamMessage] = []

    init(connection: SQLServerConnection, batches: [String]) {
        self.connection = connection
        self.batches = batches
    }

    func next() async throws -> BatchStreamEvent? {
        while true {
            if current != nil {
                let batchIndex = index
                do {
                    if let event = try await current!.next() {
                        if case .message(let message) = event { messages.append(message) }
                        return .batchEvent(index: batchIndex, event: event)
                    }
                    current = nil
                    index += 1
                    if let error = SQLServerError.fromServerMessages(messages) {
                        return .batchFailed(index: batchIndex, error: error, messages: messages)
                    }
                    return .batchCompleted(index: batchIndex)
                } catch {
                    current = nil
                    index += 1
                    if error is CancellationError || Task.isCancelled { throw CancellationError() }
                    if SQLServerError.normalize(error).isConnectionLost {
                        index = batches.count
                    }
                    return .batchFailed(index: batchIndex, error: error, messages: messages)
                }
            }
            guard index < batches.count else { return nil }
            try Task.checkCancellation()
            let sql = batches[index].trimmingCharacters(in: .whitespacesAndNewlines)
            guard !sql.isEmpty else {
                index += 1
                continue
            }
            messages = []
            current = connection.streamQuery(sql).makeAsyncIterator()
            return .batchStarted(index: index)
        }
    }
}
