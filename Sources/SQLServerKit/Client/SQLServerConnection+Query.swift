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
        let future = executeWithRetry(operationName: "execute") {
            self.runBatch(sql)
        }
        // Apply configured default query timeout (sends ATTENTION on expiry)
        let guarded: EventLoopFuture<SQLServerExecutionResult>
        if applyDefaultTimeout, let timeout = configuration.sessionOptions.defaultQueryTimeout {
            let timed = future.withTimeout(on: self.eventLoop, seconds: timeout)
            timed.whenFailure { error in
                if case .timeout = SQLServerError.normalize(error) {
                    self.base.sendAttention()
                }
            }
            guarded = timed
        } else {
            guarded = future
        }
        return guarded.flatMapError { error in
            let normalized = SQLServerError.normalize(error)
            switch normalized {
            case .timeout:
                var meta: Logger.Metadata = [
                    "db": .string(self.currentDatabase),
                    "snippet": .string(String(sql.prefix(120)))
                ]
                let trace = self.base.tokenTraceSnapshot().suffix(10).joined(separator: " | ")
                if !trace.isEmpty { meta["tdsTrace"] = .string(trace) }
                self.logger.error("SQL execute timed out", metadata: meta)
            case .connectionClosed:
                var meta: Logger.Metadata = ["db": .string(self.currentDatabase)]
                let trace = self.base.tokenTraceSnapshot().suffix(10).joined(separator: " | ")
                if !trace.isEmpty { meta["tdsTrace"] = .string(trace) }
                self.logger.error("SQL execute connection closed", metadata: meta)
            default:
                break
            }
            return self.eventLoop.makeFailedFuture(normalized)
        }.withTestTimeoutIfEnabled(on: self.eventLoop)
    }

    @available(macOS 12.0, *)
    public func execute(_ sql: String) async throws -> SQLServerExecutionResult {
        try checkClosed()
        let future: EventLoopFuture<SQLServerExecutionResult> = self.execute(sql)
        return try await withTaskCancellationHandler(operation: {
            try await future.get()
        }, onCancel: { [base] in
            base.sendAttention()
        })
    }

    internal func query(_ sql: String) -> EventLoopFuture<[SQLServerRow]> {
        execute(sql).map(\.rows)
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [SQLServerRow] {
        try await execute(sql).rows
    }

    internal func execute(_ sql: String, timeout seconds: TimeInterval) -> EventLoopFuture<SQLServerExecutionResult> {
        execute(sql, timeout: seconds, invalidateOnTimeout: true)
    }

    internal func execute(
        _ sql: String,
        timeout seconds: TimeInterval,
        invalidateOnTimeout: Bool
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let fut: EventLoopFuture<SQLServerExecutionResult> = execute(sql, applyDefaultTimeout: false)
        let timed = fut.withTimeout(on: self.eventLoop, seconds: seconds)
        timed.whenFailure { error in
            if case .timeout = SQLServerError.normalize(error) {
                self.base.sendAttention()
                if invalidateOnTimeout {
                    _ = self.invalidate()
                }
            }
        }
        return timed
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
                    $0.dones.append(SQLServerStreamDone(status: token.status, rowCount: token.doneRowCount))
                }
            },
            onMessage: { token, isError in
                accumulator.withLockedValue {
                    $0.messages.append(SQLServerStreamMessage(kind: isError ? .error : .info, number: Int32(token.number), message: token.messageText, state: token.state, severity: token.classValue))
                }
            },
            onReturnValue: { token in
                let tdsValue: TDSData? = token.value.map { TDSData(metadata: token.metadata, value: $0) }
                accumulator.withLockedValue {
                    $0.returnValues.append(SQLServerReturnValue(name: token.name, status: token.status, value: tdsValue.map(SQLServerValue.init(base:))))
                }
            }
        )

        return self.base.send(request, logger: self.logger).flatMapThrowing { _ in
            let snapshot = accumulator.withLockedValue { $0 }
            let result = SQLServerExecutionResult(rows: snapshot.rows, done: snapshot.dones, messages: snapshot.messages, returnValues: snapshot.returnValues)
            if let err = snapshot.messages.first(where: { $0.kind == .error }) {
                if err.number == 1205 { throw SQLServerError.deadlockDetected(message: err.message) }
                else { throw SQLServerError.sqlExecutionError(message: err.message) }
            }
            return result
        }
    }

    @available(macOS 12.0, *)
    public func call(procedure name: String, parameters: [ProcedureParameter] = []) async throws -> SQLServerExecutionResult {
        try await call(procedure: name, parameters: parameters).get()
    }

    @available(macOS 12.0, *)
    public func streamQuery(_ sql: String) -> SQLServerStreamSequence {
        let delegate = SQLServerStreamDelegate(connection: base)
        let produced = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: SQLServerStreamEvent.self,
            failureType: (any Error).self,
            backPressureStrategy: AdaptiveRowBuffer(),
            finishOnDeinit: false,
            delegate: delegate
        )
        let source = produced.source

        // Disable auto-read so data is only read on demand from the delegate.
        base.suspendAutoRead()

        let request = RawSqlRequest(
            sql: sql,
            onRow: { row in _ = source.yield(.row(SQLServerRow(base: row))) },
            onMetadata: { metadata in
                let columns = metadata.map { column in
                    SQLServerColumnDescription(
                        name: column.colName,
                        type: SQLServerDataType(base: column.dataType),
                        typeName: column.udtInfo?.typeName ?? SQLServerDataType(base: column.dataType).name,
                        length: Int(column.length),
                        precision: Int(column.precision),
                        scale: Int(column.scale),
                        flags: column.flags
                    )
                }
                _ = source.yield(.metadata(columns))
            },
            onDone: { done in _ = source.yield(.done(SQLServerStreamDone(status: done.status, rowCount: done.doneRowCount))) },
            onMessage: { token, isError in
                _ = source.yield(.message(SQLServerStreamMessage(kind: isError ? .error : .info, number: Int32(token.number), message: token.messageText, state: token.state, severity: token.classValue)))
            }
        )
        let future = self.base.send(request, logger: self.logger)
        future.whenComplete { [base] result in
            delegate.markFinished()
            switch result {
            case .success: source.finish()
            case .failure(let error): source.finish(error)
            }
            // Restore auto-read for subsequent non-streaming requests.
            base.resumeAutoRead()
        }

        // Trigger the first read to start receiving the response.
        base.requestRead()

        return SQLServerStreamSequence(produced.sequence)
    }

    // MARK: - Explicit transaction helpers (SSMS parity)
    internal func beginTransaction() -> EventLoopFuture<Void> {
        let request = TransactionManagerRequest(
            command: .begin(),
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount
        )
        return base.send(request, logger: logger)
    }

    internal func commit() -> EventLoopFuture<Void> {
        let request = TransactionManagerRequest(
            command: .commit,
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount
        )
        return base.send(request, logger: logger)
    }

    internal func rollback() -> EventLoopFuture<Void> {
        let request = TransactionManagerRequest(
            command: .rollback,
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount
        )
        return base.send(request, logger: logger)
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
            return Self.escapeIdentifier(name)
        }
        return name
    }
}
