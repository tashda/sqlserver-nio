import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerConnection {
    @available(*, deprecated, message: "Use async execute(_:) instead.")
    public func execute(_ sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        let future = executeWithRetry(operationName: "execute") {
            self.runBatch(sql)
        }
        return future.flatMapError { error in
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

    @available(*, deprecated, message: "Use async query(_:) instead.")
    public func query(_ sql: String) -> EventLoopFuture<[SQLServerRow]> {
        execute(sql).map(\.rows)
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [SQLServerRow] {
        try await execute(sql).rows
    }

    @available(*, deprecated, message: "Use async execute(_:timeout:) instead.")
    public func execute(_ sql: String, timeout seconds: TimeInterval) -> EventLoopFuture<SQLServerExecutionResult> {
        execute(sql, timeout: seconds, invalidateOnTimeout: true)
    }

    internal func execute(
        _ sql: String,
        timeout seconds: TimeInterval,
        invalidateOnTimeout: Bool
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let fut: EventLoopFuture<SQLServerExecutionResult> = execute(sql)
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

    @available(*, deprecated, message: "Use async queryScalar(_:as:) instead.")
    public func queryScalar<T: SQLServerDataConvertible & Sendable>(_ sql: String, as type: T.Type = T.self) -> EventLoopFuture<T?> {
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

    @available(*, deprecated, message: "Use async call(procedure:parameters:) instead.")
    public func call(procedure name: String, parameters: [ProcedureParameter] = []) -> EventLoopFuture<SQLServerExecutionResult> {
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
    public func streamQuery(_ sql: String) -> AsyncThrowingStream<SQLServerStreamEvent, Error> {
        AsyncThrowingStream(SQLServerStreamEvent.self) { continuation in
            let request = RawSqlRequest(
                sql: sql,
                onRow: { row in _ = continuation.yield(.row(SQLServerRow(base: row))) },
                onMetadata: { metadata in
                    let columns = metadata.map { column in
                        SQLServerColumnDescription(name: column.colName, type: SQLServerDataType(base: column.dataType), length: Int(column.length), precision: Int(column.precision), scale: Int(column.scale), flags: column.flags)
                    }
                    _ = continuation.yield(.metadata(columns))
                },
                onDone: { done in _ = continuation.yield(.done(SQLServerStreamDone(status: done.status, rowCount: done.doneRowCount))) },
                onMessage: { token, isError in
                    _ = continuation.yield(.message(SQLServerStreamMessage(kind: isError ? .error : .info, number: Int32(token.number), message: token.messageText, state: token.state, severity: token.classValue)))
                }
            )
            let future = self.base.send(request, logger: self.logger)
            future.whenComplete { result in
                switch result {
                case .success: continuation.finish()
                case .failure(let error): continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in self.base.sendAttention() }
        }
    }

    // MARK: - Explicit transaction helpers (SSMS parity)
    @available(*, deprecated, message: "Use async beginTransaction() instead.")
    public func beginTransaction() -> EventLoopFuture<Void> {
        let request = TransactionManagerRequest(
            command: .begin(),
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount
        )
        return base.send(request, logger: logger)
    }

    @available(*, deprecated, message: "Use async commit() instead.")
    public func commit() -> EventLoopFuture<Void> {
        let request = TransactionManagerRequest(
            command: .commit,
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount
        )
        return base.send(request, logger: logger)
    }

    @available(*, deprecated, message: "Use async rollback() instead.")
    public func rollback() -> EventLoopFuture<Void> {
        let request = TransactionManagerRequest(
            command: .rollback,
            transactionDescriptor: base.transactionDescriptor,
            outstandingRequestCount: base.requestCount
        )
        return base.send(request, logger: logger)
    }

    @available(*, deprecated, message: "Use async createSavepoint(_:) instead.")
    public func createSavepoint(_ name: String) -> EventLoopFuture<Void> {
        execute("SAVE TRANSACTION \(savepointIdentifier(name))").map { _ in () }
    }

    @available(*, deprecated, message: "Use async rollbackToSavepoint(_:) instead.")
    public func rollbackToSavepoint(_ name: String) -> EventLoopFuture<Void> {
        execute("ROLLBACK TRANSACTION \(savepointIdentifier(name))").map { _ in () }
    }

    @available(*, deprecated, message: "Use async setIsolationLevel(_:) instead.")
    public func setIsolationLevel(_ level: IsolationLevel) -> EventLoopFuture<Void> {
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
