import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOPosix
import NIOSSL
import SQLServerTDS
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

public final class SQLServerConnection {

    public struct Configuration: Sendable {
        public struct Login: Sendable {
            public var database: String
            public var authentication: TDSAuthentication

            public init(database: String, authentication: TDSAuthentication) {
                self.database = database
                self.authentication = authentication
            }
        }

        public var hostname: String
        public var port: Int
        public var login: Login
        public var tlsConfiguration: TLSConfiguration?
        public var transparentNetworkIPResolution: Bool
        public var metadataConfiguration: SQLServerMetadataClient.Configuration
        public var retryConfiguration: SQLServerRetryConfiguration
        public var sessionOptions: SessionOptions

        public init(
            hostname: String,
            port: Int = 1433,
            login: Login,
            tlsConfiguration: TLSConfiguration? = .makeClientConfiguration(),
            metadataConfiguration: SQLServerMetadataClient.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init(),
            sessionOptions: SessionOptions = .ssmsDefaults,
            transparentNetworkIPResolution: Bool = true
        ) {
            self.hostname = hostname
            self.port = port
            self.login = login
            self.tlsConfiguration = tlsConfiguration
            self.metadataConfiguration = metadataConfiguration
            self.retryConfiguration = retryConfiguration
            self.sessionOptions = sessionOptions
            self.transparentNetworkIPResolution = transparentNetworkIPResolution
        }
    }

    public static func connect(
        configuration: Configuration,
        eventLoopGroupProvider: SQLServerClient.EventLoopGroupProvider = .createNew(numberOfThreads: System.coreCount),
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) -> EventLoopFuture<SQLServerConnection> {
        let group: EventLoopGroup
        let ownsGroup: Bool

        switch eventLoopGroupProvider {
        case .shared(let provided):
            group = provided
            ownsGroup = false
        case .createNew(let threads):
            group = MultiThreadedEventLoopGroup(numberOfThreads: threads)
            ownsGroup = true
        }

        let loop = group.next()
        let fut = connect(configuration: configuration, on: loop, logger: logger)
            .map { connection in
                connection.ownsEventLoopGroup = ownsGroup ? group : nil
                return connection
            }
            .flatMapError { error in
                if ownsGroup {
                    _ = SQLServerClient.shutdownEventLoopGroup(group)
                }
                return loop.makeFailedFuture(error)
            }

        return fut
    }

    public static func connect(
        configuration: Configuration,
        on eventLoop: EventLoop,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) -> EventLoopFuture<SQLServerConnection> {
        func attempt(_ cfg: Configuration) -> EventLoopFuture<SQLServerConnection> {
            let loginConfiguration = TDSLoginConfiguration(
                serverName: cfg.hostname,
                port: cfg.port,
                database: cfg.login.database,
                authentication: cfg.login.authentication
            )

            return resolveSocketAddresses(
                hostname: cfg.hostname,
                port: cfg.port,
                transparentResolution: cfg.transparentNetworkIPResolution,
                on: eventLoop
            ).flatMap { addresses in
                Self.establishTDSConnection(
                    addresses: addresses,
                    tlsConfiguration: cfg.tlsConfiguration,
                    serverHostname: cfg.hostname,
                    on: eventLoop,
                    logger: logger
                )
            }.flatMap { connection in
                connection.login(configuration: loginConfiguration).map { connection }.flatMapError { error in
                    connection.close().flatMapThrowing { throw error }
                }
            }.flatMap { connection in
                let sqlConnection = SQLServerConnection(
                    base: connection,
                    configuration: cfg,
                    metadataCache: nil,
                    logger: logger,
                    reuseOnClose: false,
                    releaseClosure: { close in
                        if close || connection.isClosed {
                            return connection.close()
                        } else {
                            return connection.eventLoop.makeSucceededFuture(())
                        }
                    }
                )
                return sqlConnection.bootstrapSession().map { sqlConnection }
            }
        }

        // Primary attempt with provided configuration
        return attempt(configuration).flatMapError { error in
            // Fallback: if non-default port is set and connect failed due to connection issues, try default 1433
            let normalized = SQLServerError.normalize(error)
            switch normalized {
            case .connectionClosed, .transient, .timeout:
                if configuration.port != 1433 {
                    var fallback = configuration
                    fallback.port = 1433
                    logger.warning("Primary port \(configuration.port) connect failed; attempting fallback to 1433")
                    return attempt(fallback)
                }
                return eventLoop.makeFailedFuture(normalized)
            default:
                return eventLoop.makeFailedFuture(normalized)
            }
        }
    }

    public var eventLoop: EventLoop {
        base.eventLoop
    }

    public var logger: Logger {
        base.logger
    }

    // Latest raw session state/data classification payloads received on this connection
    public var lastSessionStatePayload: [UInt8] { base.snapshotSessionStatePayload() }
    public var lastDataClassificationPayload: [UInt8] { base.snapshotDataClassificationPayload() }

    public var currentDatabase: String {
        stateLock.withLock { _currentDatabase }
    }

    public func changeDatabase(_ database: String) -> EventLoopFuture<Void> {
        let current = stateLock.withLock { _currentDatabase }
        if Self.equalsIgnoreCase(current, database) {
            return eventLoop.makeSucceededFuture(())
        }
        let fut = executeWithRetry(operationName: "changeDatabase") {
            let sql = "USE \(Self.escapeIdentifier(database));"
            return self.base.rawSql(sql).map { _ in
                self.setCurrentDatabase(database)
            }
        }
        return fut.withTestTimeoutIfEnabled(on: self.eventLoop)
    }

    @available(macOS 12.0, *)
    public func changeDatabase(_ database: String) async throws {
        try await changeDatabase(database).get()
    }

    public func close() -> EventLoopFuture<Void> {
        if reuseOnClose {
            // For pooled connections, avoid issuing session-reset queries during close.
            // This prevents scheduling work on event loops that may be shutting down
            // during test teardown and matches common pool behavior where reset happens
            // on next checkout if needed.
            return self.release(close: false).flatMap { _ in
                self.shutdownGroupIfNeeded()
            }
        } else {
            return release(close: true).flatMap { _ in
                self.shutdownGroupIfNeeded()
            }
        }
    }

    public func execute(_ sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        let future = executeWithRetry(operationName: "execute") {
            self.runBatch(sql)
        }
        let guarded = future.flatMapError { error in
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
        }
        return guarded.withTestTimeoutIfEnabled(on: self.eventLoop)
    }

    /// Checks if the connection is closed and throws an appropriate error
    private func checkClosed() throws {
        if base.isClosed {
            throw SQLServerError.connectionClosed
        }
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

    public func query(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        execute(sql).map(\.rows)
    }

    // MARK: - Explicit transaction helpers (SSMS parity)
    public func beginTransaction() -> EventLoopFuture<Void> {
        execute("BEGIN TRANSACTION").map { _ in () }
    }

    public func commit() -> EventLoopFuture<Void> {
        execute("COMMIT").map { _ in () }
    }

    public func rollback() -> EventLoopFuture<Void> {
        execute("ROLLBACK").map { _ in () }
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
    public func withTransaction<T>(body: @escaping (SQLServerConnection) async throws -> T) async throws -> T {
        do {
            try await beginTransaction()
            let result = try await body(self)
            try await commit()
            return result
        } catch {
            // Best-effort rollback
            _ = try? await rollback()
            throw error
        }
    }

    // MARK: - Per-call timeout variants
    public func execute(_ sql: String, timeout seconds: TimeInterval) -> EventLoopFuture<SQLServerExecutionResult> {
        let fut: EventLoopFuture<SQLServerExecutionResult> = execute(sql)
        return fut.withTimeout(on: self.eventLoop, seconds: seconds)
    }

    public func query(_ sql: String, timeout seconds: TimeInterval) -> EventLoopFuture<[TDSRow]> {
        execute(sql, timeout: seconds).map(\.rows)
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [TDSRow] {
        try checkClosed()
        return try await withTaskCancellationHandler(operation: { [self] in
            let result = try await self.execute(sql).get()
            return result.rows
        }, onCancel: { [base] in
            base.sendAttention()
        })
    }

    public func queryScalar<T: TDSDataConvertible>(_ sql: String, as type: T.Type = T.self) -> EventLoopFuture<T?> {
        execute(sql).map { result in
            guard
                let row = result.rows.first,
                let firstColumn = row.columnMetadata.first?.colName,
                let valueData = row.column(firstColumn),
                let value = T(tdsData: valueData)
            else {
                return nil
            }
            return value
        }
    }

    // MARK: - RPC stored procedure calls
    public struct ProcedureParameter: Sendable {
        public enum Direction: Sendable { case `in`, out, `inout` }
        public var name: String
        public var value: TDSData?
        public var direction: Direction
        public init(name: String, value: TDSData?, direction: Direction = .in) {
            self.name = name; self.value = value; self.direction = direction
        }
    }

    public func call(procedure name: String, parameters: [ProcedureParameter] = []) -> EventLoopFuture<SQLServerExecutionResult> {
        let rows: [TDSRow] = []
        let dones: [SQLServerStreamDone] = []
        let messages: [SQLServerStreamMessage] = []
        let returnValues: [SQLServerReturnValue] = []

        let tdsParams = parameters.map { p in
            TDSMessages.RpcParameter(name: p.name, data: p.value, direction: {
                switch p.direction { case .in: return .in; case .out: return .out; case .inout: return .inout }
            }())
        }

        // Build a single RPC request and send it; do not send twice
        let request = RpcRequest(
            rpcMessage: TDSMessages.RpcRequestMessage(
                procedureName: name,
                parameters: tdsParams,
                transactionDescriptor: base.transactionDescriptor,
                outstandingRequestCount: base.requestCount
            )
        )

        return self.base.send(request, logger: self.logger).flatMapThrowing { _ in
            let result = SQLServerExecutionResult(rows: rows, done: dones, messages: messages, returnValues: returnValues)

            if let err = messages.first(where: { $0.kind == .error }) {
                if err.number == 1205 {
                    throw SQLServerError.deadlockDetected(message: err.message)
                } else {
                    throw SQLServerError.sqlExecutionError(message: err.message)
                }
            }

            return result
        }
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: TDSDataConvertible>(_ sql: String, as type: T.Type = T.self) async throws -> T? {
        try await queryScalar(sql, as: type).get()
    }

    @available(macOS 12.0, *)
    public func streamQuery(_ sql: String) -> AsyncThrowingStream<SQLServerStreamEvent, Error> {
        AsyncThrowingStream { continuation in
            let message = TDSMessages.RawSqlBatchMessage(sqlText: sql)
            let request = RawSqlRequest(
                sql: message.sqlText,
                onRow: { row in
                    _ = continuation.yield(.row(row))
                },
                onMetadata: { metadata in
                    let columns = metadata.map { column in
                        SQLServerColumnDescription(
                            name: column.colName,
                            type: column.dataType,
                            length: column.length,
                            precision: column.precision,
                            scale: column.scale,
                            flags: column.flags
                        )
                    }
                    _ = continuation.yield(.metadata(columns))
                },
                onDone: { done in
                    let doneEvent = SQLServerStreamDone(status: done.status, rowCount: done.doneRowCount)
                    _ = continuation.yield(.done(doneEvent))
                },
                onMessage: { token, isError in
                    let message = SQLServerStreamMessage(
                        kind: isError ? .error : .info,
                        number: Int32(token.number),
                        message: token.messageText,
                        state: token.state,
                        severity: token.classValue
                    )
                    _ = continuation.yield(.message(message))
                },
            )

            let future = self.base.send(request, logger: self.logger)
            future.whenComplete { result in
                switch result {
                case .success:
                    continuation.finish()
                case .failure(let error):
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                // Send ATTENTION to abort the running batch if the consumer cancels.
                self.base.sendAttention()
            }
        }
    }

    // MARK: - Execution options (experimental)
    // These overloads accept advisory execution options but currently forward to the existing behavior unchanged.
    // They provide a stable surface for future mode routing, rowset sizing, and progress throttling.
    @available(macOS 12.0, *)
    public func streamQuery(_ sql: String, options: SqlServerExecutionOptions?) -> AsyncThrowingStream<SQLServerStreamEvent, Error> {
        _ = options // reserved for future implementation
        return self.streamQuery(sql)
    }

    @available(macOS 12.0, *)
    public func query(
        _ sql: String,
        options: SqlServerExecutionOptions?,
        logger: Logger? = nil
    ) -> AsyncThrowingStream<SQLServerStreamEvent, Error> {
        _ = logger // mirrors style; reserved for future use
        return self.streamQuery(sql, options: options)
    }

    public func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        executeWithRetry(operationName: "listDatabases") {
            self.metadataClient.listDatabases()
        }
    }

    @available(macOS 12.0, *)
    public func listDatabases() async throws -> [DatabaseMetadata] {
        try await listDatabases().get()
    }

    public func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        executeWithRetry(operationName: "listSchemas") {
            self.metadataClient.listSchemas(in: database)
        }
    }

    @available(macOS 12.0, *)
    public func listSchemas(in database: String? = nil) async throws -> [SchemaMetadata] {
        try await listSchemas(in: database).get()
    }

    public func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TableMetadata]> {
        // Always prefer the current database when caller doesn't override
        let db = database ?? self.currentDatabase
        return executeWithRetry(operationName: "listTables") {
            self.metadataClient.listTables(database: db, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) async throws -> [TableMetadata] {
        try await listTables(database: database, schema: schema, includeComments: includeComments).get()
    }

    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[ColumnMetadata]> {
        executeWithRetry(operationName: "listColumns") {
            self.metadataClient.listColumns(
                database: database,
                schema: schema,
                table: table,
                objectTypeHint: objectTypeHint,
                includeComments: includeComments
            )
        }
    }

    @available(macOS 12.0, *)
    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false
    ) async throws -> [ColumnMetadata] {
        try await listColumns(
            database: database,
            schema: schema,
            table: table,
            objectTypeHint: objectTypeHint,
            includeComments: includeComments
        ).get()
    }

    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String
    ) -> EventLoopFuture<[ParameterMetadata]> {
        executeWithRetry(operationName: "listParameters") {
            self.metadataClient.listParameters(database: database, schema: schema, object: object)
        }
    }

    @available(macOS 12.0, *)
    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String
    ) async throws -> [ParameterMetadata] {
        try await listParameters(database: database, schema: schema, object: object).get()
    }

    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        executeWithRetry(operationName: "listPrimaryKeys") {
            self.metadataClient.listPrimaryKeys(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listPrimaryKeys(database: database, schema: schema, table: table).get()
    }

    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        executeWithRetry(operationName: "listUniqueConstraints") {
            self.metadataClient.listUniqueConstraints(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listUniqueConstraints(database: database, schema: schema, table: table).get()
    }

    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String
    ) -> EventLoopFuture<[IndexMetadata]> {
        executeWithRetry(operationName: "listIndexes") {
            self.metadataClient.listIndexes(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String
    ) async throws -> [IndexMetadata] {
        try await listIndexes(database: database, schema: schema, table: table).get()
    }

    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String
    ) -> EventLoopFuture<[ForeignKeyMetadata]> {
        executeWithRetry(operationName: "listForeignKeys") {
            self.metadataClient.listForeignKeys(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String
    ) async throws -> [ForeignKeyMetadata] {
        try await listForeignKeys(database: database, schema: schema, table: table).get()
    }

    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String
    ) -> EventLoopFuture<[DependencyMetadata]> {
        executeWithRetry(operationName: "listDependencies") {
            self.metadataClient.listDependencies(database: database, schema: schema, object: object)
        }
    }

    @available(macOS 12.0, *)
    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String
    ) async throws -> [DependencyMetadata] {
        try await listDependencies(database: database, schema: schema, object: object).get()
    }

    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[TriggerMetadata]> {
        executeWithRetry(operationName: "listTriggers") {
            self.metadataClient.listTriggers(database: database, schema: schema, table: table, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false
    ) async throws -> [TriggerMetadata] {
        try await listTriggers(database: database, schema: schema, table: table, includeComments: includeComments).get()
    }

    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[RoutineMetadata]> {
        executeWithRetry(operationName: "listProcedures") {
            self.metadataClient.listProcedures(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [RoutineMetadata] {
        try await listProcedures(database: database, schema: schema, includeComments: includeComments).get()
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[RoutineMetadata]> {
        executeWithRetry(operationName: "listFunctions") {
            self.metadataClient.listFunctions(database: database, schema: schema, includeComments: includeComments)
        }
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) async throws -> [RoutineMetadata] {
        try await listFunctions(database: database, schema: schema, includeComments: includeComments).get()
    }

    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) -> EventLoopFuture<[ObjectDefinition]> {
        metadataClient.fetchObjectDefinitions(identifiers)
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) async throws -> [ObjectDefinition] {
        try await fetchObjectDefinitions(identifiers).get()
    }

    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind
    ) -> EventLoopFuture<ObjectDefinition?> {
        let identifier = SQLServerMetadataObjectIdentifier(database: database, schema: schema, name: name, kind: kind)
        return metadataClient.fetchObjectDefinitions([identifier]).map { $0.first }
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind
    ) async throws -> ObjectDefinition? {
        try await fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind).get()
    }

    // MARK: - SQL Agent Status

    /// Returns SQL Server Agent status (enabled + running) via metadata.
    public func fetchAgentStatus() -> EventLoopFuture<SQLServerMetadataClient.SQLServerAgentStatus> {
        executeWithRetry(operationName: "fetchAgentStatus") {
            self.metadataClient.fetchAgentStatus()
        }
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerMetadataClient.SQLServerAgentStatus {
        try await fetchAgentStatus().get()
    }

    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default
    ) -> EventLoopFuture<[MetadataSearchHit]> {
        executeWithRetry(operationName: "searchMetadata") {
            self.metadataClient.searchMetadata(query: query, database: database, schema: schema, scopes: scopes)
        }
    }

    
    @available(macOS 12.0, *)
    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default
    ) async throws -> [MetadataSearchHit] {
        try await searchMetadata(query: query, database: database, schema: schema, scopes: scopes).get()
    }

    /// Returns the SQL Server product version in the form `major.minor.build.revision`.
    public func serverVersion() -> EventLoopFuture<String> {
        executeWithRetry(operationName: "serverVersion") {
            self.metadataClient.serverVersion()
        }
    }

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await serverVersion().get()
    }

    /// Sends an ATTENTION signal to cancel the current request on this connection.
    /// Useful when higher-level code needs to abort a long-running query.
    public func cancelActiveRequest() {
        self.base.sendAttention()
    }

    deinit {
        // Never create new promises in deinit; close best‑effort without futures
        if let ownsGroup = ownsEventLoopGroup {
            base.closeSilently()
            ownsEventLoopGroup = nil
            // Shut down group asynchronously without creating an EventLoopFuture
            ownsGroup.shutdownGracefully { _ in }
        } else {
            if !reuseOnClose || base.isClosed {
                base.closeSilently()
            }
        }
    }

    internal init(
        base: TDSConnection,
        configuration: Configuration,
        metadataCache: MetadataCache<[ColumnMetadata]>?,
        logger: Logger,
        reuseOnClose: Bool,
        releaseClosure: @escaping @Sendable (_ close: Bool) -> EventLoopFuture<Void>
    ) {
        self.base = base
        self.base.logger = logger
        self.configuration = configuration
        self.sessionOptions = configuration.sessionOptions
        self.retryConfiguration = configuration.retryConfiguration
        self.sharedMetadataCache = metadataCache
        self._currentDatabase = configuration.login.database
        self.releaseClosure = releaseClosure
        self.reuseOnClose = reuseOnClose
    }

    internal func invalidate() -> EventLoopFuture<Void> {
        let future = release(close: true)
        guard let group = ownsEventLoopGroup else {
            return future
        }
        return future.flatMap {
            SQLServerClient.shutdownEventLoopGroup(group)
        }.map {
            self.ownsEventLoopGroup = nil
        }
    }

    private let base: TDSConnection
    private let configuration: Configuration
    private let sessionOptions: SessionOptions
    private let retryConfiguration: SQLServerRetryConfiguration
    private let sharedMetadataCache: MetadataCache<[ColumnMetadata]>?
    private lazy var metadataClient: SQLServerMetadataClient = {
        let baseLoop = base.eventLoop
        let executor: @Sendable (String) -> EventLoopFuture<[TDSRow]> = { [weak self] sql in
            guard let self else {
                return baseLoop.makeFailedFuture(SQLServerError.connectionClosed)
            }
            return self.runBatch(sql).map(\.rows)
        }
        return SQLServerMetadataClient(
            connection: self,
            configuration: configuration.metadataConfiguration,
            sharedCache: sharedMetadataCache,
            defaultDatabase: configuration.login.database,
            queryExecutor: executor
        )
    }()
    private let stateLock = NIOLock()
    private var _currentDatabase: String
    private var didApplySessionOptions: Bool = false
    private let releaseClosure: @Sendable (_ close: Bool) -> EventLoopFuture<Void>
    private let releaseLock = NIOLock()
    private var didRelease = false
    private var ownsEventLoopGroup: EventLoopGroup?
    private let reuseOnClose: Bool

    internal var underlying: TDSConnection {
        base
    }

    private func setCurrentDatabase(_ database: String) {
        stateLock.withLock { _currentDatabase = database }
        metadataClient.updateDefaultDatabase(database)
    }

    internal func bootstrapSession() -> EventLoopFuture<Void> {
        applySessionOptions(force: true)
    }

    internal func markSessionPrimed() {
        stateLock.withLock { didApplySessionOptions = true }
    }

    private func runBatch(_ sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        logger.trace("SQLServerConnection executing batch: \(sql)")

        let rows: [TDSRow] = []
        let dones: [SQLServerStreamDone] = []
        let messages: [SQLServerStreamMessage] = []
        let returnValues: [SQLServerReturnValue] = []
        let request = RawSqlRequest(
            sql: sql
        )
        logger.info("RUNBATCH_ENTRY: Sending RawSqlBatchRequest with Microsoft JDBC-compatible packet accumulation")
        logger.debug("SQLServerConnection runBatch sending request on loop=\(base.eventLoop) channelActive=\(!base.isClosed)")
        return base.send(request, logger: logger).flatMapThrowing { _ in
            self.logger.trace("SQLServerConnection completed batch")
            let result = SQLServerExecutionResult(rows: rows, done: dones, messages: messages, returnValues: returnValues)

            // Check for errors and throw them as Swift exceptions. Distinguish deadlocks (1205).
            if let firstError = messages.first(where: { $0.kind == .error }) {
                if firstError.number == 1205 {
                    throw SQLServerError.deadlockDetected(message: firstError.message)
                } else {
                    throw SQLServerError.sqlExecutionError(message: firstError.message)
                }
            }

            return result
        }
    }

    private func resetSessionState() -> EventLoopFuture<Void> {
        resetDatabaseIfNeeded(to: configuration.login.database).flatMap {
            self.applySessionOptions(force: true)
        }
    }

    private func resetDatabaseIfNeeded(to database: String) -> EventLoopFuture<Void> {
        let current = stateLock.withLock { _currentDatabase }
        if Self.equalsIgnoreCase(current, database) {
            return eventLoop.makeSucceededFuture(())
        }
        let sql = "USE \(Self.escapeIdentifier(database));"
        return executeWithRetry(operationName: "resetDatabase") {
            self.base.rawSql(sql).map { _ in
                self.setCurrentDatabase(database)
            }
        }
    }

    private func applySessionOptions(force: Bool = false) -> EventLoopFuture<Void> {
        if !force {
            let alreadyApplied = stateLock.withLock { didApplySessionOptions }
            if alreadyApplied {
                return eventLoop.makeSucceededFuture(())
            }
        }
        let statements = sessionOptions.buildStatements()
        if statements.isEmpty {
            stateLock.withLock { didApplySessionOptions = true }
            return eventLoop.makeSucceededFuture(())
        }
        let batch = statements.joined(separator: " ")
        // Do not use executeWithRetry during bootstrap; if the channel closes we want an immediate
        // failure without scheduling backoff on an event loop that may be shutting down.
        self.logger.debug("SQLServerConnection applySessionOptions executing on loop=\(self.eventLoop) channelActive=\(!self.base.isClosed) batch=\(batch)")
        return self.base.rawSql(batch).map { _ in
            self.stateLock.withLock { self.didApplySessionOptions = true }
        }
    }

    private func shutdownGroupIfNeeded() -> EventLoopFuture<Void> {
        guard let group = ownsEventLoopGroup else {
            return eventLoop.makeSucceededFuture(())
        }
        return SQLServerClient.shutdownEventLoopGroup(group).map {
            self.ownsEventLoopGroup = nil
        }
    }

    private static func equalsIgnoreCase(_ lhs: String, _ rhs: String) -> Bool {
        lhs.caseInsensitiveCompare(rhs) == .orderedSame
    }

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    private func executeWithRetry<Result>(
        operationName: String,
        operation: @escaping () -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        func attempt(_ currentAttempt: Int) -> EventLoopFuture<Result> {
            operation().flatMapError { error in
                let normalized = SQLServerError.normalize(error)
                guard self.shouldRetry(error: normalized, attempt: currentAttempt) else {
                    return self.eventLoop.makeFailedFuture(normalized)
                }
                self.logger.debug("SQLServerConnection \(operationName) attempt \(currentAttempt) failed with \(String(describing: error)); retrying.")
                // Avoid scheduling timers on EventLoops that may be shutting down (which can crash or log loudly).
                // Retry immediately on the same event loop. Tests and callers can implement external backoff if needed.
                self.logger.debug("\(operationName) retrying immediately without backoff (attempt=\(currentAttempt))")
                return attempt(currentAttempt + 1)
            }
        }

        return attempt(1)
    }

    private func shouldRetry(error: Swift.Error, attempt: Int) -> Bool {
        if attempt >= retryConfiguration.maximumAttempts {
            return false
        }
        // Do not attempt retries if the underlying channel is already closed.
        // Scheduling backoff on a closed EventLoop will crash in newer NIO versions.
        if base.isClosed {
            return false
        }
        return retryConfiguration.shouldRetry(error)
    }

    private func release(close: Bool) -> EventLoopFuture<Void> {
        let alreadyReleased = releaseLock.withLock { () -> Bool in
            if didRelease {
                return true
            }
            didRelease = true
            return false
        }

        if alreadyReleased {
            return eventLoop.makeSucceededFuture(())
        }

        return releaseClosure(close)
    }

    internal static func resolveSocketAddresses(
        hostname: String,
        port: Int,
        transparentResolution: Bool,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<[SocketAddress]> {
        eventLoop.submit {
            if transparentResolution {
                return try makeAllSocketAddresses(hostname: hostname, port: port)
            } else {
                let address = try SocketAddress.makeAddressResolvingHost(hostname, port: port)
                return [address]
            }
        }
    }
    
    private static func makeAllSocketAddresses(hostname: String, port: Int) throws -> [SocketAddress] {
        var hints = addrinfo(
            ai_flags: AI_ADDRCONFIG,
            ai_family: AF_UNSPEC,
            ai_socktype: SOCK_STREAM,
            ai_protocol: IPPROTO_TCP,
            ai_addrlen: 0,
            ai_canonname: nil,
            ai_addr: nil,
            ai_next: nil
        )
        var infoPointer: UnsafeMutablePointer<addrinfo>?
        let portString = "\(port)"
        let error = getaddrinfo(hostname, portString, &hints, &infoPointer)
        guard error == 0 else {
            throw IOError(errnoCode: error, reason: "getaddrinfo")
        }
        defer {
            if let infoPointer {
                freeaddrinfo(infoPointer)
            }
        }
        
        var addresses: [SocketAddress] = []
        var cursor = infoPointer
        while let entry = cursor?.pointee {
            if let addr = entry.ai_addr {
                switch entry.ai_family {
                case AF_INET:
                    let address = addr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { pointer in
                        pointer.pointee
                    }
                    addresses.append(SocketAddress(address))
                case AF_INET6:
                    let address = addr.withMemoryRebound(to: sockaddr_in6.self, capacity: 1) { pointer in
                        pointer.pointee
                    }
                    addresses.append(SocketAddress(address))
                default:
                    break
                }
            }
            cursor = entry.ai_next
        }
        
        if addresses.isEmpty {
            throw SQLServerError.connectionClosed
        }
        
        return addresses
    }
    
    internal static func establishTDSConnection(
        addresses: [SocketAddress],
        tlsConfiguration: TLSConfiguration?,
        serverHostname: String,
        on eventLoop: EventLoop,
        logger: Logger
    ) -> EventLoopFuture<TDSConnection> {
        func attempt(_ index: Int) -> EventLoopFuture<TDSConnection> {
            guard index < addresses.count else {
                return eventLoop.makeFailedFuture(SQLServerError.connectionClosed)
            }
            let address = addresses[index]
            return TDSConnection.connect(
                to: address,
                tlsConfiguration: tlsConfiguration,
                serverHostname: serverHostname,
                on: eventLoop
            ).flatMapError { error in
                logger.warning("SQLServerConnection failed to connect to \(address). \(error)")
                if index + 1 < addresses.count {
                    return attempt(index + 1)
                } else {
                    return eventLoop.makeFailedFuture(error)
                }
            }
        }
        return attempt(0)
    }
}

extension SQLServerConnection: @unchecked Sendable {}
