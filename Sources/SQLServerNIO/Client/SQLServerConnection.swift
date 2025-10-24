import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOPosix
import NIOSSL

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
            sessionOptions: SessionOptions = .ssmsDefaults
        ) {
            self.hostname = hostname
            self.port = port
            self.login = login
            self.tlsConfiguration = tlsConfiguration
            self.metadataConfiguration = metadataConfiguration
            self.retryConfiguration = retryConfiguration
            self.sessionOptions = sessionOptions
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

        let promise = group.next().makePromise(of: SQLServerConnection.self)

        connect(
            configuration: configuration,
            on: group.next(),
            logger: logger
        ).whenComplete { result in
            switch result {
            case .success(let connection):
                connection.ownsEventLoopGroup = ownsGroup ? group : nil
                promise.succeed(connection)
            case .failure(let error):
                if ownsGroup {
                    SQLServerClient.shutdownEventLoopGroup(group).whenComplete { _ in
                        promise.fail(error)
                    }
                } else {
                    promise.fail(error)
                }
            }
        }

        return promise.futureResult
    }

    public static func connect(
        configuration: Configuration,
        on eventLoop: EventLoop,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) -> EventLoopFuture<SQLServerConnection> {
        let loginConfiguration = TDSLoginConfiguration(
            serverName: configuration.hostname,
            port: configuration.port,
            database: configuration.login.database,
            authentication: configuration.login.authentication
        )

        return resolveSocketAddress(
            hostname: configuration.hostname,
            port: configuration.port,
            on: eventLoop
        ).flatMap { address in
            TDSConnection.connect(
                to: address,
                tlsConfiguration: configuration.tlsConfiguration,
                serverHostname: configuration.hostname,
                on: eventLoop
            )
        }.flatMap { connection in
            connection.login(configuration: loginConfiguration).map { connection }.flatMapError { error in
                connection.close().flatMapThrowing { throw error }
            }
        }.flatMap { connection in
            let sqlConnection = SQLServerConnection(
                base: connection,
                configuration: configuration,
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

    public var eventLoop: EventLoop {
        base.eventLoop
    }

    public var logger: Logger {
        base.logger
    }

    public var currentDatabase: String {
        stateLock.withLock { _currentDatabase }
    }

    public func changeDatabase(_ database: String) -> EventLoopFuture<Void> {
        let current = stateLock.withLock { _currentDatabase }
        if Self.equalsIgnoreCase(current, database) {
            return eventLoop.makeSucceededFuture(())
        }
        return executeWithRetry(operationName: "changeDatabase") {
            let sql = "USE \(Self.escapeIdentifier(database));"
            return self.base.rawSql(sql).map { _ in
                self.setCurrentDatabase(database)
            }
        }
    }

    @available(macOS 12.0, *)
    public func changeDatabase(_ database: String) async throws {
        try await changeDatabase(database).get()
    }

    public func close() -> EventLoopFuture<Void> {
        if reuseOnClose {
            return resetSessionState().flatMapError { error in
                self.logger.warning("SQLServerConnection failed to reset session before reuse: \(error)")
                return self.release(close: true).flatMap { _ in
                    self.shutdownGroupIfNeeded().flatMapThrowing { throw error }
                }
            }.flatMap { _ in
                self.release(close: false)
            }.flatMap { _ in
                self.shutdownGroupIfNeeded()
            }
        } else {
            return release(close: true).flatMap { _ in
                self.shutdownGroupIfNeeded()
            }
        }
    }

    public func execute(_ sql: String) -> EventLoopFuture<SQLServerExecutionResult> {
        executeWithRetry(operationName: "execute") {
            self.runBatch(sql)
        }
    }

    @available(macOS 12.0, *)
    public func execute(_ sql: String) async throws -> SQLServerExecutionResult {
        let future: EventLoopFuture<SQLServerExecutionResult> = execute(sql)
        return try await future.get()
    }

    public func query(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        execute(sql).map(\.rows)
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [TDSRow] {
        let result = try await execute(sql).get()
        return result.rows
    }

    public func queryScalar<T: TDSDataConvertible>(_ sql: String, as type: T.Type = T.self) -> EventLoopFuture<T?> {
        execute(sql).map { result in
            guard
                let row = result.rows.first,
                let firstColumn = row.columnMetadata.colData.first?.colName,
                let valueData = row.column(firstColumn),
                let value = T(tdsData: valueData)
            else {
                return nil
            }
            return value
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
            let request = RawSqlBatchRequest(
                sqlBatch: message,
                logger: self.logger,
                onRow: { row in
                    _ = continuation.yield(.row(row))
                },
                onMetadata: { metadata in
                    let columns = metadata.colData.map { column in
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
                }
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
                // TODO: send ATTENTION token for graceful cancellation.
            }
        }
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

    public func listTables(database: String? = nil, schema: String? = nil) -> EventLoopFuture<[TableMetadata]> {
        executeWithRetry(operationName: "listTables") {
            self.metadataClient.listTables(database: database, schema: schema)
        }
    }

    @available(macOS 12.0, *)
    public func listTables(database: String? = nil, schema: String? = nil) async throws -> [TableMetadata] {
        try await listTables(database: database, schema: schema).get()
    }

    public func listColumns(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[ColumnMetadata]> {
        executeWithRetry(operationName: "listColumns") {
            self.metadataClient.listColumns(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listColumns(database: String? = nil, schema: String, table: String) async throws -> [ColumnMetadata] {
        try await listColumns(database: database, schema: schema, table: table).get()
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
        table: String? = nil
    ) -> EventLoopFuture<[TriggerMetadata]> {
        executeWithRetry(operationName: "listTriggers") {
            self.metadataClient.listTriggers(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) async throws -> [TriggerMetadata] {
        try await listTriggers(database: database, schema: schema, table: table).get()
    }

    public func listProcedures(
        database: String? = nil,
        schema: String? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        executeWithRetry(operationName: "listProcedures") {
            self.metadataClient.listProcedures(database: database, schema: schema)
        }
    }

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil
    ) async throws -> [RoutineMetadata] {
        try await listProcedures(database: database, schema: schema).get()
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        executeWithRetry(operationName: "listFunctions") {
            self.metadataClient.listFunctions(database: database, schema: schema)
        }
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil
    ) async throws -> [RoutineMetadata] {
        try await listFunctions(database: database, schema: schema).get()
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

    deinit {
        if let ownsGroup = ownsEventLoopGroup {
            _ = release(close: true)
            _ = SQLServerClient.shutdownEventLoopGroup(ownsGroup)
        } else {
            _ = release(close: !reuseOnClose || base.isClosed)
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
            connection: base,
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
        var rows: [TDSRow] = []
        var dones: [SQLServerStreamDone] = []
        var messages: [SQLServerStreamMessage] = []
        let request = RawSqlBatchRequest(
            sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sql),
            logger: logger,
            onRow: { row in rows.append(row) },
            onMetadata: nil,
            onDone: { token in
                let doneEvent = SQLServerStreamDone(status: token.status, rowCount: token.doneRowCount)
                dones.append(doneEvent)
            },
            onMessage: { token, isError in
                let message = SQLServerStreamMessage(
                    kind: isError ? .error : .info,
                    number: Int32(token.number),
                    message: token.messageText,
                    state: token.state,
                    severity: token.classValue
                )
                messages.append(message)
            }
        )
        return base.send(request, logger: logger).map {
            self.logger.trace("SQLServerConnection completed batch")
            return SQLServerExecutionResult(rows: rows, done: dones, messages: messages)
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
        return executeWithRetry(operationName: "applySessionOptions") {
            self.base.rawSql(batch).map { _ in
                self.stateLock.withLock { self.didApplySessionOptions = true }
            }
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
                let backoff = self.retryConfiguration.backoffStrategy(currentAttempt)
                return self.eventLoop.scheduleTask(in: backoff) { () }
                    .futureResult
                    .flatMap {
                        attempt(currentAttempt + 1)
                    }
            }
        }

        return attempt(1)
    }

    private func shouldRetry(error: Swift.Error, attempt: Int) -> Bool {
        if attempt >= retryConfiguration.maximumAttempts {
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

    internal static func resolveSocketAddress(
        hostname: String,
        port: Int,
        on eventLoop: EventLoop
    ) -> EventLoopFuture<SocketAddress> {
        eventLoop.submit {
            try SocketAddress.makeAddressResolvingHost(hostname, port: port)
        }
    }
}

extension SQLServerConnection: @unchecked Sendable {}
