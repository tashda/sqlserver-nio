import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOPosix
import NIOSSL

public final class SQLServerClient {
    public enum EventLoopGroupProvider {
        case shared(EventLoopGroup)
        case createNew(numberOfThreads: Int)
    }

    public struct Configuration {
        public var connection: SQLServerConnection.Configuration
        public var poolConfiguration: SQLServerConnectionPool.Configuration
        public var retryConfiguration: SQLServerRetryConfiguration {
            get { connection.retryConfiguration }
            set { connection.retryConfiguration = newValue }
        }
        public var metadataConfiguration: SQLServerMetadataClient.Configuration {
            get { connection.metadataConfiguration }
            set { connection.metadataConfiguration = newValue }
        }

        public init(
            connection: SQLServerConnection.Configuration,
            poolConfiguration: SQLServerConnectionPool.Configuration = .init()
        ) {
            self.connection = connection
            self.poolConfiguration = poolConfiguration
        }

        public init(
            hostname: String,
            port: Int = 1433,
            login: SQLServerConnection.Configuration.Login,
            tlsConfiguration: TLSConfiguration? = .makeClientConfiguration(),
            poolConfiguration: SQLServerConnectionPool.Configuration = .init(),
            metadataConfiguration: SQLServerMetadataClient.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init()
        ) {
            self.connection = SQLServerConnection.Configuration(
                hostname: hostname,
                port: port,
                login: login,
                tlsConfiguration: tlsConfiguration,
                metadataConfiguration: metadataConfiguration,
                retryConfiguration: retryConfiguration
            )
            self.poolConfiguration = poolConfiguration
        }

        public var hostname: String {
            get { connection.hostname }
            set { connection.hostname = newValue }
        }

        public var port: Int {
            get { connection.port }
            set { connection.port = newValue }
        }

        public var login: SQLServerConnection.Configuration.Login {
            get { connection.login }
            set { connection.login = newValue }
        }

        public var tlsConfiguration: TLSConfiguration? {
            get { connection.tlsConfiguration }
            set { connection.tlsConfiguration = newValue }
        }
    }

    public static func connect(
        configuration: Configuration,
        eventLoopGroupProvider: EventLoopGroupProvider = .createNew(numberOfThreads: System.coreCount),
        logger: Logger = Logger(label: "tds.sqlserver.client")
    ) -> EventLoopFuture<SQLServerClient> {
        let eventLoopGroup: EventLoopGroup
        let ownsGroup: Bool

        switch eventLoopGroupProvider {
        case .shared(let group):
            eventLoopGroup = group
            ownsGroup = false
        case .createNew(let numberOfThreads):
            eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: numberOfThreads)
            ownsGroup = true
        }

        let connectionFactory: (EventLoop) -> EventLoopFuture<TDSConnection> = { eventLoop in
            let loginConfiguration = TDSLoginConfiguration(
                serverName: configuration.connection.hostname,
                port: configuration.connection.port,
                database: configuration.connection.login.database,
                authentication: configuration.connection.login.authentication
            )

            return SQLServerConnection.resolveSocketAddress(
                hostname: configuration.connection.hostname,
                port: configuration.connection.port,
                on: eventLoop
            ).flatMap { address in
                TDSConnection.connect(
                    to: address,
                    tlsConfiguration: configuration.connection.tlsConfiguration,
                    serverHostname: configuration.connection.hostname,
                    on: eventLoop
                )
            }.flatMap { connection in
                connection.login(configuration: loginConfiguration).flatMap { _ -> EventLoopFuture<TDSConnection> in
                    let statements = configuration.connection.sessionOptions.buildStatements()
                    guard !statements.isEmpty else {
                        return eventLoop.makeSucceededFuture(connection)
                    }
                    let batch = statements.joined(separator: " " )
                    return connection.rawSql(batch).map { _ in connection }
                }.flatMapError { error in
                    connection.close().flatMapThrowing { throw error }
                }
            }
        }

        let pool = SQLServerConnectionPool(
            configuration: configuration.poolConfiguration,
            eventLoopGroup: eventLoopGroup,
            logger: logger,
            connectionFactory: connectionFactory
        )
        pool.start()

        let warmupLoop = eventLoopGroup.next()
        let warmupFuture = pool.withConnection(on: warmupLoop) { connection in
            connection.eventLoop.makeSucceededFuture(())
        }

        return warmupFuture.map { _ in
            SQLServerClient(
                configuration: configuration,
                eventLoopGroup: eventLoopGroup,
                ownsEventLoopGroup: ownsGroup,
                pool: pool,
                logger: logger
            )
        }.flatMapError { error in
            let cleanup = pool.shutdownGracefully().flatMap { _ -> EventLoopFuture<Void> in
                guard ownsGroup else {
                    return eventLoopGroup.next().makeSucceededFuture(())
                }
                return shutdownEventLoopGroup(eventLoopGroup)
            }

            return cleanup.flatMapThrowing {
                throw error
            }
        }
    }

    public func shutdownGracefully() -> EventLoopFuture<Void> {
        let shouldShutdown = stateLock.withLock { () -> Bool in
            if isShutdown {
                return false
            }
            isShutdown = true
            return true
        }

        if !shouldShutdown {
            return eventLoopGroup.next().makeSucceededFuture(())
        }

        let poolShutdown = pool.shutdownGracefully()
        if ownsEventLoopGroup {
            return poolShutdown.flatMap {
                SQLServerClient.shutdownEventLoopGroup(self.eventLoopGroup)
            }
        } else {
            return poolShutdown
        }
    }

    public func withConnection<Result>(
        on eventLoop: EventLoop? = nil,
        _ operation: @escaping (SQLServerConnection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return executeWithRetry(operationName: "withConnection", on: loop) {
            self.pool.checkout(on: loop).flatMap { pooled -> EventLoopFuture<Result> in
                let sqlConnection = self.makeConnection(from: pooled)
                return operation(sqlConnection).flatMap { value in
                    sqlConnection.close().map { value }
                }.flatMapError { error in
                    sqlConnection.invalidate().flatMapThrowing { throw error }
                }
            }
        }
    }

    @available(macOS 12.0, *)
    public func withConnection<Result>(
        on eventLoop: EventLoop? = nil,
        _ operation: @escaping (SQLServerConnection) async throws -> Result
    ) async throws -> Result {
        try await withCheckedThrowingContinuation { continuation in
            let future: EventLoopFuture<Result> = self.withConnection(on: eventLoop) { connection in
                let promise = connection.eventLoop.makePromise(of: Result.self)
                connection.eventLoop.execute {
                    Task {
                        do {
                            let value = try await operation(connection)
                            promise.succeed(value)
                        } catch {
                            promise.fail(error)
                        }
                    }
                }
                return promise.futureResult
            }

            future.whenComplete { result in
                continuation.resume(with: result)
            }
        }
    }

    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.execute(sql)
        }
    }

    @available(macOS 12.0, *)
    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await execute(sql, on: eventLoop).get()
    }

    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[TDSRow]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.query(sql)
        }
    }

    @available(macOS 12.0, *)
    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TDSRow] {
        try await query(sql, on: eventLoop).get()
    }

    public func queryScalar<T: TDSDataConvertible>(
        _ sql: String,
        as type: T.Type = T.self,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<T?> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.queryScalar(sql, as: type)
        }
    }

    @available(macOS 12.0, *)
    public func queryScalar<T: TDSDataConvertible>(
        _ sql: String,
        as type: T.Type = T.self,
        on eventLoop: EventLoop? = nil
    ) async throws -> T? {
        try await queryScalar(sql, as: type, on: eventLoop).get()
    }

    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier],
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ObjectDefinition]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.fetchObjectDefinitions(identifiers)
        }
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier],
        on eventLoop: EventLoop? = nil
    ) async throws -> [ObjectDefinition] {
        try await fetchObjectDefinitions(identifiers, on: eventLoop).get()
    }

    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<ObjectDefinition?> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind)
        }
    }

    @available(macOS 12.0, *)
    public func fetchObjectDefinition(
        database: String? = nil,
        schema: String,
        name: String,
        kind: SQLServerMetadataObjectIdentifier.Kind,
        on eventLoop: EventLoop? = nil
    ) async throws -> ObjectDefinition? {
        try await fetchObjectDefinition(database: database, schema: schema, name: name, kind: kind, on: eventLoop).get()
    }

        public func searchMetadata(
            query: String,
            database: String? = nil,
            schema: String? = nil,
            scopes: MetadataSearchScope = .default,
            on eventLoop: EventLoop? = nil
        ) -> EventLoopFuture<[MetadataSearchHit]> {
            let loop = eventLoop ?? eventLoopGroup.next()
            return withConnection(on: loop) { connection in
                connection.searchMetadata(query: query, database: database, schema: schema, scopes: scopes)
            }
       }

    @available(macOS 12.0, *)
    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default,
        on eventLoop: EventLoop? = nil
    ) async throws -> [MetadataSearchHit] {
        try await searchMetadata(query: query, database: database, schema: schema, scopes: scopes, on: eventLoop).get()
    }

    public func listDatabases(on eventLoop: EventLoop? = nil) -> EventLoopFuture<[DatabaseMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listDatabases()
        }
    }

    @available(macOS 12.0, *)
    public func listDatabases(on eventLoop: EventLoop? = nil) async throws -> [DatabaseMetadata] {
        try await listDatabases(on: eventLoop).get()
    }

    public func listSchemas(
        in database: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[SchemaMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listSchemas(in: database)
        }
    }

    @available(macOS 12.0, *)
    public func listSchemas(
        in database: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [SchemaMetadata] {
        try await listSchemas(in: database, on: eventLoop).get()
    }

    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[TableMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listTables(database: database, schema: schema)
        }
    }

    @available(macOS 12.0, *)
    public func listTables(
        database: String? = nil,
        schema: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TableMetadata] {
        try await listTables(database: database, schema: schema, on: eventLoop).get()
    }

    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ColumnMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listColumns(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ColumnMetadata] {
        try await listColumns(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ParameterMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listParameters(database: database, schema: schema, object: object)
        }
    }

    @available(macOS 12.0, *)
    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ParameterMetadata] {
        try await listParameters(database: database, schema: schema, object: object, on: eventLoop).get()
    }

    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listPrimaryKeys(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listPrimaryKeys(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listUniqueConstraints(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [KeyConstraintMetadata] {
        try await listUniqueConstraints(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[IndexMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listIndexes(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [IndexMetadata] {
        try await listIndexes(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[ForeignKeyMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listForeignKeys(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [ForeignKeyMetadata] {
        try await listForeignKeys(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[DependencyMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listDependencies(database: database, schema: schema, object: object)
        }
    }

    @available(macOS 12.0, *)
    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> [DependencyMetadata] {
        try await listDependencies(database: database, schema: schema, object: object, on: eventLoop).get()
    }

    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[TriggerMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listTriggers(database: database, schema: schema, table: table)
        }
    }

    @available(macOS 12.0, *)
    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [TriggerMetadata] {
        try await listTriggers(database: database, schema: schema, table: table, on: eventLoop).get()
    }

    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listProcedures(database: database, schema: schema)
        }
    }

    @available(macOS 12.0, *)
    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await listProcedures(database: database, schema: schema, on: eventLoop).get()
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        withConnection(on: eventLoop) { connection in
            connection.listFunctions(database: database, schema: schema)
        }
    }

    @available(macOS 12.0, *)
    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        on eventLoop: EventLoop? = nil
    ) async throws -> [RoutineMetadata] {
        try await listFunctions(database: database, schema: schema, on: eventLoop).get()
    }

    deinit {
        assert(stateLock.withLock { isShutdown }, "SQLServerClient deinitialized without shutdownGracefully()")
    }

    private init(
        configuration: Configuration,
        eventLoopGroup: EventLoopGroup,
        ownsEventLoopGroup: Bool,
        pool: SQLServerConnectionPool,
        logger: Logger
    ) {
        self.configuration = configuration
        self.eventLoopGroup = eventLoopGroup
        self.ownsEventLoopGroup = ownsEventLoopGroup
        self.pool = pool
        self.logger = logger
        self.retryConfiguration = configuration.retryConfiguration
        if configuration.metadataConfiguration.enableColumnCache {
            self.metadataCache = MetadataCache<[ColumnMetadata]>()
        } else {
            self.metadataCache = nil
        }
    }

    private let configuration: Configuration
    private let eventLoopGroup: EventLoopGroup
    private let ownsEventLoopGroup: Bool
    private let pool: SQLServerConnectionPool
    private let logger: Logger
    private let retryConfiguration: SQLServerRetryConfiguration
    private let metadataCache: MetadataCache<[ColumnMetadata]>?

    private let stateLock = NIOLock()
    private var isShutdown = false

    private func makeConnection(from pooled: SQLServerConnectionPool.PooledConnection) -> SQLServerConnection {
        let connectionConfiguration = configuration.connection
        let baseConnection = pooled.base
        let connection = SQLServerConnection(
            base: baseConnection,
            configuration: connectionConfiguration,
            metadataCache: metadataCache,
            logger: logger,
            reuseOnClose: true,
            releaseClosure: { close in
                if close || baseConnection.isClosed {
                    return pooled.release(close: true)
                } else {
                    return pooled.release()
                }
            }
        )
        connection.markSessionPrimed()
        return connection
    }

    private func executeWithRetry<Result>(
        operationName: String,
        on eventLoop: EventLoop,
        operation: @escaping () -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        if isClientShutdown {
            return eventLoop.makeFailedFuture(SQLServerError.clientShutdown)
        }

        func attempt(_ currentAttempt: Int) -> EventLoopFuture<Result> {
            if self.isClientShutdown {
                return eventLoop.makeFailedFuture(SQLServerError.clientShutdown)
            }

            return operation().flatMapError { error in
                let normalized = SQLServerError.normalize(error)
                guard self.shouldRetry(error: normalized, attempt: currentAttempt) else {
                    return eventLoop.makeFailedFuture(normalized)
                }
                self.logger.debug("Operation \(operationName) attempt \(currentAttempt) failed with \(normalized); retrying.")
                let backoff = self.retryConfiguration.backoffStrategy(currentAttempt)
                return eventLoop.scheduleTask(in: backoff) { () }
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

    private var isClientShutdown: Bool {
        stateLock.withLock { isShutdown }
    }

    internal static func shutdownEventLoopGroup(_ group: EventLoopGroup) -> EventLoopFuture<Void> {
        let promise = group.next().makePromise(of: Void.self)
        group.shutdownGracefully { error in
            if let error {
                promise.fail(error)
            } else {
                promise.succeed(())
            }
        }
        return promise.futureResult
    }
}

extension SQLServerClient: @unchecked Sendable {}
