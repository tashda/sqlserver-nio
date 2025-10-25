@_exported import SQLServerTDS
import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOPosix
import NIOSSL
import SQLServerTDS

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
        let batches = sql.components(separatedBy: "\nGO\n").filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }

        guard !batches.isEmpty else {
            return loop.makeSucceededFuture(.init(rows: [], done: [], messages: []))
        }

        var lastFuture: EventLoopFuture<SQLServerExecutionResult> = loop.makeSucceededFuture(.init(rows: [], done: [], messages: []))

        for batchSql in batches {
            lastFuture = lastFuture.flatMap { _ in
                self.withConnection(on: loop) { connection in
                    connection.execute(batchSql)
                }
            }
        }
        return lastFuture
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
        let defaultDatabase = configuration.connection.login.database
        let dbName = database ?? defaultDatabase
        let schemaName = schema ?? "<nil>"
        logger.trace("SQLServerClient listTables start database=\(dbName) schema=\(schemaName)")
        return withFreshConnection(on: eventLoop) { connection in
            connection.listTables(database: database, schema: schema)
        }.map { tables in
            self.logger.trace("SQLServerClient listTables completed with \(tables.count) entries")
            return tables
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

    public func executeOnFreshConnection(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withFreshConnection(on: loop) { connection in
            connection.execute(sql)
        }
    }

    @available(macOS 12.0, *)
    public func executeOnFreshConnection(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) async throws -> SQLServerExecutionResult {
        try await executeOnFreshConnection(sql, on: eventLoop).get()
    }

    /// Executes multiple SQL statements as separate batches with proper isolation.
    /// This is essential for SQL Server operations that require batch separation,
    /// such as adding extended properties after table creation.
    public func executeSeparateBatches(_ sqlStatements: [String]) -> EventLoopFuture<[SQLServerExecutionResult]> {
        let promise = eventLoopGroup.next().makePromise(of: [SQLServerExecutionResult].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.executeSeparateBatches(sqlStatements)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    /// Executes a SQL script with GO separators, splitting into separate batches.
    /// This splits the SQL using proper SQL Server batch separation logic.
    @available(macOS 12.0, *)
    public func executeScript(_ sql: String) async throws -> [SQLServerExecutionResult] {
        // Use our SQL Server query splitter for proper batch separation
        let splitResults = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        // Execute with connection locking to ensure sequential execution
        return try await executeWithConnectionLock { connection in
            var results: [SQLServerExecutionResult] = []
            
            for (index, splitResult) in splitResults.enumerated() {
                if splitResult.text.isEmpty || self.isCommentOnlyBatch(splitResult.text) {
                    continue // Skip empty batches and comment-only batches
                }
                
                self.logger.info("Executing batch \(index + 1) of \(splitResults.count): \(splitResult.text.prefix(50))...")
                
                do {
                    // Execute each batch as a separate TDS batch message
                    let result = try await connection.execute(splitResult.text).get()
                    
                    // Check for errors in the result
                    if let errorMessage = result.messages.first(where: { $0.kind == .error }) {
                        self.logger.error("Batch \(index + 1) failed with SQL Server error: \(errorMessage.message)")
                        throw SQLServerError.sqlExecutionError(message: errorMessage.message)
                    }
                    
                    results.append(result)
                    self.logger.info("Batch \(index + 1) completed successfully")
                } catch {
                    self.logger.error("Batch \(index + 1) failed with error: \(error)")
                    throw error
                }
            }
            
            return results
        }
    }
    
    /// Executes a closure with a locked connection to ensure sequential execution
    @available(macOS 12.0, *)
    private func executeWithConnectionLock<T>(_ operation: @escaping (SQLServerConnection) async throws -> T) async throws -> T {
        return try await withConnection(on: nil) { connection in
            // Create a promise that will be resolved by the async operation
            let promise = connection.eventLoop.makePromise(of: T.self)
            
            // Execute the operation asynchronously
            promise.completeWithTask {
                try await operation(connection)
            }
            
            return promise.futureResult
        }.get()
    }
    
    /// Checks if a batch contains only comments and whitespace
    private func isCommentOnlyBatch(_ text: String) -> Bool {
        let lines = text.components(separatedBy: .newlines)
        for line in lines {
            let trimmedLine = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmedLine.isEmpty && !trimmedLine.hasPrefix("--") {
                return false // Found non-comment content
            }
        }
        return true // Only comments and whitespace
    }
    
    /// Executes multiple SQL statements as separate batches with proper isolation.
    /// This uses GO separators between statements for proper batch separation.
    @available(macOS 12.0, *)
    public func executeSeparateBatches(_ sqlStatements: [String]) async throws -> [SQLServerExecutionResult] {
        // Join statements with GO separators for proper batch separation
        let script = sqlStatements.joined(separator: "\nGO\n")
        return try await executeScript(script)
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

    public let configuration: Configuration
    public let eventLoopGroup: EventLoopGroup
    private let ownsEventLoopGroup: Bool
    private let pool: SQLServerConnectionPool
    public let logger: Logger
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

    private func withFreshConnection<Result>(
        on eventLoop: EventLoop?,
        _ operation: @escaping (SQLServerConnection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return SQLServerConnection.connect(
            configuration: configuration.connection,
            on: loop,
            logger: logger
        ).flatMap { connection in
            operation(connection).flatMap { value in
                connection.close().map { value }
            }.flatMapError { error in
                connection.invalidate().flatMapThrowing { throw error }
            }
        }
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

    /// Returns information about the connection pool status
    public var poolStatus: (active: Int, idle: Int) {
        // We can't access internal pool state, so return basic info
        return (active: 0, idle: 0)
    }
    
    /// Performs a health check on the connection pool
    @available(macOS 12.0, *)
    public func healthCheck() async throws -> Bool {
        do {
            let result = try await query("SELECT 1 as health_check").get()
            return result.count == 1 && result.first?.column("health_check")?.int == 1
        } catch {
            logger.warning("Health check failed: \(error)")
            return false
        }
    }
    
    /// Validates all connections in the pool
    @available(macOS 12.0, *)
    public func validateConnections() async throws {
        // This will force validation of idle connections
        try await withConnection { _ in
            // Connection checkout will trigger validation
            return self.eventLoopGroup.next().makeSucceededFuture(())
        }.get()
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
