import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

public final class SQLServerClient: @unchecked Sendable {
    internal enum EventLoopGroupProvider {
        case shared(EventLoopGroup)
        case createNew(numberOfThreads: Int)
    }

    public let configuration: Configuration
    internal let eventLoopGroup: EventLoopGroup
    internal private(set) var ownsEventLoopGroup: Bool
    internal let pool: SQLServerConnectionPool
    public let logger: Logger
    internal let retryConfiguration: SQLServerRetryConfiguration
    internal let metadataCache: MetadataCache<[ColumnMetadata]>?

    internal let stateLock = NIOLock()
    internal var _isShutdown = false
    internal var inFlightOperations: Int = 0
    internal var drainWaiters: [EventLoopPromise<Void>] = []

    public static func connect(
        configuration: Configuration,
        logger: Logger = Logger(label: "tds.sqlserver.client")
    ) async throws -> SQLServerClient {
        try await connect(
            configuration: configuration,
            numberOfThreads: System.coreCount,
            logger: logger
        )
    }

    public static func connect(
        configuration: Configuration,
        numberOfThreads: Int,
        logger: Logger = Logger(label: "tds.sqlserver.client")
    ) async throws -> SQLServerClient {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: numberOfThreads)
        do {
            // Use .shared so the ELF connect does NOT own or shut down the ELG on error.
            // We manage the ELG lifecycle here in async code instead.
            let client = try await connect(
                configuration: configuration,
                eventLoopGroupProvider: .shared(eventLoopGroup),
                logger: logger
            ).get()
            // Transfer ELG ownership to the client so it shuts down the group on close.
            client.transferEventLoopGroupOwnership(eventLoopGroup)
            return client
        } catch {
            // Clean up the ELG using pure async — no ELF hopping race.
            try? await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                eventLoopGroup.shutdownGracefully { shutdownError in
                    if let shutdownError {
                        continuation.resume(throwing: shutdownError)
                    } else {
                        continuation.resume()
                    }
                }
            }
            throw SQLServerError.normalize(error)
        }
    }

    public static func connect(
        hostname: String,
        port: Int = 1433,
        database: String = "master",
        authentication: SQLServerAuthentication,
        tlsEnabled: Bool = true,
        trustServerCertificate: Bool = false,
        caCertificatePath: String? = nil,
        encryptionMode: SQLServerEncryptionMode = .optional,
        numberOfThreads: Int = System.coreCount,
        poolConfiguration: SQLServerConnectionPool.Configuration = .init(),
        metadataConfiguration: SQLServerMetadataOperations.Configuration = .init(),
        retryConfiguration: SQLServerRetryConfiguration = .init(),
        transparentNetworkIPResolution: Bool = true,
        logger: Logger = Logger(label: "tds.sqlserver.client")
    ) async throws -> SQLServerClient {
        try await connect(
            configuration: .init(
                hostname: hostname,
                port: port,
                database: database,
                authentication: authentication,
                tlsEnabled: tlsEnabled,
                trustServerCertificate: trustServerCertificate,
                caCertificatePath: caCertificatePath,
                encryptionMode: encryptionMode,
                poolConfiguration: poolConfiguration,
                metadataConfiguration: metadataConfiguration,
                retryConfiguration: retryConfiguration,
                transparentNetworkIPResolution: transparentNetworkIPResolution
            ),
            numberOfThreads: numberOfThreads,
            logger: logger
        )
    }

    internal static func connect(
        configuration: Configuration,
        eventLoopGroupProvider: EventLoopGroupProvider = .createNew(numberOfThreads: System.coreCount),
        logger: Logger = Logger(label: "tds.sqlserver.client")
    ) -> EventLoopFuture<SQLServerClient> {
        @Sendable func scheduleRetry<T: Sendable>(
            on eventLoop: EventLoop,
            attempt: Int,
            error: Error,
            operation: @Sendable @escaping () -> EventLoopFuture<T>
        ) -> EventLoopFuture<T> {
            let normalized = SQLServerError.normalize(error)
            guard attempt < configuration.retryConfiguration.maximumAttempts,
                  configuration.retryConfiguration.shouldRetry(normalized) else {
                return eventLoop.makeFailedFuture(normalized)
            }

            logger.debug("Initial connection attempt \(attempt) failed; retrying with \(normalized)")
            let delay = configuration.retryConfiguration.backoffStrategy(attempt).nioTimeAmount
            return eventLoop.scheduleTask(in: delay) {}.futureResult.flatMap { operation() }
        }

        @Sendable func establishConnection(on eventLoop: EventLoop, attempt: Int = 1) -> EventLoopFuture<TDSConnection> {
            let targetDatabase = configuration.connection.login.database
            let escapedTargetDatabase = targetDatabase.replacingOccurrences(of: "]", with: "]]")

            func attemptConnect() -> EventLoopFuture<TDSConnection> {
                @Sendable
                func bootstrapSession(on connection: TDSConnection) -> EventLoopFuture<TDSConnection> {
                    let statements = configuration.connection.sessionOptions.buildStatements()
                    guard !statements.isEmpty else {
                        return eventLoop.makeSucceededFuture(connection)
                    }
                    let batch = statements.joined(separator: " ")
                    return connection.rawSql(batch).map { _ in connection }
                }

                @Sendable
                func connectForDatabase(_ database: String) -> EventLoopFuture<TDSConnection> {
                    SQLServerConnection.resolveSocketAddresses(
                        hostname: configuration.connection.hostname,
                        port: configuration.connection.port,
                        transparentResolution: configuration.connection.transparentNetworkIPResolution,
                        on: eventLoop
                    ).flatMap { addresses in
                        SQLServerConnection.establishTDSConnection(
                            addresses: addresses,
                            tlsConfiguration: configuration.connection.tlsConfiguration,
                            serverHostname: configuration.connection.hostname,
                            encryptionMode: configuration.connection.encryptionMode.asTDSMode,
                            connectTimeout: .seconds(Int64(configuration.connection.connectTimeoutSeconds)),
                            on: eventLoop,
                            logger: logger
                        )
                    }.flatMap { connection in
                        let cfg = TDSLoginConfiguration(
                            serverName: configuration.connection.hostname,
                            port: configuration.connection.port,
                            database: database,
                            authentication: configuration.connection.login.authentication.tdsAuthentication
                        )
                        return connection.login(configuration: cfg)
                            .flatMap { bootstrapSession(on: connection) }
                            .flatMapError { error in
                                connection.close().recover { _ in () }.flatMapThrowing {
                                    throw SQLServerError.normalize(error)
                                }
                            }
                    }
                }

                return connectForDatabase(configuration.connection.login.database).flatMapError { error in
                    let normalized = SQLServerError.normalize(error)
                    guard case .authenticationFailed = normalized,
                          targetDatabase.caseInsensitiveCompare("master") != .orderedSame
                    else {
                        return eventLoop.makeFailedFuture(normalized)
                    }
                    logger.warning("Direct login to \(targetDatabase) failed during connect; retrying via master and issuing USE")
                    return connectForDatabase("master").flatMap { connection in
                        connection.rawSql("USE [\(escapedTargetDatabase)];").map { _ in connection }
                    }
                }.flatMapError { error in
                    scheduleRetry(on: eventLoop, attempt: attempt, error: error) {
                        establishConnection(on: eventLoop, attempt: attempt + 1)
                    }
                }
            }

            return attemptConnect()
        }

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
            establishConnection(on: eventLoop)
        }

        let pool = SQLServerConnectionPool(
            configuration: configuration.poolConfiguration,
            eventLoopGroup: eventLoopGroup,
            logger: logger,
            connectionFactory: connectionFactory
        )

        let loop = eventLoopGroup.next()
        return connectionFactory(loop).flatMap { connection in
            connection.close().map {
                SQLServerClient(
                    configuration: configuration,
                    eventLoopGroup: eventLoopGroup,
                    ownsEventLoopGroup: ownsGroup,
                    pool: pool,
                    logger: logger
                )
            }
        }.flatMapError { error in
            pool.shutdownGracefully().flatMapThrowing { _ -> SQLServerClient in
                // Fire-and-forget ELG shutdown — returning a future from shutdownEventLoopGroup
                // would require NIO to hop back to this event loop which is being shut down.
                if ownsGroup {
                    eventLoopGroup.shutdownGracefully { _ in }
                }
                throw SQLServerError.normalize(error)
            }
        }
    }

    public func shutdownGracefully() async throws {
        var already = false
        stateLock.withLock {
            if _isShutdown { already = true } else { _isShutdown = true }
        }
        if already { return }

        while true {
            let pending = stateLock.withLock { inFlightOperations }
            if pending == 0 { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }

        try await pool.shutdownGracefully().get()

        if ownsEventLoopGroup {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                eventLoopGroup.shutdownGracefully { error in
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }
    }

    public func close() async throws {
        try await shutdownGracefully()
    }

    @available(macOS 12.0, *)
    public func connection() async throws -> SQLServerConnection {
        let pooled = try await pool.checkout().get()
        let connection = makeConnection(from: pooled)
        let loop = connection.eventLoop

        do {
            try await healthProbe(connection, on: loop).get()
            return connection
        } catch {
            _ = try? await connection.invalidate().get()
            throw SQLServerError.normalize(error)
        }
    }

    internal func shutdownGracefully() -> EventLoopFuture<Void> {
        let loop = eventLoopGroup.next()
        var already = false
        stateLock.withLock {
            if _isShutdown { already = true } else { _isShutdown = true }
        }
        if already { return loop.makeSucceededFuture(()) }
        let drained: EventLoopFuture<Void>
        if inFlightOperations == 0 {
            drained = loop.makeSucceededFuture(())
        } else {
            let p = loop.makePromise(of: Void.self)
            stateLock.withLock { drainWaiters.append(p) }
            drained = p.futureResult
        }
        return drained.flatMap { self.pool.shutdownGracefully() }.map { _ in
            // Fire-and-forget the ELG shutdown. We cannot return a future that depends on
            // the ELG shutdown completing, because NIO would need to hop the result back to
            // this event loop — which is the one being shut down — causing a race.
            if self.ownsEventLoopGroup {
                self.eventLoopGroup.shutdownGracefully { _ in }
            }
        }
    }

    public func withConnection<Result: Sendable>(
        on eventLoop: EventLoop? = nil,
        _ operation: @Sendable @escaping (SQLServerConnection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        if let scoped = ClientScopedConnection.current {
            return operation(scoped)
        }
        let loop = eventLoop ?? eventLoopGroup.next()
        let fut = executeWithRetry(operationName: "withConnection", on: loop) {
            self.pool.checkout(on: loop).flatMap { pooled -> EventLoopFuture<Result> in
                let sqlConnection = self.makeConnection(from: pooled)
                let probe = self.healthProbe(sqlConnection, on: loop).flatMapError { hpError in
                    let normalized = SQLServerError.normalize(hpError)
                    let retryError: SQLServerError = (hpError is SQLServerError) ? (hpError as! SQLServerError) : .connectionClosed
                    self.logger.debug("Connection health probe failed: \(normalized); invalidating connection")
                    return sqlConnection.invalidate().recover { _ in () }.flatMap { _ in
                        loop.makeFailedFuture(retryError)
                    }
                }
                let op: EventLoopFuture<Result> = probe.flatMap {
                    self.stateLock.withLock { self.inFlightOperations += 1 }
                    let userFuture = operation(sqlConnection)
                    let bridge = loop.makePromise(of: Result.self)
                    userFuture.whenComplete { result in
                        var toComplete: [EventLoopPromise<Void>] = []
                        self.stateLock.withLock {
                            self.inFlightOperations = max(0, self.inFlightOperations - 1)
                            if self.inFlightOperations == 0 && self._isShutdown {
                                toComplete = self.drainWaiters; self.drainWaiters.removeAll(keepingCapacity: false)
                            }
                        }
                        toComplete.forEach { $0.succeed(()) }
                        bridge.completeWith(result)
                    }
                    return bridge.futureResult
                }
                return op.flatMap { value in
                    sqlConnection.close().map { value }
                }.flatMapError { error in
                    let normalized = SQLServerError.normalize(error)
                    switch normalized {
                    case .sqlExecutionError, .deadlockDetected:
                        return sqlConnection.close().recover { _ in () }.flatMap { _ in
                            loop.makeFailedFuture(normalized)
                        }
                    default:
                        return sqlConnection.invalidate().recover { _ in () }.flatMap { _ in
                            loop.makeFailedFuture(normalized)
                        }
                    }
                }
            }
        }
        return fut.withTestTimeoutIfEnabled(on: loop)
    }

    internal init(
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

    internal func transferEventLoopGroupOwnership(_ group: EventLoopGroup) {
        assert(!ownsEventLoopGroup, "Client already owns an EventLoopGroup")
        assert(group === eventLoopGroup, "Transferred group must match the client's group")
        ownsEventLoopGroup = true
    }

    internal var isClientShutdown: Bool {
        stateLock.withLock { _isShutdown }
    }

    deinit {
        let shut = stateLock.withLock { _isShutdown }
        if !shut {
            logger.warning("SQLServerClient deinitialized without shutdownGracefully()")
        }
    }
}

// Task-local scoped connection used to force nested client operations to reuse the
// same SQLServerConnection (including its current database) within a logical block.
public enum ClientScopedConnection {
    @TaskLocal public static var current: SQLServerConnection?
}
