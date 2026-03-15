import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

public final class SQLServerConnection: @unchecked Sendable {
    public struct Configuration: Sendable {
        public struct Login: Sendable {
            public var database: String
            public var authentication: SQLServerAuthentication

            public init(database: String, authentication: SQLServerAuthentication) {
                self.database = database
                self.authentication = authentication
            }
        }

        public var hostname: String
        public var port: Int
        public var login: Login
        public var tlsConfiguration: SQLServerTLSConfiguration?
        public var encryptionMode: SQLServerEncryptionMode
        public var transparentNetworkIPResolution: Bool
        public var metadataConfiguration: SQLServerMetadataOperations.Configuration
        public var retryConfiguration: SQLServerRetryConfiguration
        public var sessionOptions: SessionOptions
        /// TCP connect timeout in seconds. Defaults to 10.
        public var connectTimeoutSeconds: Int
        /// When true, signals read-only application intent for AG secondary routing.
        public var readOnlyIntent: Bool

        public init(
            hostname: String,
            port: Int = 1433,
            login: Login,
            tlsConfiguration: SQLServerTLSConfiguration? = .makeClientConfiguration(),
            encryptionMode: SQLServerEncryptionMode = .optional,
            metadataConfiguration: SQLServerMetadataOperations.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init(),
            sessionOptions: SessionOptions = .ssmsDefaults,
            transparentNetworkIPResolution: Bool = true,
            connectTimeoutSeconds: Int = 10,
            readOnlyIntent: Bool = false
        ) {
            self.hostname = hostname
            self.port = port
            self.login = login
            self.tlsConfiguration = tlsConfiguration
            self.encryptionMode = encryptionMode
            self.metadataConfiguration = metadataConfiguration
            self.retryConfiguration = retryConfiguration
            self.sessionOptions = sessionOptions
            self.transparentNetworkIPResolution = transparentNetworkIPResolution
            self.connectTimeoutSeconds = connectTimeoutSeconds
            self.readOnlyIntent = readOnlyIntent
        }
    }

    internal let base: TDSConnection
    public let configuration: Configuration
    internal var metadataClient: SQLServerMetadataOperations!
    internal let reuseOnClose: Bool
    internal let release: (Bool) -> EventLoopFuture<Void>
    internal var ownsEventLoopGroup: EventLoopGroup?

    internal let stateLock = NIOLock()
    internal var _currentDatabase: String
    internal var _isSessionPrimed = false
    internal var _isClosed = false

    internal var underlying: TDSConnection { base }
    internal var eventLoop: EventLoop { base.eventLoop }
    public var logger: Logger { base.logger }
    public var currentDatabase: String { stateLock.withLock { _currentDatabase } }

    public var lastSessionStatePayload: [UInt8] { base.snapshotSessionStatePayload() }
    public var lastDataClassificationPayload: [UInt8] { base.snapshotDataClassificationPayload() }

    public static func connect(
        configuration: Configuration,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) async throws -> SQLServerConnection {
        try await connect(
            configuration: configuration,
            numberOfThreads: System.coreCount,
            logger: logger
        )
    }

    public static func connect(
        configuration: Configuration,
        numberOfThreads: Int,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) async throws -> SQLServerConnection {
        try await connect(
            configuration: configuration,
            eventLoopGroupProvider: .createNew(numberOfThreads: numberOfThreads),
            logger: logger
        ).get()
    }

    internal static func connect(
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
                    group.shutdownGracefully { _ in }
                }
                return loop.makeFailedFuture(error)
            }

        return fut
    }

    internal static func connect(
        configuration: Configuration,
        on eventLoop: EventLoop,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) -> EventLoopFuture<SQLServerConnection> {
        @Sendable
        func escapeIdentifier(_ identifier: String) -> String {
            identifier.replacingOccurrences(of: "]", with: "]]")
        }

        @Sendable
        func attempt(_ cfg: Configuration) -> EventLoopFuture<SQLServerConnection> {
            let loginConfiguration = TDSLoginConfiguration(
                serverName: cfg.hostname,
                port: cfg.port,
                database: cfg.login.database,
                authentication: cfg.login.authentication.tdsAuthentication,
                readOnlyIntent: cfg.readOnlyIntent
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
                    encryptionMode: cfg.encryptionMode.asTDSMode,
                    connectTimeout: .seconds(Int64(cfg.connectTimeoutSeconds)),
                    on: eventLoop,
                    logger: logger
                )
            }.flatMap { connection in
                connection.login(configuration: loginConfiguration)
                    .map { connection }
                    .flatMapError { error in
                        let normalized = SQLServerError.normalize(error)
                        guard case .authenticationFailed = normalized,
                              cfg.login.database.caseInsensitiveCompare("master") != .orderedSame
                        else {
                            return connection.close().flatMapThrowing { throw normalized }
                        }

                        logger.warning("Login to database \(cfg.login.database) failed; retrying via master and issuing USE")
                        let masterLogin = TDSLoginConfiguration(
                            serverName: cfg.hostname,
                            port: cfg.port,
                            database: "master",
                            authentication: cfg.login.authentication.tdsAuthentication,
                            readOnlyIntent: cfg.readOnlyIntent
                        )
                        return connection.login(configuration: masterLogin)
                            .flatMap {
                                connection.rawSql("USE [\(escapeIdentifier(cfg.login.database))];")
                            }
                            .map { _ in connection }
                            .flatMapError { fallbackError in
                                connection.close().flatMapThrowing {
                                    throw SQLServerError.normalize(fallbackError)
                                }
                            }
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

        return attempt(configuration).flatMapError { error in
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

    internal init(
        base: TDSConnection,
        configuration: Configuration,
        metadataCache: MetadataCache<[ColumnMetadata]>?,
        logger: Logger,
        reuseOnClose: Bool,
        releaseClosure: @escaping (Bool) -> EventLoopFuture<Void>
    ) {
        self.base = base
        self.configuration = configuration
        self.reuseOnClose = reuseOnClose
        self.release = releaseClosure
        self._currentDatabase = configuration.login.database
        self.metadataClient = SQLServerMetadataOperations(
            eventLoop: base.eventLoop,
            configuration: configuration.metadataConfiguration,
            sharedCache: metadataCache,
            defaultDatabase: configuration.login.database,
            logger: logger,
            queryExecutor: { [weak self, eventLoop = base.eventLoop] sql in
                guard let self else {
                    return eventLoop.makeFailedFuture(SQLServerError.connectionClosed)
                }
                let timeout = self.configuration.metadataConfiguration.commandTimeout
                if let timeout {
                    return self.execute(sql, timeout: timeout, invalidateOnTimeout: false).map(\.rawRows)
                }
                return self.execute(sql).map(\.rawRows)
            }
        )
    }

    internal func close() -> EventLoopFuture<Void> {
        let shouldClose = stateLock.withLock { () -> Bool in
            if _isClosed {
                return false
            }
            _isClosed = true
            return true
        }
        guard shouldClose else {
            return eventLoop.makeSucceededFuture(())
        }
        if reuseOnClose {
            let defaultDatabase = configuration.login.database
            let currentDatabase = self.currentDatabase
            let releaseFuture: EventLoopFuture<Void>
            if !base.isClosed,
               currentDatabase.caseInsensitiveCompare(defaultDatabase) != .orderedSame {
                releaseFuture = changeDatabase(defaultDatabase).flatMap {
                    self.release(false)
                }.flatMapError { _ in
                    self.release(true)
                }
            } else {
                releaseFuture = self.release(false)
            }
            return releaseFuture.map { _ in self.fireAndForgetGroupShutdown() }
        } else {
            return release(true).map { _ in self.fireAndForgetGroupShutdown() }
        }
    }

    public func close() async throws {
        let shouldClose = stateLock.withLock { () -> Bool in
            if _isClosed {
                return false
            }
            _isClosed = true
            return true
        }
        guard shouldClose else { return }

        if reuseOnClose {
            let defaultDatabase = configuration.login.database
            let currentDatabase = self.currentDatabase
            if !base.isClosed,
               currentDatabase.caseInsensitiveCompare(defaultDatabase) != .orderedSame {
                do {
                    let switchFuture: EventLoopFuture<Void> = self.changeDatabase(defaultDatabase)
                    try await switchFuture.get()
                    try await release(false).get()
                } catch {
                    try await release(true).get()
                }
            } else {
                try await release(false).get()
            }
        } else {
            try await release(true).get()
        }

        if let group = ownsEventLoopGroup {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                group.shutdownGracefully { error in
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }
    }

    @available(macOS 12.0, *)
    public func withTransaction<T>(body: @escaping (SQLServerConnection) async throws -> T) async throws -> T {
        do {
            try await beginTransaction()
            let result = try await body(self)
            try await commit()
            return result
        } catch {
            _ = try? await rollback()
            throw error
        }
    }

    public func cancelActiveRequest() {
        base.sendAttention()
    }

    deinit {
        let shouldClose = stateLock.withLock {
            if _isClosed {
                return false
            }
            _isClosed = true
            return true
        }
        if shouldClose {
            _ = release(!reuseOnClose)
        }
    }
}
