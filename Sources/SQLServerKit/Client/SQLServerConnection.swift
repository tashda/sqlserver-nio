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
        /// Overrides the hostname used to validate the server certificate's
        /// CN / subject alternative names. Mirrors SSMS's "Host Name In
        /// Certificate" / JDBC's `hostNameInCertificate`. When nil, the
        /// connection's `hostname` is used (the default).
        public var hostNameInCertificate: String?
        public var transparentNetworkIPResolution: Bool
        public var metadataConfiguration: SQLServerMetadataOperations.Configuration
        public var retryConfiguration: SQLServerRetryConfiguration
        public var sessionOptions: SessionOptions
        /// TCP connect timeout in seconds. Defaults to 10.
        public var connectTimeoutSeconds: Int
        /// When true, signals read-only application intent for AG secondary routing.
        public var readOnlyIntent: Bool
        /// Reported to SQL Server as APP_NAME() and program_name, which DBAs
        /// use in monitoring, auditing and Resource Governor classification.
        public var applicationName: String = "sqlserver-nio"

        public init(
            hostname: String,
            port: Int = 1433,
            login: Login,
            tlsConfiguration: SQLServerTLSConfiguration? = .clientDefault,
            encryptionMode: SQLServerEncryptionMode = .mandatory,
            hostNameInCertificate: String? = nil,
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
            self.hostNameInCertificate = hostNameInCertificate
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
    /// True once the physical connection is closed, whether by `close()`, the
    /// server, the network or a protocol failure. A closed connection cannot
    /// be used again; open a new one. Its session state (temporary tables,
    /// SET options, open transaction) is gone.
    public var isClosed: Bool { base.isClosed }

    public var lastSessionStatePayload: [UInt8] { base.snapshotSessionStatePayload() }
    public var lastDataClassificationPayload: [UInt8] { base.snapshotDataClassificationPayload() }

    /// Opens a dedicated connection on NIO's shared event loop group, so
    /// many dedicated connections do not each start their own thread.
    public static func connect(
        configuration: Configuration,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) async throws -> SQLServerConnection {
        try await connect(configuration: configuration, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, logger: logger)
    }

    /// Opens a dedicated connection on a caller-owned event loop group. The
    /// group is not shut down when the connection closes.
    public static func connect(
        configuration: Configuration,
        eventLoopGroup: EventLoopGroup,
        logger: Logger = Logger(label: "tds.sqlserver.connection")
    ) async throws -> SQLServerConnection {
        try await connect(
            configuration: configuration,
            eventLoopGroupProvider: .shared(eventLoopGroup),
            logger: logger
        ).get()
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
        eventLoopGroupProvider: SQLServerClient.EventLoopGroupProvider = .createNew(numberOfThreads: 1),
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
        openSession(configuration: configuration, on: eventLoop, logger: logger).map { connection in
            let sqlConnection = SQLServerConnection(
                base: connection,
                configuration: configuration,
                metadataCache: nil,
                logger: logger,
                reuseOnClose: false,
                releaseClosure: { _ in connection.close() }
            )
            sqlConnection.syncCurrentDatabaseFromServer()
            logger.info("Connected to \(configuration.hostname):\(configuration.port)/\(sqlConnection.currentDatabase)")
            return sqlConnection
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
            serverMajorVersion: base.serverMajorVersion,
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
        logger.debug(reuseOnClose ? "Connection returned to pool" : "Connection closed")
        return release(!reuseOnClose).map { _ in self.fireAndForgetGroupShutdown() }
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
        logger.debug(reuseOnClose ? "Connection returned to pool" : "Connection closed")

        // A pooled session is returned and reset before reuse; a dedicated
        // connection is closed.
        try await release(!reuseOnClose).get()

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

    /// Runs `body` in a transaction on this connection, committing when it
    /// returns and rolling back when it throws. The body runs once and is
    /// never retried.
    ///
    /// If SQL Server rejects the COMMIT, the transaction did not commit and
    /// that server error is thrown. If the connection fails while the COMMIT
    /// is in flight, `SQLServerError.commitOutcomeUnknown` is thrown: the
    /// transaction may or may not be durable, and the caller must check
    /// before repeating any writes.
    @available(macOS 12.0, *)
    public func withTransaction<T>(body: @escaping (SQLServerConnection) async throws -> T) async throws -> T {
        try await beginTransaction()
        let result: T
        do {
            result = try await body(self)
        } catch {
            _ = try? await rollback().get()
            throw error
        }
        do {
            try await commit().get()
            return result
        } catch let error as SQLServerError where error.serverDetails != nil && !error.isConnectionLost {
            // The server answered: the transaction was not committed.
            _ = try? await rollback().get()
            throw error
        } catch {
            throw SQLServerError.commitOutcomeUnknown(error)
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
            _ = release(true)
        }
    }
}
