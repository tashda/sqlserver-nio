import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOPosix
import NIOSSL

public final class SQLServerConnection {
    public struct Configuration {
        public struct Login {
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

        public init(
            hostname: String,
            port: Int = 1433,
            login: Login,
            tlsConfiguration: TLSConfiguration? = .makeClientConfiguration(),
            metadataConfiguration: SQLServerMetadataClient.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init()
        ) {
            self.hostname = hostname
            self.port = port
            self.login = login
            self.tlsConfiguration = tlsConfiguration
            self.metadataConfiguration = metadataConfiguration
            self.retryConfiguration = retryConfiguration
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
        }.map { connection in
            SQLServerConnection(
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
        }
    }

    public var eventLoop: EventLoop {
        base.eventLoop
    }

    public var logger: Logger {
        base.logger
    }

    public func close() -> EventLoopFuture<Void> {
        let future = release(close: !reuseOnClose)
        guard let group = ownsEventLoopGroup else {
            return future
        }
        return future.flatMap {
            SQLServerClient.shutdownEventLoopGroup(group)
        }.map {
            self.ownsEventLoopGroup = nil
        }
    }

    public func query(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        executeWithRetry(operationName: "query") {
            self.base.rawSql(sql)
        }
    }

    @available(macOS 12.0, *)
    public func query(_ sql: String) async throws -> [TDSRow] {
        try await query(sql).get()
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
        self.retryConfiguration = configuration.retryConfiguration
        self.metadataClient = SQLServerMetadataClient(
            connection: base,
            configuration: configuration.metadataConfiguration,
            sharedCache: metadataCache
        )
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
    private let retryConfiguration: SQLServerRetryConfiguration
    private let metadataClient: SQLServerMetadataClient
    private let releaseClosure: @Sendable (_ close: Bool) -> EventLoopFuture<Void>
    private let releaseLock = NIOLock()
    private var didRelease = false
    private var ownsEventLoopGroup: EventLoopGroup?
    private let reuseOnClose: Bool

    internal var underlying: TDSConnection {
        base
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
