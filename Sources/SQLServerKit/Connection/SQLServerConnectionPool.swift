import NIO
import NIOEmbedded
import NIOCore
import NIOConcurrencyHelpers
import Logging
import SQLServerTDS

public final class SQLServerConnectionPool {
    public struct Configuration {
        public var maximumConcurrentConnections: Int
        public var minimumIdleConnections: Int
        public var connectionIdleTimeout: TimeAmount?
        public var validationQuery: String?

        public init(
            maximumConcurrentConnections: Int = 8,
            minimumIdleConnections: Int = 0,
            connectionIdleTimeout: TimeAmount? = nil,
            validationQuery: String? = nil
        ) {
            precondition(maximumConcurrentConnections > 0, "maximumConcurrentConnections must be positive")
            precondition(minimumIdleConnections >= 0, "minimumIdleConnections must be non-negative")
            precondition(minimumIdleConnections <= maximumConcurrentConnections, "minimumIdleConnections cannot exceed maximumConcurrentConnections")
            self.maximumConcurrentConnections = maximumConcurrentConnections
            self.minimumIdleConnections = minimumIdleConnections
            self.connectionIdleTimeout = connectionIdleTimeout
            self.validationQuery = validationQuery
        }
    }

    public enum Error: Swift.Error {
        case poolClosed
        case shutdown
    }

    private struct PoolRequest {
        let promise: EventLoopPromise<TDSConnection>
        let eventLoop: EventLoop
    }

    private struct IdleConnection {
        let connection: TDSConnection
        var idleTask: Scheduled<Void>?
    }

    public final class PooledConnection {
        fileprivate let connection: TDSConnection
        fileprivate unowned let pool: SQLServerConnectionPool
        private let releaseLock = NIOLock()
        private var released = false

        fileprivate init(connection: TDSConnection, pool: SQLServerConnectionPool) {
            self.connection = connection
            self.pool = pool
        }

        public var base: TDSConnection {
            connection
        }

        @discardableResult
        public func release(close: Bool = false) -> EventLoopFuture<Void> {
            let alreadyReleased = releaseLock.withLock { () -> Bool in
                if released {
                    return true
                }
                released = true
                return false
            }

            if alreadyReleased {
                return connection.eventLoop.makeSucceededFuture(())
            }
            return pool.release(connection, close: close)
        }

        deinit {
            let shouldRelease = releaseLock.withLock { () -> Bool in
                if released {
                    return false
                }
                released = true
                return true
            }

            if shouldRelease {
                _ = release(close: connection.isClosed)
            }
        }
    }

    private enum Action {
        case succeed(request: PoolRequest, connection: TDSConnection)
        case create(request: PoolRequest)
        case closeAndMaybeCreate(connection: TDSConnection, next: PoolRequest?)
        case close(connection: TDSConnection)
        case fail(request: PoolRequest, error: Swift.Error)
        case none
    }

    private let configuration: Configuration
    private let eventLoopGroup: EventLoopGroup
    private let connectionFactory: (EventLoop) -> EventLoopFuture<TDSConnection>
    private let lock = NIOLock()
    private var idle: [IdleConnection] = []
    private var waiters = CircularBuffer<PoolRequest>()
    private var activeConnections = 0
    private var isShuttingDown = false
    private let logger: Logger

    public init(
        configuration: Configuration,
        eventLoopGroup: EventLoopGroup,
        logger: Logger = Logger(label: "tds.sqlserver.pool"),
        connectionFactory: @escaping (EventLoop) -> EventLoopFuture<TDSConnection>
    ) {
        self.configuration = configuration
        self.eventLoopGroup = eventLoopGroup
        self.logger = logger
        self.connectionFactory = connectionFactory
        // Do not prefill on init. Allow lazy creation or explicit start() to prefill,
        // mirroring SSMS/JDBC behavior where connections are created on demand.
    }

    public func checkout(on eventLoop: EventLoop? = nil) -> EventLoopFuture<PooledConnection> {
        let targetLoop = eventLoop ?? eventLoopGroup.next()
        let promise = targetLoop.makePromise(of: TDSConnection.self)
        let request = PoolRequest(promise: promise, eventLoop: targetLoop)
        process(request: request)
        return promise.futureResult.map { connection in
            PooledConnection(connection: connection, pool: self)
        }
    }

    @discardableResult
    public func withConnection<Result>(
        on eventLoop: EventLoop? = nil,
        _ closure: @escaping (TDSConnection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        return checkout(on: eventLoop).flatMap { pooled in
            let connection = pooled.base
            return closure(connection).flatMap { value in
                pooled.release().map { value }
            }.flatMapError { error in
                pooled.release(close: true).flatMapThrowing { throw error }
            }
        }
    }

    public func shutdownGracefully() -> EventLoopFuture<Void> {
        var connectionsToClose: [TDSConnection] = []
        var waiting: [PoolRequest] = []
        var alreadyShuttingDown = false

        lock.withLock {
            if isShuttingDown {
                alreadyShuttingDown = true
                return
            }
            isShuttingDown = true
            connectionsToClose = idle.map { $0.connection }
            idle.forEach { $0.idleTask?.cancel() }
            idle.removeAll(keepingCapacity: true)
            waiting = Array(waiters)
            waiters.removeAll(keepingCapacity: true)
        }

        if alreadyShuttingDown {
            // Avoid scheduling onto a possibly closing event loop; return a pre-completed future
            // backed by an active loop if available, otherwise use the event loop of an active
            // connection or an EmbeddedEventLoop fallback.
            return makeImmediateSucceededFuture(on: eventLoopGroup)
        }

        waiting.forEach { request in
            // Complete the promise - promises can be completed from any thread
            request.promise.fail(Error.shutdown)
        }

        if connectionsToClose.isEmpty {
            return makeImmediateSucceededFuture(on: eventLoopGroup)
        }

        let futures = connectionsToClose.map { $0.close() }
        // Avoid selecting a new loop from a group that is shutting down; chain completion
        // onto the event loop of the first connection being closed, which is guaranteed
        // to remain valid for the duration of the close.
        if let first = connectionsToClose.first {
            return EventLoopFuture.andAllSucceed(futures, on: first.eventLoop)
        } else {
            return makeImmediateSucceededFuture(on: eventLoopGroup)
        }
    }

    private func makeImmediateSucceededFuture(on group: EventLoopGroup) -> EventLoopFuture<Void> {
        // Always use a real event loop from the group to avoid EmbeddedEventLoop thread safety issues
        let loop = group.next()
        return loop.makeSucceededFuture(())
    }

    private func process(request: PoolRequest) {
        let action: Action = lock.withLock {
            if isShuttingDown {
                return .fail(request: request, error: Error.poolClosed)
            }

            if !idle.isEmpty {
                let entry = idle.removeLast()
                entry.idleTask?.cancel()
                return .succeed(request: request, connection: entry.connection)
            }

            if activeConnections < configuration.maximumConcurrentConnections {
                activeConnections += 1
                return .create(request: request)
            }

            waiters.append(request)
            return .none
        }

        run(action)
    }

    fileprivate func release(_ connection: TDSConnection, close: Bool) -> EventLoopFuture<Void> {
        var shouldEnsure = false

        let action: Action = lock.withLock {
            if isShuttingDown {
                activeConnections = max(0, activeConnections - 1)
                return .close(connection: connection)
            }

            if close || connection.isClosed {
                activeConnections = max(0, activeConnections - 1)
                let next = waiters.popFirst()
                if next != nil {
                    activeConnections += 1
                }
                shouldEnsure = true
                return .closeAndMaybeCreate(connection: connection, next: next)
            }

            if let waiter = waiters.popFirst() {
                return .succeed(request: waiter, connection: connection)
            }

            let task = scheduleIdleClose(for: connection)
            idle.append(IdleConnection(connection: connection, idleTask: task))
            shouldEnsure = true
            return .none
        }

        run(action)
        if shouldEnsure {
            ensureMinimumIdleConnections()
        }
        return connection.eventLoop.makeSucceededFuture(())
    }

    private func run(_ action: Action) {
        switch action {
        case .succeed(let request, let connection):
            deliver(connection: connection, to: request)
        case .create(let request):
            createConnection(for: request)
        case .closeAndMaybeCreate(let connection, let next):
            _ = connection.close()
            if let request = next {
                createConnection(for: request)
            }
        case .close(let connection):
            _ = connection.close()
        case .fail(let request, let error):
            request.promise.fail(error)
        case .none:
            break
        }
    }

    private func scheduleIdleClose(for connection: TDSConnection) -> Scheduled<Void>? {
        guard let timeout = configuration.connectionIdleTimeout else {
            return nil
        }
        return connection.eventLoop.scheduleTask(deadline: .now() + timeout) { [weak self, weak connection] in
            guard let self = self, let connection = connection else { return }
            self.expireIdleConnection(connection)
        }
    }

    private func expireIdleConnection(_ connection: TDSConnection) {
        var shouldClose = false
        self.lock.withLock {
            if let index = self.idle.firstIndex(where: { $0.connection === connection }) {
                let entry = self.idle.remove(at: index)
                entry.idleTask?.cancel()
                self.activeConnections = max(0, self.activeConnections - 1)
                shouldClose = true
            }
        }
        if shouldClose {
            _ = connection.close()
            ensureMinimumIdleConnections()
        }
    }

    private func ensureMinimumIdleConnections() {
        guard configuration.minimumIdleConnections > 0 else { return }

        var toCreate = 0
        self.lock.withLock {
            if self.isShuttingDown {
                return
            }
            let idleCount = self.idle.count
            if idleCount >= self.configuration.minimumIdleConnections {
                return
            }
            let availableSlots = self.configuration.maximumConcurrentConnections - self.activeConnections
            if availableSlots <= 0 {
                return
            }
            toCreate = min(self.configuration.minimumIdleConnections - idleCount, availableSlots)
            self.activeConnections += toCreate
        }

        guard toCreate > 0 else { return }

        for _ in 0..<toCreate {
            createIdleConnection()
        }
    }

    private func createIdleConnection() {
        let loop = eventLoopGroup.next()
        connectionFactory(loop).whenComplete { result in
            switch result {
            case .success(let connection):
                var waiter: PoolRequest?
                var shouldClose = false
                self.lock.withLock {
                    if self.isShuttingDown {
                        self.activeConnections = max(0, self.activeConnections - 1)
                        shouldClose = true
                    } else if let request = self.waiters.popFirst() {
                        waiter = request
                    } else {
                        let task = self.scheduleIdleClose(for: connection)
                        self.idle.append(IdleConnection(connection: connection, idleTask: task))
                    }
                }

                if let waiter = waiter {
                    self.deliver(connection: connection, to: waiter)
                } else if shouldClose {
                    _ = connection.close()
                }

            case .failure(let error):
                self.logger.error("Connection pool warm-up failed: \(error)")
                self.lock.withLock {
                    self.activeConnections = max(0, self.activeConnections - 1)
                }
                self.ensureMinimumIdleConnections()
            }
        }
    }

    private func createConnection(for request: PoolRequest) {
        let future = connectionFactory(request.eventLoop)
        future.whenComplete { result in
            switch result {
            case .success(let connection):
                self.deliver(connection: connection, to: request)
            case .failure(let error):
                self.logger.error("Connection pool failed to create connection: \(error)")
                self.handleCreationFailure(request: request, error: error)
            }
        }
    }

    private func handleCreationFailure(request: PoolRequest, error: Swift.Error) {
        var next: PoolRequest?
        self.lock.withLock {
            self.activeConnections = max(0, self.activeConnections - 1)
            next = self.waiters.popFirst()
            if next != nil {
                self.activeConnections += 1
            }
        }

        request.eventLoop.execute {
            request.promise.fail(error)
        }

        if let nextRequest = next {
            createConnection(for: nextRequest)
        }
        ensureMinimumIdleConnections()
    }

    private func deliver(connection: TDSConnection, to request: PoolRequest) {
        if let validationQuery = configuration.validationQuery {
            connection.rawSql(validationQuery).whenComplete { result in
                switch result {
                case .success:
                    request.promise.succeed(connection)
                case .failure(let error):
                    self.logger.warning("Validation query failed: \(error)")
                    _ = connection.close()
                    self.lock.withLock {
                        self.activeConnections = max(0, self.activeConnections - 1)
                    }
                    self.process(request: request)
                    self.ensureMinimumIdleConnections()
                }
            }
        } else {
            request.promise.succeed(connection)
        }
    }

    public func start() {
        ensureMinimumIdleConnections()
    }
}
