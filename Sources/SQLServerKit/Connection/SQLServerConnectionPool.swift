import NIO
import NIOEmbedded
import NIOCore
import NIOConcurrencyHelpers
import Foundation
import Logging
import SQLServerTDS

public final class SQLServerConnectionPool: @unchecked Sendable {
    public struct Configuration: Sendable {
        public var maximumConcurrentConnections: Int
        public var minimumIdleConnections: Int
        public var connectionIdleTimeout: TimeInterval?
        public var checkoutTimeout: TimeInterval
        public var validationQuery: String?

        public init(
            maximumConcurrentConnections: Int = 8,
            minimumIdleConnections: Int = 0,
            connectionIdleTimeout: TimeInterval? = nil,
            checkoutTimeout: TimeInterval = 30,
            validationQuery: String? = nil
        ) {
            precondition(maximumConcurrentConnections > 0, "maximumConcurrentConnections must be positive")
            precondition(minimumIdleConnections >= 0, "minimumIdleConnections must be non-negative")
            precondition(minimumIdleConnections <= maximumConcurrentConnections, "minimumIdleConnections cannot exceed maximumConcurrentConnections")
            precondition(checkoutTimeout.isFinite && checkoutTimeout > 0, "checkoutTimeout must be finite and positive")
            if let connectionIdleTimeout {
                precondition(connectionIdleTimeout.isFinite && connectionIdleTimeout > 0, "connectionIdleTimeout must be finite and positive")
            }
            self.maximumConcurrentConnections = maximumConcurrentConnections
            self.minimumIdleConnections = minimumIdleConnections
            self.connectionIdleTimeout = connectionIdleTimeout
            self.checkoutTimeout = checkoutTimeout
            self.validationQuery = validationQuery
        }
    }

    public enum Error: Swift.Error {
        case poolClosed
        case shutdown
    }

    private final class PoolRequest: @unchecked Sendable {
        let promise: EventLoopPromise<TDSConnection>
        let eventLoop: EventLoop
        let id: UInt64
        private let stateLock = NIOLock()
        private var completed = false
        private var attachedConnection: TDSConnection?

        init(promise: EventLoopPromise<TDSConnection>, eventLoop: EventLoop, id: UInt64) {
            self.promise = promise
            self.eventLoop = eventLoop
            self.id = id
        }

        var isPending: Bool { stateLock.withLock { !completed } }

        func attach(_ connection: TDSConnection) -> Bool {
            stateLock.withLock {
                guard !completed else { return false }
                attachedConnection = connection
                return true
            }
        }

        @discardableResult
        func succeed(_ connection: TDSConnection) -> Bool {
            let won = stateLock.withLock { () -> Bool in
                guard !completed else { return false }
                completed = true
                attachedConnection = nil
                return true
            }
            if won { promise.succeed(connection) }
            return won
        }

        func fail(_ error: Swift.Error) -> (won: Bool, attached: TDSConnection?) {
            let result = stateLock.withLock { () -> (Bool, TDSConnection?) in
                guard !completed else { return (false, nil) }
                completed = true
                let connection = attachedConnection
                attachedConnection = nil
                return (true, connection)
            }
            if result.0 { promise.fail(error) }
            return result
        }
    }

    private let requestIDCounter = NIOLockedValueBox<UInt64>(0)

    private struct IdleConnection {
        let connection: TDSConnection
        var idleTask: Scheduled<Void>?
    }

    public final class PooledConnection: @unchecked Sendable {
        fileprivate let connection: TDSConnection
        fileprivate let pool: SQLServerConnectionPool
        private let releaseLock = NIOLock()
        private var released = false

        fileprivate init(connection: TDSConnection, pool: SQLServerConnectionPool) {
            self.connection = connection
            self.pool = pool
        }

        internal var base: TDSConnection {
            connection
        }

        @discardableResult
        internal func release(close: Bool = false) -> EventLoopFuture<Void> {
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
                _ = pool.release(connection, close: true)
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
    private var leased: [Swift.ObjectIdentifier: TDSConnection] = [:]
    private var waiters = CircularBuffer<PoolRequest>()
    private var pendingRequests: [UInt64: PoolRequest] = [:]
    private var activeConnections = 0
    private var isShuttingDown = false
    private var warmupFailures = 0
    private var warmupRetryTask: Scheduled<Void>?
    private var shutdownPromise: EventLoopPromise<Void>?
    private var shutdownClosesInProgress = 0
    private let logger: Logger

    internal init(
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

    internal func checkout(on eventLoop: EventLoop? = nil) -> EventLoopFuture<PooledConnection> {
        let targetLoop = eventLoop ?? eventLoopGroup.next()
        let promise = targetLoop.makePromise(of: TDSConnection.self)
        let requestID = requestIDCounter.withLockedValue { id -> UInt64 in
            id += 1
            return id
        }
        let request = PoolRequest(promise: promise, eventLoop: targetLoop, id: requestID)
        lock.withLock { pendingRequests[requestID] = request }
        process(request: request)

        let timeout = configuration.checkoutTimeout
        let timeoutNanos = Int64(timeout * 1_000_000_000)
        let timeoutTask = targetLoop.scheduleTask(deadline: .now() + .nanoseconds(timeoutNanos)) { [weak self] in
            let failure = request.fail(SQLServerError.timeout(
                description: "connection pool checkout timed out after \(timeout)s (pool may be exhausted or a connection is stuck)",
                underlying: nil
            ))
            guard failure.won else { return }
            // Remove the timed-out waiter from the queue
            let waiterCount: Int = self?.lock.withLock {
                if let index = self?.waiters.firstIndex(where: { $0.id == requestID }) {
                    self?.waiters.remove(at: index)
                }
                return self?.waiters.count ?? 0
            } ?? 0
            self?.logger.warning("Connection pool checkout timed out after \(timeout)s, waiters=\(waiterCount)")
            if let attached = failure.attached {
                _ = self?.release(attached, close: true)
            }
        }

        return promise.futureResult.always { _ in
            timeoutTask.cancel()
            _ = self.lock.withLock { self.pendingRequests.removeValue(forKey: requestID) }
        }.map { connection in
            PooledConnection(connection: connection, pool: self)
        }
    }

    @discardableResult
    internal func withConnection<Result: Sendable>(
        on eventLoop: EventLoop? = nil,
        _ closure: @Sendable @escaping (TDSConnection) -> EventLoopFuture<Result>
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

    internal func shutdownGracefully() -> EventLoopFuture<Void> {
        var connectionsToClose: [TDSConnection] = []
        var pending: [PoolRequest] = []
        let promise = eventLoopGroup.next().makePromise(of: Void.self)
        var existing: EventLoopFuture<Void>?

        lock.withLock {
            if let shutdownPromise {
                existing = shutdownPromise.futureResult
                return
            }
            shutdownPromise = promise
            isShuttingDown = true
            warmupRetryTask?.cancel()
            warmupRetryTask = nil
            connectionsToClose = idle.map { $0.connection }
            idle.forEach { $0.idleTask?.cancel() }
            shutdownClosesInProgress += idle.count
            activeConnections = max(0, activeConnections - idle.count)
            idle.removeAll(keepingCapacity: true)
            waiters.removeAll(keepingCapacity: true)
            pending = Array(pendingRequests.values)
            pendingRequests.removeAll(keepingCapacity: true)
        }

        if let existing { return existing }

        pending.forEach { request in
            let failure = request.fail(Error.shutdown)
            if let attached = failure.attached { _ = release(attached, close: true) }
        }

        connectionsToClose.forEach { connection in
            _ = closeTracked(connection, alreadyCounted: true)
        }
        finishShutdownIfDrained()
        return promise.futureResult
    }

    private func closeTracked(_ connection: TDSConnection, alreadyCounted: Bool = false) -> EventLoopFuture<Void> {
        if !alreadyCounted { lock.withLock { shutdownClosesInProgress += 1 } }
        return connection.close().always { _ in
            self.lock.withLock { self.shutdownClosesInProgress -= 1 }
            self.finishShutdownIfDrained()
        }
    }

    private func finishShutdownIfDrained() {
        let complete: EventLoopPromise<Void>? = lock.withLock {
            guard isShuttingDown, activeConnections == 0,
                  shutdownClosesInProgress == 0 else { return nil }
            let promise = shutdownPromise
            shutdownPromise = nil
            return promise
        }
        complete?.succeed(())
    }

    internal func statusSnapshot() -> SQLServerConnectionPoolStatus {
        lock.withLock {
            SQLServerConnectionPoolStatus(
                active: activeConnections,
                idle: idle.count,
                waiting: waiters.count,
                isShuttingDown: isShuttingDown
            )
        }
    }

    private func process(request: PoolRequest) {
        guard request.isPending else { return }
        let action: Action = lock.withLock {
            if isShuttingDown {
                return .fail(request: request, error: Error.poolClosed)
            }

            if !idle.isEmpty {
                let entry = idle.removeLast()
                entry.idleTask?.cancel()
                leased[Swift.ObjectIdentifier(entry.connection)] = entry.connection
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

    fileprivate func release(_ connection: TDSConnection, close _: Bool) -> EventLoopFuture<Void> {
        var shouldEnsure = false

        let action: Action = lock.withLock {
            guard leased.removeValue(forKey: Swift.ObjectIdentifier(connection)) != nil else {
                return .none
            }
            if isShuttingDown {
                activeConnections = max(0, activeConnections - 1)
                shutdownClosesInProgress += 1
                return .close(connection: connection)
            }

            // A returned SQL Server session can retain SET options, SESSION_CONTEXT,
            // temp objects, security context, and transaction state. Until the
            // RESETCONNECTION path is verified, only a fresh physical session is
            // safe to lease to a different operation.
            activeConnections = max(0, activeConnections - 1)
            shutdownClosesInProgress += 1
            let next = waiters.popFirst()
            if next != nil { activeConnections += 1 }
            shouldEnsure = true
            return .closeAndMaybeCreate(connection: connection, next: next)
        }

        switch action {
        case .closeAndMaybeCreate(let connection, let next):
            let closed = closeTracked(connection, alreadyCounted: true)
            let ensure = shouldEnsure
            _ = closed.always { _ in
                if let next { self.createConnection(for: next) }
                if ensure { self.ensureMinimumIdleConnections() }
            }
            return closed
        case .close(let connection):
            return closeTracked(connection, alreadyCounted: true)
        default:
            run(action)
            if shouldEnsure { ensureMinimumIdleConnections() }
            return connection.eventLoop.makeSucceededFuture(())
        }
    }

    private func run(_ action: Action) {
        switch action {
        case .succeed(let request, let connection):
            deliver(connection: connection, to: request)
        case .create(let request):
            createConnection(for: request)
        case .closeAndMaybeCreate(let connection, let next):
            _ = closeTracked(connection, alreadyCounted: true)
            if let request = next {
                createConnection(for: request)
            }
        case .close(let connection):
            _ = closeTracked(connection, alreadyCounted: true)
        case .fail(let request, let error):
            _ = request.fail(error)
        case .none:
            break
        }
    }

    private func scheduleIdleClose(for connection: TDSConnection) -> Scheduled<Void>? {
        guard let timeout = configuration.connectionIdleTimeout else {
            return nil
        }
        return connection.eventLoop.scheduleTask(in: timeout.nioTimeAmount) { [weak self, weak connection] in
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
                self.shutdownClosesInProgress += 1
                shouldClose = true
            }
        }
        if shouldClose {
            _ = closeTracked(connection, alreadyCounted: true)
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
                    self.warmupFailures = 0
                    if self.isShuttingDown {
                        self.activeConnections = max(0, self.activeConnections - 1)
                        self.shutdownClosesInProgress += 1
                        shouldClose = true
                    } else if let request = self.waiters.popFirst() {
                        waiter = request
                        self.leased[Swift.ObjectIdentifier(connection)] = connection
                    } else {
                        let task = self.scheduleIdleClose(for: connection)
                        self.idle.append(IdleConnection(connection: connection, idleTask: task))
                    }
                }

                if let waiter = waiter {
                    self.deliver(connection: connection, to: waiter)
                } else if shouldClose {
                    _ = self.closeTracked(connection, alreadyCounted: true)
                }

            case .failure(let error):
                self.logger.error("Connection pool warm-up failed: \(error)")
                self.lock.withLock {
                    self.activeConnections = max(0, self.activeConnections - 1)
                }
                self.finishShutdownIfDrained()
                self.scheduleWarmupRetry()
            }
        }
    }

    private func scheduleWarmupRetry() {
        lock.withLock {
            guard !isShuttingDown, warmupRetryTask == nil else { return }
            warmupFailures = min(warmupFailures + 1, 6)
            let delay = TimeAmount.milliseconds(Int64(500 * (1 << (warmupFailures - 1))))
            warmupRetryTask = eventLoopGroup.next().scheduleTask(in: delay) { [weak self] in
                guard let self else { return }
                self.lock.withLock { self.warmupRetryTask = nil }
                self.ensureMinimumIdleConnections()
            }
        }
    }

    private func createConnection(for request: PoolRequest) {
        let shouldCreate = lock.withLock { () -> Bool in
            guard !isShuttingDown, request.isPending else {
                activeConnections = max(0, activeConnections - 1)
                return false
            }
            return true
        }
        guard shouldCreate else {
            _ = request.fail(Error.shutdown)
            finishShutdownIfDrained()
            ensureMinimumIdleConnections()
            return
        }
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

        _ = request.fail(error)
        finishShutdownIfDrained()

        if let nextRequest = next {
            createConnection(for: nextRequest)
        }
        ensureMinimumIdleConnections()
    }

    private func deliver(connection: TDSConnection, to request: PoolRequest) {
        let shuttingDown = self.lock.withLock { () -> Bool in
            if self.isShuttingDown {
                self.activeConnections = max(0, self.activeConnections - 1)
                self.shutdownClosesInProgress += 1
                return true
            }
            self.leased[Swift.ObjectIdentifier(connection)] = connection
            return false
        }
        if shuttingDown {
            _ = request.fail(Error.shutdown)
            _ = closeTracked(connection, alreadyCounted: true)
            return
        }
        guard request.attach(connection) else {
            _ = release(connection, close: true)
            return
        }
        let poolStats = self.lock.withLock {
            (active: self.activeConnections, idle: self.idle.count, waiters: self.waiters.count)
        }
        logger.debug("Pool checkout: active=\(poolStats.active) idle=\(poolStats.idle) waiters=\(poolStats.waiters)")
        if let validationQuery = configuration.validationQuery {
            connection.rawSql(validationQuery).whenComplete { result in
                switch result {
                case .success:
                    if !request.succeed(connection) {
                        _ = self.release(connection, close: true)
                    }
                case .failure(let error):
                    self.logger.warning("Validation query failed: \(error)")
                    _ = self.release(connection, close: true)
                    self.process(request: request)
                }
            }
        } else {
            if !request.succeed(connection) {
                _ = release(connection, close: true)
            }
        }
    }

    public func start() {
        ensureMinimumIdleConnections()
    }
}
