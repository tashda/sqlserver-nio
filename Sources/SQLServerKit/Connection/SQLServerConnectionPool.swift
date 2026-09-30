import NIO
import NIOCore
import NIOConcurrencyHelpers
import Foundation
import Logging
import SQLServerTDS

/// A pool of authenticated SQL Server sessions.
///
/// A session returned to the pool is reset before anyone else can use it: the
/// pool sends a batch with the TDS RESETCONNECTION flag, which makes SQL
/// Server run `sp_reset_connection` (roll back open transactions, drop
/// temporary objects, restore SET options, database, CONTEXT_INFO,
/// SESSION_CONTEXT and security context to their post-login state), and then
/// re-applies the configured session options. If the reset fails for any
/// reason, including an impersonation that was never reverted (error 18059),
/// the session is closed instead of reused. This matches the reset contract
/// of the Microsoft ODBC, JDBC and SqlClient pools.
public final class SQLServerConnectionPool: @unchecked Sendable {
    public struct Configuration: Sendable {
        public var maximumConcurrentConnections: Int
        public var minimumIdleConnections: Int
        /// Idle sessions are closed after this long. SQL Server, firewalls and
        /// load balancers drop idle TCP sessions; closing first avoids handing
        /// out a dead session. Nil keeps idle sessions indefinitely.
        public var connectionIdleTimeout: TimeInterval?
        public var checkoutTimeout: TimeInterval
        /// Query used to validate a session that has been idle for longer
        /// than `validationInterval`. Defaults to `SELECT 1`.
        public var validationQuery: String?
        /// Sessions idle for less than this are handed out without a
        /// round trip. Nil validates every checkout.
        public var validationInterval: TimeInterval?

        public init(
            maximumConcurrentConnections: Int = 8,
            minimumIdleConnections: Int = 0,
            connectionIdleTimeout: TimeInterval? = 300,
            checkoutTimeout: TimeInterval = 30,
            validationQuery: String? = nil,
            validationInterval: TimeInterval? = 30
        ) {
            precondition(maximumConcurrentConnections > 0, "maximumConcurrentConnections must be positive")
            precondition(minimumIdleConnections >= 0, "minimumIdleConnections must be non-negative")
            precondition(minimumIdleConnections <= maximumConcurrentConnections, "minimumIdleConnections cannot exceed maximumConcurrentConnections")
            precondition(checkoutTimeout.isFinite && checkoutTimeout > 0, "checkoutTimeout must be finite and positive")
            if let connectionIdleTimeout {
                precondition(connectionIdleTimeout.isFinite && connectionIdleTimeout > 0, "connectionIdleTimeout must be finite and positive")
            }
            if let validationInterval {
                precondition(validationInterval.isFinite && validationInterval >= 0, "validationInterval must be finite and non-negative")
            }
            self.maximumConcurrentConnections = maximumConcurrentConnections
            self.minimumIdleConnections = minimumIdleConnections
            self.connectionIdleTimeout = connectionIdleTimeout
            self.checkoutTimeout = checkoutTimeout
            self.validationQuery = validationQuery
            self.validationInterval = validationInterval
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

        init(promise: EventLoopPromise<TDSConnection>, eventLoop: EventLoop, id: UInt64) {
            self.promise = promise
            self.eventLoop = eventLoop
            self.id = id
        }

        var isPending: Bool { stateLock.withLock { !completed } }

        @discardableResult
        func succeed(_ connection: TDSConnection) -> Bool {
            let won = stateLock.withLock { () -> Bool in
                guard !completed else { return false }
                completed = true
                return true
            }
            if won { promise.succeed(connection) }
            return won
        }

        @discardableResult
        func fail(_ error: Swift.Error) -> Bool {
            let won = stateLock.withLock { () -> Bool in
                guard !completed else { return false }
                completed = true
                return true
            }
            if won { promise.fail(error) }
            return won
        }
    }

    private struct IdleConnection {
        let connection: TDSConnection
        let since: NIODeadline
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

        /// Returns the session to the pool. With `close: true`, or when the
        /// session is no longer usable, the physical connection is closed.
        @discardableResult
        internal func release(close: Bool = false) -> EventLoopFuture<Void> {
            let alreadyReleased = releaseLock.withLock { () -> Bool in
                if released { return true }
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
                if released { return false }
                released = true
                return true
            }
            if shouldRelease {
                // An abandoned lease may hold any session state.
                _ = pool.release(connection, close: true)
            }
        }
    }

    private let configuration: Configuration
    private let eventLoopGroup: EventLoopGroup
    private let connectionFactory: (EventLoop) -> EventLoopFuture<TDSConnection>
    /// Resets a returned session. Nil closes returned sessions instead.
    private let sessionReset: ((TDSConnection) -> EventLoopFuture<Void>)?
    private let lock = NIOLock()
    private var idle: [IdleConnection] = []
    private var leased: [Swift.ObjectIdentifier: TDSConnection] = [:]
    private var waiters = CircularBuffer<PoolRequest>()
    private var pendingRequests: [UInt64: PoolRequest] = [:]
    private var nextRequestID: UInt64 = 0
    /// Every physical session that exists or is being created: idle, leased,
    /// resetting, validating and connecting.
    private var activeConnections = 0
    private var isShuttingDown = false
    private var warmupFailures = 0
    private var warmupRetryTask: Scheduled<Void>?
    private var shutdownPromise: EventLoopPromise<Void>?
    private var closesInProgress = 0
    private let logger: Logger

    internal init(
        configuration: Configuration,
        eventLoopGroup: EventLoopGroup,
        logger: Logger = Logger(label: "tds.sqlserver.pool"),
        connectionFactory: @escaping (EventLoop) -> EventLoopFuture<TDSConnection>,
        sessionReset: ((TDSConnection) -> EventLoopFuture<Void>)? = nil
    ) {
        self.configuration = configuration
        self.eventLoopGroup = eventLoopGroup
        self.logger = logger
        self.connectionFactory = connectionFactory
        self.sessionReset = sessionReset
    }

    // MARK: - Checkout

    internal func checkout(on eventLoop: EventLoop? = nil) -> EventLoopFuture<PooledConnection> {
        let targetLoop = eventLoop ?? eventLoopGroup.next()
        let promise = targetLoop.makePromise(of: TDSConnection.self)
        let request = lock.withLock { () -> PoolRequest in
            nextRequestID += 1
            let request = PoolRequest(promise: promise, eventLoop: targetLoop, id: nextRequestID)
            pendingRequests[request.id] = request
            return request
        }

        let timeout = configuration.checkoutTimeout
        let timeoutTask = targetLoop.scheduleTask(in: timeout.nioTimeAmount) { [weak self] in
            guard request.fail(SQLServerError.timeout(
                description: "connection pool checkout timed out after \(timeout)s (all \(self?.configuration.maximumConcurrentConnections ?? 0) connections are in use or the server is unreachable)",
                underlying: nil
            )) else { return }
            let waiterCount: Int = self?.lock.withLock {
                if let index = self?.waiters.firstIndex(where: { $0.id == request.id }) {
                    self?.waiters.remove(at: index)
                }
                return self?.waiters.count ?? 0
            } ?? 0
            self?.logger.warning("Connection pool checkout timed out after \(timeout)s, waiters=\(waiterCount)")
        }

        process(request)

        return promise.futureResult.always { _ in
            timeoutTask.cancel()
            _ = self.lock.withLock { self.pendingRequests.removeValue(forKey: request.id) }
        }.map { connection in
            PooledConnection(connection: connection, pool: self)
        }
    }

    private enum CheckoutAction {
        case reuse(IdleConnection)
        case create
        case wait
        case fail(Swift.Error)
    }

    private func process(_ request: PoolRequest) {
        guard request.isPending else { return }
        let action: CheckoutAction = lock.withLock {
            if isShuttingDown { return .fail(Error.poolClosed) }
            // Most recently used first keeps the working set small, so
            // surplus sessions age out through the idle timeout.
            while let entry = idle.popLast() {
                entry.idleTask?.cancel()
                if entry.connection.isClosed {
                    activeConnections -= 1
                    continue
                }
                return .reuse(entry)
            }
            if activeConnections < configuration.maximumConcurrentConnections {
                activeConnections += 1
                return .create
            }
            waiters.append(request)
            return .wait
        }

        switch action {
        case .reuse(let entry):
            validateIfNeeded(entry).whenComplete { result in
                switch result {
                case .success:
                    self.deliver(entry.connection, to: request)
                case .failure(let error):
                    self.logger.debug("Discarding pooled session that failed validation: \(error)")
                    self.discard(entry.connection)
                    self.process(request)
                }
            }
        case .create:
            create(for: request)
        case .wait:
            break
        case .fail(let error):
            request.fail(error)
        }
        // A new session is created above whenever capacity allows, so the
        // pool never leaves a waiter queued behind closed idle entries.
    }

    private func validateIfNeeded(_ entry: IdleConnection) -> EventLoopFuture<Void> {
        let connection = entry.connection
        if let interval = configuration.validationInterval,
           NIODeadline.now() - entry.since < interval.nioTimeAmount {
            return connection.eventLoop.makeSucceededFuture(())
        }
        let query = configuration.validationQuery ?? "SELECT 1;"
        return connection.start(RawSqlRequest(sql: query), timeout: .seconds(15)).future
    }

    private func create(for request: PoolRequest) {
        connectionFactory(request.eventLoop).whenComplete { result in
            switch result {
            case .success(let connection):
                self.deliver(connection, to: request)
            case .failure(let error):
                self.logger.error("Connection pool failed to create connection: \(error)")
                self.lock.withLock { self.activeConnections -= 1 }
                request.fail(error)
                self.finishShutdownIfDrained()
                self.serveNextWaiter()
            }
        }
    }

    /// Hands a usable session to a checkout request, or back to the idle set
    /// when the request is no longer waiting (for example after its timeout).
    private func deliver(_ connection: TDSConnection, to request: PoolRequest) {
        let shuttingDown = lock.withLock { () -> Bool in
            if isShuttingDown { return true }
            leased[Swift.ObjectIdentifier(connection)] = connection
            return false
        }
        if shuttingDown {
            request.fail(Error.shutdown)
            discard(connection)
            return
        }
        guard request.succeed(connection) else {
            // Never used, so it needs no reset.
            lock.withLock { _ = leased.removeValue(forKey: Swift.ObjectIdentifier(connection)) }
            makeAvailable(connection)
            return
        }
        logger.trace("Pool checkout", metadata: ["active": "\(statusSnapshot().active)"])
    }

    // MARK: - Return

    fileprivate func release(_ connection: TDSConnection, close: Bool) -> EventLoopFuture<Void> {
        let wasLeased = lock.withLock { leased.removeValue(forKey: Swift.ObjectIdentifier(connection)) != nil }
        guard wasLeased else { return connection.eventLoop.makeSucceededFuture(()) }

        let shuttingDown = lock.withLock { isShuttingDown }
        guard !close, !shuttingDown, !connection.isClosed, let sessionReset else {
            return discard(connection)
        }

        // The caller does not wait for the reset; the session re-enters the
        // pool only after the server confirms it.
        sessionReset(connection).whenComplete { result in
            switch result {
            case .success:
                self.makeAvailable(connection)
            case .failure(let error):
                self.logger.debug("Closing pooled session that could not be reset: \(error)")
                self.discard(connection)
            }
        }
        return connection.eventLoop.makeSucceededFuture(())
    }

    /// Adds a freshly opened session to the idle set.
    internal func adoptIdle(_ connection: TDSConnection) {
        let accepted = lock.withLock { () -> Bool in
            guard !isShuttingDown, activeConnections < configuration.maximumConcurrentConnections else { return false }
            activeConnections += 1
            return true
        }
        if accepted {
            makeAvailable(connection)
        } else {
            connection.closeSilently()
        }
    }

    private func makeAvailable(_ connection: TDSConnection) {
        enum Next { case deliver(PoolRequest), idle, discard }
        let next: Next = lock.withLock {
            if isShuttingDown || connection.isClosed { return .discard }
            while let waiter = waiters.popFirst() {
                if waiter.isPending { return .deliver(waiter) }
            }
            let task = scheduleIdleClose(for: connection)
            idle.append(IdleConnection(connection: connection, since: .now(), idleTask: task))
            return .idle
        }
        switch next {
        case .deliver(let waiter):
            deliver(connection, to: waiter)
        case .idle:
            break
        case .discard:
            discard(connection)
        }
    }

    /// Closes a session and frees its slot for a waiting checkout.
    @discardableResult
    private func discard(_ connection: TDSConnection) -> EventLoopFuture<Void> {
        lock.withLock {
            activeConnections -= 1
            closesInProgress += 1
        }
        let closed = connection.close().always { _ in
            self.lock.withLock { self.closesInProgress -= 1 }
            self.finishShutdownIfDrained()
        }
        serveNextWaiter()
        ensureMinimumIdleConnections()
        return closed.recover { _ in }
    }

    private func serveNextWaiter() {
        let request: PoolRequest? = lock.withLock {
            guard !isShuttingDown, activeConnections < configuration.maximumConcurrentConnections else { return nil }
            while let waiter = waiters.popFirst() {
                if waiter.isPending {
                    activeConnections += 1
                    return waiter
                }
            }
            return nil
        }
        if let request { create(for: request) }
    }

    // MARK: - Idle maintenance

    private func scheduleIdleClose(for connection: TDSConnection) -> Scheduled<Void>? {
        guard let timeout = configuration.connectionIdleTimeout else { return nil }
        return connection.eventLoop.scheduleTask(in: timeout.nioTimeAmount) { [weak self, weak connection] in
            guard let self, let connection else { return }
            self.expireIdleConnection(connection)
        }
    }

    private func expireIdleConnection(_ connection: TDSConnection) {
        let expired = lock.withLock { () -> Bool in
            guard idle.count > configuration.minimumIdleConnections,
                  let index = idle.firstIndex(where: { $0.connection === connection }) else { return false }
            idle.remove(at: index)
            return true
        }
        if expired {
            discard(connection)
        } else if lock.withLock({ idle.contains { $0.connection === connection } }) {
            // Kept for the minimum idle count; check again later.
            lock.withLock {
                if let index = idle.firstIndex(where: { $0.connection === connection }) {
                    idle[index].idleTask = scheduleIdleClose(for: connection)
                }
            }
        }
    }

    private func ensureMinimumIdleConnections() {
        guard configuration.minimumIdleConnections > 0 else { return }
        let toCreate: Int = lock.withLock {
            guard !isShuttingDown else { return 0 }
            let missing = configuration.minimumIdleConnections - idle.count
            let available = configuration.maximumConcurrentConnections - activeConnections
            let count = max(0, min(missing, available))
            activeConnections += count
            return count
        }
        for _ in 0..<toCreate {
            connectionFactory(eventLoopGroup.next()).whenComplete { result in
                switch result {
                case .success(let connection):
                    self.lock.withLock { self.warmupFailures = 0 }
                    self.makeAvailable(connection)
                case .failure(let error):
                    self.logger.error("Connection pool warm-up failed: \(error)")
                    self.lock.withLock { self.activeConnections -= 1 }
                    self.finishShutdownIfDrained()
                    self.scheduleWarmupRetry()
                }
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

    public func start() {
        ensureMinimumIdleConnections()
    }

    // MARK: - Shutdown

    /// Fails waiting checkouts and closes every session, including sessions
    /// that are still leased; their current operations fail with
    /// `connectionClosed`. Completes once every physical connection is closed
    /// and no connection attempt is still pending.
    internal func shutdownGracefully() -> EventLoopFuture<Void> {
        let promise = eventLoopGroup.next().makePromise(of: Void.self)
        var existing: EventLoopFuture<Void>?
        var idleToClose: [TDSConnection] = []
        var leasedToClose: [TDSConnection] = []
        var pending: [PoolRequest] = []

        lock.withLock {
            if let shutdownPromise {
                existing = shutdownPromise.futureResult
                return
            }
            shutdownPromise = promise
            isShuttingDown = true
            warmupRetryTask?.cancel()
            warmupRetryTask = nil
            idle.forEach { $0.idleTask?.cancel() }
            idleToClose = idle.map(\.connection)
            idle.removeAll()
            leasedToClose = Array(leased.values)
            leased.removeAll()
            waiters.removeAll()
            pending = Array(pendingRequests.values)
            pendingRequests.removeAll()
        }
        if let existing { return existing }

        pending.forEach { $0.fail(Error.shutdown) }
        idleToClose.forEach { discard($0) }
        // Leased sessions are closed as well, so a lease that is never
        // returned cannot keep shutdown waiting. Their owners' operations
        // fail with connectionClosed and a later release is a no-op.
        leasedToClose.forEach { discard($0) }
        finishShutdownIfDrained()
        return promise.futureResult
    }

    private func finishShutdownIfDrained() {
        let complete: EventLoopPromise<Void>? = lock.withLock {
            guard isShuttingDown, activeConnections <= 0, closesInProgress == 0 else { return nil }
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
}
