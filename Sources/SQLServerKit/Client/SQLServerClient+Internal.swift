import Foundation
import NIO
import SQLServerTDS

extension SQLServerClient {
    internal func makeConnection(from pooled: SQLServerConnectionPool.PooledConnection) -> SQLServerConnection {
        let connectionConfiguration = configuration.connection
        let baseConnection = pooled.base
        let connection = SQLServerConnection(
            base: baseConnection,
            configuration: connectionConfiguration,
            metadataCache: metadataCache,
            logger: logger,
            reuseOnClose: true,
            releaseClosure: { (close: Bool) -> EventLoopFuture<Void> in
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

    internal func withFreshConnection<Result: Sendable>(
        on eventLoop: EventLoop?,
        _ operation: @Sendable @escaping (SQLServerConnection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return SQLServerConnection.connect(
            configuration: configuration.connection,
            on: loop,
            logger: logger
        ).withTestTimeoutIfEnabled(on: loop).flatMap { connection in
            operation(connection).flatMap { value in
                connection.close().map { value }
            }.flatMapError { error in
                connection.invalidate().flatMap { _ in
                    loop.makeFailedFuture(SQLServerError.normalize(error))
                }
            }
        }
    }

    internal func healthProbe(_ connection: SQLServerConnection, on loop: EventLoop) -> EventLoopFuture<Void> {
        let request = RawSqlRequest(
            sql: "SELECT 1 AS __ping__;"
        )
        return connection.underlying.send(request, logger: connection.logger).map { _ in () }
    }

    /// Retry only acquisition and health validation. The caller's operation has
    /// not started yet, so there is no SQL outcome to duplicate.
    internal func acquireHealthyConnection(on loop: EventLoop, attempt: Int = 1) -> EventLoopFuture<SQLServerConnection> {
        guard !isClientShutdown else { return loop.makeFailedFuture(SQLServerError.clientShutdown) }
        return pool.checkout(on: loop).flatMap { pooled in
            let connection = self.makeConnection(from: pooled)
            return self.healthProbe(connection, on: loop).map { connection }.flatMapError { error in
                connection.invalidate().recover { _ in () }.flatMap {
                    loop.makeFailedFuture(SQLServerError.normalize(error))
                }
            }
        }.flatMapError { error in
            let normalized = SQLServerError.normalize(error)
            guard attempt < self.retryConfiguration.maximumAttempts,
                  !self.isClientShutdown,
                  self.retryConfiguration.shouldRetry(normalized)
            else { return loop.makeFailedFuture(normalized) }
            let proposed = self.retryConfiguration.backoffStrategy(attempt)
            let seconds = proposed.isFinite ? min(max(proposed, 0), 30) : 30
            self.logger.debug("Connection acquisition attempt \(attempt) failed; retrying after \(seconds)s")
            return loop.scheduleTask(in: seconds.nioTimeAmount) {}.futureResult.flatMap {
                self.acquireHealthyConnection(on: loop, attempt: attempt + 1)
            }
        }
    }

}
