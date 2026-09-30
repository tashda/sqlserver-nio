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

    /// Checks out a session. Validation and reset happen inside the pool,
    /// and session creation already retries transient network failures, so
    /// no caller operation has started when this fails.
    internal func acquireHealthyConnection(on loop: EventLoop) -> EventLoopFuture<SQLServerConnection> {
        guard !isClientShutdown else { return loop.makeFailedFuture(SQLServerError.clientShutdown) }
        return pool.checkout(on: loop)
            .map { self.makeConnection(from: $0) }
            .flatMapErrorThrowing { error in
                if case SQLServerConnectionPool.Error.poolClosed = error { throw SQLServerError.clientShutdown }
                if case SQLServerConnectionPool.Error.shutdown = error { throw SQLServerError.clientShutdown }
                throw SQLServerError.normalize(error)
            }
    }
}
