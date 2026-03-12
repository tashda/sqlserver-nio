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

    internal func executeWithRetry<Result: Sendable>(
        operationName: String,
        on eventLoop: EventLoop,
        operation: @Sendable @escaping () -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        if isClientShutdown {
            return eventLoop.makeFailedFuture(SQLServerError.clientShutdown)
        }

        @Sendable
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
                return attempt(currentAttempt + 1)
            }
        }

        return attempt(1)
    }

    internal func shouldRetry(error: Swift.Error, attempt: Int) -> Bool {
        if attempt >= retryConfiguration.maximumAttempts {
            return false
        }
        if isClientShutdown {
            return false
        }
        return retryConfiguration.shouldRetry(error)
    }

    internal func healthProbe(_ connection: SQLServerConnection, on loop: EventLoop) -> EventLoopFuture<Void> {
        let request = RawSqlRequest(
            sql: "SELECT 1 AS __ping__;"
        )
        return connection.underlying.send(request, logger: connection.logger).map { _ in () }
    }

}
