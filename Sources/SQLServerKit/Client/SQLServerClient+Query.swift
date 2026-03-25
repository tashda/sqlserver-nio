import Foundation
import NIO
import Logging
import SQLServerTDS

extension SQLServerClient {
    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        let start = Date()
        let sqlSnippet = String(sql.prefix(80))
        let queryLogger = self.logger
        let fut = withConnection(on: loop) { connection in
            connection.execute(sql)
        }.map { result -> SQLServerExecutionResult in
            let elapsed = Date().timeIntervalSince(start)
            let elapsedMs = String(format: "%.1fms", elapsed * 1000)
            queryLogger.debug("Query completed: \(result.rows.count) rows in \(elapsedMs) — \(sqlSnippet)")
            return result
        }
        return fut.withTestTimeoutIfEnabled(on: loop)
    }

    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil,
        timeout seconds: TimeInterval
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return self.withConnection(on: loop) { conn in
            conn.execute(sql, timeout: seconds)
        }
    }

    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<[SQLServerRow]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        let fut: EventLoopFuture<[SQLServerRow]> = withConnection(on: loop) { connection in
            connection.query(sql)
        }
        return fut.withTestTimeoutIfEnabled(on: loop)
    }

    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil,
        timeout seconds: TimeInterval
    ) -> EventLoopFuture<[SQLServerRow]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return self.withConnection(on: loop) { conn in
            conn.execute(sql, timeout: seconds).map(\.rows)
        }
    }

    public func queryScalar<T: SQLServerDataConvertible & Sendable>(
        _ sql: String,
        as type: T.Type = T.self,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<T?> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withConnection(on: loop) { connection in
            connection.queryScalar(sql, as: type)
        }
    }

    public func executeOnFreshConnection(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return withFreshConnection(on: loop) { connection in
            connection.execute(sql)
        }
    }

    internal func executeSeparateBatches(_ sqlStatements: [String]) -> EventLoopFuture<[SQLServerExecutionResult]> {
        let promise = eventLoopGroup.next().makePromise(of: [SQLServerExecutionResult].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                do {
                    return try await self.executeSeparateBatches(sqlStatements)
                } catch {
                    if error.localizedDescription.contains("Already closed") {
                        throw SQLServerError.connectionClosed
                    }
                    throw error
                }
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

}
