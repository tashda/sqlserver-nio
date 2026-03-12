import Foundation
import NIO
import SQLServerTDS

extension SQLServerClient {
    @available(*, deprecated, message: "Use async execute(_:on:) instead.")
    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        let batches = sql.components(separatedBy: "\nGO\n").filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }

        guard !batches.isEmpty else {
            return loop.makeSucceededFuture(SQLServerExecutionResult(rows: [TDSRow](), done: [], messages: []))
        }

        let fut = withConnection(on: loop) { connection in
            var last: EventLoopFuture<SQLServerExecutionResult> = connection.eventLoop.makeSucceededFuture(SQLServerExecutionResult(rows: [TDSRow](), done: [], messages: []))
            for batchSql in batches {
                last = last.flatMap { _ in connection.execute(batchSql) }
            }
            return last
        }
        return fut.withTestTimeoutIfEnabled(on: loop)
    }

    @available(*, deprecated, message: "Use async execute(_:on:) instead.")
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

    @available(*, deprecated, message: "Use async query(_:on:) instead.")
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

    @available(*, deprecated, message: "Use async query(_:on:) with timeout handling in Swift concurrency.")
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

    @available(*, deprecated, message: "Use async queryScalar(_:as:on:) instead.")
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

    public func executeSeparateBatches(_ sqlStatements: [String]) -> EventLoopFuture<[SQLServerExecutionResult]> {
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

    internal func isCommentOnlyBatch(_ text: String) -> Bool {
        let lines = text.components(separatedBy: .newlines)
        for line in lines {
            let trimmedLine = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmedLine.isEmpty && !trimmedLine.hasPrefix("--") {
                return false
            }
        }
        return true
    }
}
