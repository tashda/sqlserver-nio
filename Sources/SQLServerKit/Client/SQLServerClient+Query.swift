import Foundation
import NIO
import SQLServerTDS

extension SQLServerClient {
    public func execute(
        _ sql: String,
        on eventLoop: EventLoop? = nil
    ) -> EventLoopFuture<SQLServerExecutionResult> {
        let loop = eventLoop ?? eventLoopGroup.next()
        let batches = sql.components(separatedBy: "\nGO\n").filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }

        guard !batches.isEmpty else {
            return loop.makeSucceededFuture(.init(rows: [], done: [], messages: []))
        }

        let fut = withConnection(on: loop) { connection in
            var last: EventLoopFuture<SQLServerExecutionResult> = connection.eventLoop.makeSucceededFuture(.init(rows: [], done: [], messages: []))
            for batchSql in batches {
                last = last.flatMap { _ in connection.execute(batchSql) }
            }
            return last
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
    ) -> EventLoopFuture<[TDSRow]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        let fut: EventLoopFuture<[TDSRow]> = withConnection(on: loop) { connection in
            connection.query(sql)
        }
        return fut.withTestTimeoutIfEnabled(on: loop)
    }

    public func query(
        _ sql: String,
        on eventLoop: EventLoop? = nil,
        timeout seconds: TimeInterval
    ) -> EventLoopFuture<[TDSRow]> {
        let loop = eventLoop ?? eventLoopGroup.next()
        return self.withConnection(on: loop) { conn in
            conn.execute(sql, timeout: seconds).map(\.rows)
        }
    }

    public func queryScalar<T: TDSDataConvertible & Sendable>(
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
