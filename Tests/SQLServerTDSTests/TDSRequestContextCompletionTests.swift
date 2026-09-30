@testable import SQLServerTDS
import NIOCore
import NIOPosix
import XCTest

final class TDSRequestContextCompletionTests: XCTestCase, @unchecked Sendable {
    func testCloseAndWriteFailureCompleteRequestOnlyOnce() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { group.shutdownGracefully { _ in } }
        let loop = group.next()
        let completion = loop.makePromise(of: Void.self)
        let results = loop.makePromise(of: [TDSData].self)
        let tokens = RequestTokenHandler(
            promise: completion,
            onRow: nil,
            onMetadata: nil,
            onDone: nil,
            onMessage: nil,
            onReturnValue: nil
        )
        let context = TDSRequestContext(
            delegate: RawSqlRequest(sql: "SELECT 1"),
            completionPromise: completion,
            resultPromise: results,
            tokenHandler: tokens
        )

        context.fail(TDSError.connectionClosed)
        context.fail(TDSError.connectionClosed)
        context.succeed([])

        do {
            try await completion.futureResult.get()
            XCTFail("Closed request should fail")
        } catch {
            XCTAssertEqual(error as? TDSError, .connectionClosed)
        }
        do {
            _ = try await results.futureResult.get()
            XCTFail("Closed request results should fail")
        } catch {
            XCTAssertEqual(error as? TDSError, .connectionClosed)
        }
    }
}
