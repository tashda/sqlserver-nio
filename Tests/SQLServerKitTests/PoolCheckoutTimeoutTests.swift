import XCTest
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
import SQLServerTDS
@testable import SQLServerKit

final class PoolCheckoutTimeoutTests: XCTestCase, @unchecked Sendable {
    func testShutdownWaitsForPendingPhysicalConnectionAttempt() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { group.shutdownGracefully { _ in } }
        let loop = group.next()
        let creation = loop.makePromise(of: TDSConnection.self)
        let pool = SQLServerConnectionPool(
            configuration: .init(maximumConcurrentConnections: 1),
            eventLoopGroup: group,
            connectionFactory: { _ in creation.futureResult }
        )

        let checkout = pool.checkout(on: loop)
        let shutdown = pool.shutdownGracefully()
        let shutdownCompleted = NIOLockedValueBox(false)
        shutdown.whenComplete { _ in shutdownCompleted.withLockedValue { $0 = true } }
        do {
            _ = try await checkout.get()
            XCTFail("Pending checkout should fail during shutdown")
        } catch SQLServerConnectionPool.Error.shutdown {
            // Expected.
        }
        XCTAssertFalse(shutdownCompleted.withLockedValue { $0 })

        creation.fail(SQLServerError.connectionClosed)
        try await shutdown.get()
    }

    func testTimedOutConnectionCreationDoesNotCompleteCheckoutTwice() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { group.shutdownGracefully { _ in } }
        let loop = group.next()
        let creation = loop.makePromise(of: TDSConnection.self)
        let pool = SQLServerConnectionPool(
            configuration: .init(maximumConcurrentConnections: 1, checkoutTimeout: 0.02),
            eventLoopGroup: group,
            connectionFactory: { _ in creation.futureResult }
        )

        do {
            _ = try await pool.checkout(on: loop).get()
            XCTFail("Checkout should time out while connection creation is pending")
        } catch let error as SQLServerError {
            guard case .timeout = error else {
                return XCTFail("Expected checkout timeout, got \(error)")
            }
        }

        // Creation can fail after the checkout promise has already failed.
        // This must not complete that promise a second time or leak a pool slot.
        creation.fail(SQLServerError.connectionClosed)
        try await Task.sleep(for: .milliseconds(20))
        XCTAssertEqual(pool.statusSnapshot().active, 0)
        try await pool.shutdownGracefully().get()
    }
}
