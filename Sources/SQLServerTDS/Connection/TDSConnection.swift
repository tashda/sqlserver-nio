import Foundation
import NIO
import Logging

public final class TDSConnection {
    let channel: Channel
    let tokenRing: TDSTokenRing
    // Coalesce concurrent login() calls on the same connection
    // to a single in-flight future.
    var _loginFuture: EventLoopFuture<Void>?
    
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return channel.closeFuture
    }
    
    public var logger: Logger

    private var didClose: Bool

    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    // Transaction state management
    private var currentTransactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0] // 8 bytes like Microsoft
    private var outstandingRequestCount: UInt32 = 1
    private var isInTransaction: Bool = false
    // Session state & data classification snapshots (raw payloads)
    private var lastSessionStatePayload: [UInt8] = []
    private var lastDataClassificationPayload: [UInt8] = []
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
        self.didClose = false
        let ringSize = ProcessInfo.processInfo.environment["TDS_TOKEN_RING_SIZE"].flatMap { Int($0) } ?? 128
        self.tokenRing = TDSTokenRing(capacity: ringSize)
    }
    
    // Transaction state accessors
    public var transactionDescriptor: [UInt8] {
        return currentTransactionDescriptor
    }
    
    public var requestCount: UInt32 {
        return outstandingRequestCount
    }
    
    internal func updateTransactionState(descriptor: [UInt8], requestCount: UInt32) {
        self.currentTransactionDescriptor = descriptor
        self.outstandingRequestCount = requestCount
        self.isInTransaction = !descriptor.allSatisfy { $0 == 0 }
        let descriptorHex = descriptor.map { String(format: "%02x", $0) }.joined()
        logger.trace("TDS transaction state updated: descriptor=\(descriptorHex), requestCount=\(requestCount), inTransaction=\(isInTransaction)")
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.didClose else {
            return self.eventLoop.makeSucceededFuture(())
        }
        self.didClose = true
       
        // Close the channel; Channel operations are thread‑safe and will hop to the
        // channel's event loop as needed. Avoid scheduling explicitly on the event loop
        // to prevent "schedule tasks on an EventLoop that has already shut down" during
        // shutdown races in tests.
        return self.channel.close(mode: .all)
    }

    /// Best-effort, promise-free close used during deinitialization to avoid
    /// creating futures that might outlive the event loop during shutdown.
    public func closeSilently() {
        guard !self.didClose else { return }
        self.didClose = true
        self.channel.close(promise: nil)
    }

    deinit {
        assert(self.didClose, "TDSConnection deinitialized before being closed.")
    }

    // Sends an ATTENTION signal to the server to cancel the currently running request.
    // This is best-effort and does not remove the current request from the queue; the
    // server will respond by terminating the active operation.
    public func sendAttention() {
        self.channel.triggerUserOutboundEvent(TDSUserEvent.attention, promise: nil)
    }

    // Fails the currently active request on this connection with a timeout-like
    // error without closing the underlying channel. Useful for watchdogs.
    public func failActiveRequestTimeout() {
        self.channel.triggerUserOutboundEvent(TDSUserEvent.failCurrentRequestTimeout, promise: nil)
    }

    public func tokenTraceSnapshot() -> [String] {
        return tokenRing.snapshot()
    }

    // MARK: - Session state & data classification
    internal func updateSessionState(payload: [UInt8]) {
        self.lastSessionStatePayload = payload
        logger.trace("TDS session state token received (\(payload.count) bytes)")
    }

    internal func updateDataClassification(payload: [UInt8]) {
        self.lastDataClassificationPayload = payload
        logger.trace("TDS data classification token received (\(payload.count) bytes)")
    }

    public func snapshotSessionStatePayload() -> [UInt8] { lastSessionStatePayload }
    public func snapshotDataClassificationPayload() -> [UInt8] { lastDataClassificationPayload }
}

extension TDSConnection: @unchecked Sendable {}
