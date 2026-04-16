import Foundation
import NIO
import NIOSSL
import Logging

public final class TDSConnection {
    let channel: Channel
    let tokenRing: TDSTokenRing
    internal let requestHandler: TDSRequestHandler
    internal let tlsConfiguration: TLSConfiguration?
    internal let serverHostname: String?
    internal let firstDecoderName: String
    internal let firstEncoderName: String
    internal let pipelineCoordinatorName: String
    
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
    // Connection reset flag — set when a pooled connection is returned.
    // The next outbound request will carry the RESETCONNECTION bit in its
    // TDS packet header, telling SQL Server to reset session state.
    internal var needsConnectionReset: Bool = false

    // Stall detection support
    var lastStallSnapshot: String = ""
    
    init(
        channel: Channel,
        requestHandler: TDSRequestHandler,
        tlsConfiguration: TLSConfiguration?,
        serverHostname: String?,
        firstDecoderName: String,
        firstEncoderName: String,
        pipelineCoordinatorName: String,
        logger: Logger
    ) {
        self.channel = channel
        self.requestHandler = requestHandler
        self.tlsConfiguration = tlsConfiguration
        self.serverHostname = serverHostname
        self.firstDecoderName = firstDecoderName
        self.firstEncoderName = firstEncoderName
        self.pipelineCoordinatorName = pipelineCoordinatorName
        self.logger = logger
        self.didClose = false
        let ringSize = ProcessInfo.processInfo.environment["TDS_TOKEN_RING_SIZE"].flatMap { Int($0) } ?? 128
        self.tokenRing = TDSTokenRing(capacity: ringSize)
        self.channel.closeFuture.whenComplete { [weak self] (_: Result<Void, any Error>) in
            self?.didClose = true
        }
    }
    
    /// SQL Server product major version from the LOGINACK token. Zero until login completes.
    public var serverMajorVersion: UInt8 {
        requestHandler.serverMajorVersion
    }

    // Transaction state accessors
    public var transactionDescriptor: [UInt8] {
        return currentTransactionDescriptor
    }
    
    public var requestCount: UInt32 {
        return outstandingRequestCount
    }
    
    public func updateTransactionState(descriptor: [UInt8], requestCount: UInt32) {
        self.currentTransactionDescriptor = descriptor
        self.outstandingRequestCount = requestCount
        self.isInTransaction = !descriptor.allSatisfy { $0 == 0 }
    }

    public func updateSessionStatePayload(_ payload: [UInt8]) {
        self.lastSessionStatePayload = payload
    }

    public func updateDataClassificationPayload(_ payload: [UInt8]) {
        self.lastDataClassificationPayload = payload
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.didClose else {
            return self.eventLoop.makeSucceededFuture(())
        }
        self.didClose = true
       
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
        if !self.didClose {
            self.closeSilently()
        }
    }

    // Sends an ATTENTION signal to the server to cancel the currently running request.
    // This is best-effort and does not remove the current request from the queue; the
    // server will respond by terminating the active operation.
    /// Marks this connection for a TDS RESETCONNECTION on the next outbound request.
    /// Called when a connection is returned to a pool and will be reused.
    public func markForReset() {
        needsConnectionReset = true
    }

    public func sendAttention() {
        self.channel.triggerUserOutboundEvent(TDSUserEvent.attention, promise: nil)
    }

    /// Disables auto-read on the underlying channel so that data is only read
    /// when explicitly requested via `requestRead()`. Used for back-pressure
    /// in streaming queries.
    public func suspendAutoRead() {
        channel.setOption(ChannelOptions.autoRead, value: false).whenFailure { _ in }
    }

    /// Re-enables auto-read on the underlying channel. Should be called when
    /// a streaming query completes to restore normal read behavior.
    public func resumeAutoRead() {
        channel.setOption(ChannelOptions.autoRead, value: true).whenFailure { _ in }
        channel.read()
    }

    /// Requests a single read from the channel. When auto-read is disabled,
    /// this triggers the next batch of data to be read from the socket.
    public func requestRead() {
        channel.read()
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
    public func snapshotSessionStatePayload() -> [UInt8] { lastSessionStatePayload }

    public func snapshotDataClassificationPayload() -> [UInt8] { lastDataClassificationPayload }
    
    public func rawSql(_ sql: String) -> EventLoopFuture<[TDSData]> {
        let promise = self.channel.eventLoop.makePromise(of: [TDSData].self)
        let request = RawSqlRequest(sql: sql, resultPromise: promise)
        _ = self.send(request, logger: self.logger)
        return promise.futureResult
    }
}

extension TDSConnection: @unchecked Sendable {}
