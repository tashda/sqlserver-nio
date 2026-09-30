import Foundation
import NIO
import NIOSSL
import NIOConcurrencyHelpers
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

    private let closeLock = NIOLock()
    private var didClose: Bool

    /// True once the connection cannot be used: the channel is closed, or
    /// the driver has given up on it (protocol failure, unacknowledged
    /// cancellation) and is closing it.
    public var isClosed: Bool {
        return !self.channel.isActive || closeLock.withLock { unusable }
    }

    private var unusable = false

    internal func markUnusable() {
        closeLock.withLock { unusable = true }
    }
    
    // Transaction state management
    private var currentTransactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0] // 8 bytes like Microsoft
    private var outstandingRequestCount: UInt32 = 1
    private var isInTransaction: Bool = false
    // Session state & data classification snapshots (raw payloads)
    private var lastSessionStatePayload: [UInt8] = []
    private var lastDataClassificationPayload: [UInt8] = []
    // Connection reset flag. The next request started carries the
    // RESETCONNECTION bit in its first packet header.
    internal var needsConnectionReset: Bool = false
    // Session facts reported by the server through ENVCHANGE tokens. Written
    // on the event loop, read from any thread.
    private let sessionLock = NIOLock()
    private var _currentDatabase: String?
    private var _routingTarget: TDSRoutingTarget?

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
            guard let self else { return }
            self.closeLock.withLock { self.didClose = true }
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

    /// The database the server reports as current. Updated by every `USE`,
    /// including one inside a user batch or stored procedure. Nil until login.
    public var currentDatabase: String? {
        sessionLock.withLock { _currentDatabase }
    }

    /// The server a login response redirected this connection to, if any.
    public var routingTarget: TDSRoutingTarget? {
        sessionLock.withLock { _routingTarget }
    }

    internal func updateCurrentDatabase(_ database: String) {
        sessionLock.withLock { _currentDatabase = database }
    }

    internal func updateRouting(_ target: TDSRoutingTarget?) {
        sessionLock.withLock { _routingTarget = target }
    }

    internal func consumeConnectionResetRequest() -> Bool {
        guard needsConnectionReset else { return false }
        needsConnectionReset = false
        return true
    }

    public func updateSessionStatePayload(_ payload: [UInt8]) {
        self.lastSessionStatePayload = payload
    }

    public func updateDataClassificationPayload(_ payload: [UInt8]) {
        self.lastDataClassificationPayload = payload
    }
    
    public func close() -> EventLoopFuture<Void> {
        let shouldClose = closeLock.withLock { () -> Bool in
            guard !didClose else { return false }
            didClose = true
            return true
        }
        guard shouldClose else { return channel.closeFuture }
       
        return self.channel.close(mode: .all).flatMapError { error in
            // SQL Server can close the TCP socket without TLS close_notify while
            // responding to our explicit close. The transport is already being
            // discarded, so this does not invalidate a completed operation.
            if let sslError = error as? NIOSSLError,
               case .uncleanShutdown = sslError {
                return self.eventLoop.makeSucceededFuture(())
            }
            return self.eventLoop.makeFailedFuture(error)
        }
    }

    /// Best-effort, promise-free close used during deinitialization to avoid
    /// creating futures that might outlive the event loop during shutdown.
    public func closeSilently() {
        let shouldClose = closeLock.withLock { () -> Bool in
            guard !didClose else { return false }
            didClose = true
            return true
        }
        guard shouldClose else { return }
        self.channel.close(promise: nil)
    }

    deinit {
        if !closeLock.withLock({ didClose }) {
            self.closeSilently()
        }
    }

    /// Marks this connection for a TDS RESETCONNECTION on the next request.
    public func markForReset() {
        eventLoop.execute { self.needsConnectionReset = true }
    }

    /// Cancels the request currently executing on this connection, if any.
    /// Prefer `TDSRequestHandle.cancel()`, which cannot affect a later request.
    public func sendAttention() {
        self.channel.triggerUserOutboundEvent(TDSUserEvent.attention, promise: nil)
    }

    /// How long to wait for the server to acknowledge a cancellation before
    /// the connection is closed.
    public func setAttentionAcknowledgementTimeout(_ timeout: TimeAmount) {
        eventLoop.execute { self.requestHandler.attentionAcknowledgementTimeout = timeout }
    }

    // Fails the currently active request with a timeout error and cancels it
    // on the server.
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
