import NIO
import NIOSSL
import NIOTLS
import Logging

extension TDSConnection: TDSClient {
    public func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void> {
        // Allow writes even if the channel isn't active when using EmbeddedChannel in tests;
        // production pipelines will guard via normal channel state and handler behavior.
        request.log(to: self.logger)
        // Separate write completion from request completion. Avoid creating extra
        // EventLoopPromises where possible to reduce the surface for leaks on loop shutdown.
        let completionPromise: EventLoopPromise<Void> = PromiseTracker.makeTrackedPromise(on: self.channel.eventLoop, label: "TDSRequest.completion")
        let context = TDSRequestContext(delegate: request, promise: completionPromise)
        // Track completion locally to decide whether to fail on channel close
        var didComplete = false
        completionPromise.futureResult.whenComplete { _ in didComplete = true }
        self.logger.debug("[TDSRequest.send] creating promises on loop=\(self.channel.eventLoop) channelActive=\(self.channel.isActive)")
        let writeFuture = self.channel.writeAndFlush(context)
        // If the channel closes at any point before the request completes, fail the
        // completion promise to avoid leaking a promise on loop shutdown. The channel’s
        // closeFuture completes successfully on normal close, so we must fail explicitly.
        self.channel.closeFuture.whenComplete { _ in
            if !didComplete {
                completionPromise.fail(TDSError.connectionClosed)
            }
        }
        // If the write fails (e.g., channel closed), reflect failure onto the overall completion.
        writeFuture.cascadeFailure(to: completionPromise)
        // We intentionally avoid waiting here; EmbeddedChannel processes writes synchronously.
        return completionPromise.futureResult
    }
}

public protocol TDSRequest {
    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket]
    func log(to logger: Logger)
}

public enum TDSPacketResponse {
    case done
    case `continue`
    case respond(with: [TDSPacket])
    case kickoffSSL
}

final class TDSRequestContext {
    let delegate: TDSRequest
    let promise: EventLoopPromise<Void>
    var lastError: Error?
    var started: Bool = false
    
    init(delegate: TDSRequest, promise: EventLoopPromise<Void>) {
        self.delegate = delegate
        self.promise = promise
    }
}

final class TDSRequestHandler: ChannelDuplexHandler {
    typealias InboundIn = TDSPacket
    typealias OutboundIn = TDSRequestContext
    typealias OutboundOut = TDSPacket
    
    /// `TDSMessage` handlers
    var firstDecoder: ByteToMessageHandler<TDSPacketDecoder>
    var firstEncoder: MessageToByteHandler<TDSPacketEncoder>
    var tlsConfiguration: TLSConfiguration?
    var serverHostname: String?
    private let firstDecoderName: String
    private let firstEncoderName: String
    private let pipelineCoordinatorName: String
    
    var sslClientHandler: NIOSSLClientHandler?
    
    var pipelineCoordinator: PipelineOrganizationHandler!
    
    enum State: Int {
        case start
        case sentPrelogin
        case sslHandshakeStarted
        case sslHandshakeComplete
        case sentLogin
        case loggedIn
    }
    
    private var state = State.start
    
    private var queue: [TDSRequestContext]
    
    let logger: Logger
    
    var currentRequest: TDSRequestContext? {
        get {
            self.queue.first
        }
    }
    
    public init(
        logger: Logger,
        firstDecoder: ByteToMessageHandler<TDSPacketDecoder>,
        firstEncoder: MessageToByteHandler<TDSPacketEncoder>,
        tlsConfiguration: TLSConfiguration? = nil,
        serverHostname: String? = nil,
        firstDecoderName: String,
        firstEncoderName: String,
        pipelineCoordinatorName: String
    ) {
        self.logger = logger
        self.queue = []
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.tlsConfiguration = tlsConfiguration
        self.serverHostname = serverHostname
        self.firstDecoderName = firstDecoderName
        self.firstEncoderName = firstEncoderName
        self.pipelineCoordinatorName = pipelineCoordinatorName
    }
    
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        let packet = self.unwrapInboundIn(data)
        guard let request = self.currentRequest else {
            // discard packet
            return
        }
        
        do {
            let response = try request.delegate.handle(packet: packet, allocator: context.channel.allocator)
            switch response {
            case .kickoffSSL:
                guard case .sentPrelogin = state else {
                    throw TDSError.protocolError("Unexpected state to initiate SSL kickoff. If encryption is negotiated, the SSL exchange should immediately follow the PRELOGIN phase.")
                }
                try sslKickoff(context: context)
            case .respond(let packets):
                try write(context: context, packets: packets, promise: nil)
                context.flush()
            case .continue:
                return
            case .done:
                if request.delegate is LoginRequest {
                    // Mark connection as logged in after LOGIN completes
                    self.state = .loggedIn
                }
                cleanupRequest(request)
                // Start the next queued request, if any
                startNextIfQueued(context: context)
            }
        } catch {
            cleanupRequest(request, error: error)
        }
    }
    
    private func sslKickoff(context: ChannelHandlerContext) throws {
        guard let tlsConfig = tlsConfiguration else {
            throw TDSError.protocolError("Encryption was requested but a TLS Configuration was not provided.")
        }
        
        let sslContext = try! NIOSSLContext(configuration: tlsConfig)
        let sslHandler = try! NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
        self.sslClientHandler = sslHandler
        
        let coordinator = PipelineOrganizationHandler(logger: logger, firstDecoder, firstEncoder, sslHandler)
        self.pipelineCoordinator = coordinator
        
        do {
            let ops = context.channel.pipeline.syncOperations
            try ops.addHandler(coordinator, name: pipelineCoordinatorName, position: .before(self))
            try ops.addHandler(sslHandler, position: .after(coordinator))
            self.state = .sslHandshakeStarted
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    private func cleanupRequest(_ request: TDSRequestContext, error: Error? = nil) {
        self.queue.removeFirst()
        if let error = error {
            request.promise.fail(error)
        } else {
            request.promise.succeed(())
        }
    }
    
    private func write(context: ChannelHandlerContext, packets: [TDSPacket], promise: EventLoopPromise<Void>?) throws {
        var packets = packets
        guard let requestType = packets.first?.type else {
            return
        }
        
        switch requestType {
        case .prelogin:
            switch state {
            case .start:
                state = .sentPrelogin
            case .sentPrelogin, .sslHandshakeStarted, .sslHandshakeComplete, .sentLogin, .loggedIn:
                throw TDSError.protocolError("PRELOGIN message must be the first message sent and may only be sent once per connection.")
            }
        case .tds7Login:
            self.logger.debug("Preparing to write LOGIN; state=\(self.state)")
            switch state {
            case .start, .sentPrelogin, .sslHandshakeComplete:
                // Be forgiving if state didn't flip to sentPrelogin yet due to scheduling.
                state = .sentLogin
            case .sslHandshakeStarted:
                // Handshake in progress: defer emitting LOGIN packets until handshake completes.
                // Keep the request queued; startNextIfQueued() will kick in on handshake completion.
                self.logger.debug("Deferring LOGIN until SSL handshake completes")
                promise?.succeed(())
                return
            case .sentLogin:
                // A LOGIN is already in-flight; do not emit another. Keep the current
                // head-of-queue request (the real LOGIN) and wait for its completion.
                self.logger.debug("Ignoring duplicate LOGIN write while previous LOGIN is in-flight")
                promise?.succeed(())
                return
            case .loggedIn:
                // Already logged in; treat any attempt to write LOGIN as a no-op and
                // complete the current request if it is a stray LOGIN.
                self.logger.debug("Dropping LOGIN after connection is already logged in")
                if let current = self.currentRequest, current.delegate is LoginRequest {
                    cleanupRequest(current)
                    startNextIfQueued(context: context)
                }
                promise?.succeed(())
                return
            }
        default:
            break
        }
        
        if let last = packets.popLast() {
            for item in packets {
                context.write(self.wrapOutboundOut(item), promise: nil)
            }
            context.write(self.wrapOutboundOut(last), promise: promise)
        } else {
            promise?.succeed(())
        }
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = self.unwrapOutboundIn(data)

        // Proactively drop duplicate LOGIN requests: if a LOGIN is already queued (or completed
        // and the connection is logged in), succeed this request immediately to avoid stalling
        // the queue with a second LOGIN that will never emit packets.
        if request.delegate is LoginRequest {
            let loginAlreadyQueued = self.queue.contains { $0.delegate is LoginRequest }
            self.logger.debug("Handling outbound LoginRequest; state=\(self.state) queuedLogin=\(loginAlreadyQueued) queueCount=\(self.queue.count)")
            if self.state == .loggedIn {
                // After successful login, any further LOGIN attempts are no-ops.
                self.logger.debug("Dropping LOGIN after connection is already logged in")
                request.promise.succeed(())
                promise?.succeed(())
                return
            } else if loginAlreadyQueued || self.state == .sentLogin {
                // A LOGIN is already in-flight or queued. Coalesce by dropping the new
                // duplicate request before it ever enters the queue. The original LOGIN
                // remains the head and will complete normally.
                self.logger.debug("Dropping duplicate queued LOGIN request")
                request.promise.succeed(())
                promise?.succeed(())
                return
            }
        }

        self.queue.append(request)
        if request.delegate is LoginRequest {
            self.logger.debug("Enqueued LoginRequest; state=\(self.state) queueCount=\(self.queue.count)")
        }
        // Only start and write immediately if this is the head of the queue; otherwise defer
        // until the current request completes to avoid interleaving requests without MARS.
        if self.queue.count == 1 {
            do {
                request.started = true
                let packets = try request.delegate.start(allocator: context.channel.allocator)
                try write(context: context, packets: packets, promise: promise)
                context.flush()
            } catch {
                self.errorCaught(context: context, error: error)
            }
        } else {
            promise?.succeed(())
        }
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        context.close(mode: mode, promise: promise)
        
        for current in self.queue {
            current.promise.fail(TDSError.connectionClosed)
        }
        self.queue = []
    }
    
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.error("TDS pipeline error: \(error.localizedDescription)")
        context.fireErrorCaught(error)
        if !queue.isEmpty {
            cleanupRequest(queue[0], error: error)
            while !queue.isEmpty {
                cleanupRequest(queue[0], error: TDSError.connectionClosed)
            }
        }
    }
    
    
    private func _userInboundEventTriggered(context: ChannelHandlerContext, event: Any) throws {
        if let sslHandler = sslClientHandler, let sslHandshakeComplete = event as? TLSUserEvent, case .handshakeCompleted = sslHandshakeComplete {
            // SSL Handshake complete
            // Remove pipeline coordinator and rearrange message encoder/decoder
            
            let pipeline = context.channel.pipeline
            let removals = pipeline.removeHandler(name: pipelineCoordinatorName)
                .flatMap {
                    pipeline.removeHandler(name: self.firstDecoderName)
                }
                .flatMap {
                    pipeline.removeHandler(name: self.firstEncoderName)
                }
            
            let future = removals.flatMap { _ in
                do {
                    let newDecoder = ByteToMessageHandler(TDSPacketDecoder(logger: self.logger))
                    let newEncoder = MessageToByteHandler(TDSPacketEncoder(logger: self.logger))
                    let ops = pipeline.syncOperations
                    try ops.addHandler(newDecoder, name: self.firstDecoderName, position: .after(sslHandler))
                    try ops.addHandler(newEncoder, name: self.firstEncoderName, position: .after(sslHandler))
                    self.firstDecoder = newDecoder
                    self.firstEncoder = newEncoder
                    self.pipelineCoordinator = nil
                    return context.eventLoop.makeSucceededFuture(())
                } catch {
                    return context.eventLoop.makeFailedFuture(error)
                }
            }
            
            future.whenSuccess {_ in
                self.logger.debug("Done w/ SSL Handshake and pipeline organization")
                self.state = .sslHandshakeComplete
                if let request = self.currentRequest {
                    self.cleanupRequest(request)
                    // Kick off the next queued request (LOGIN) immediately after PRELOGIN completes
                    self.startNextIfQueued(context: context)
                }
            }
            
            future.whenFailure { error in
                self.errorCaught(context: context, error: error)
            }
        }
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        do {
            try self._userInboundEventTriggered(context: context, event: event)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
    
    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        // Allow out-of-band ATTENTION signals to be sent without interfering
        // with the request queue. This helps cancel a long-running query.
        if let ev = event as? TDSUserEvent {
            switch ev {
            case .attention:
                guard context.channel.isActive else { promise?.fail(TDSError.connectionClosed); return }
                var empty = context.channel.allocator.buffer(capacity: 0)
                let packet = TDSPacket(
                    from: &empty,
                    ofType: .attentionSignal,
                    isLastPacket: true,
                    packetId: 1,
                    allocator: context.channel.allocator
                )
                do {
                    try write(context: context, packets: [packet], promise: nil)
                    context.flush()
                    promise?.succeed(())
                } catch {
                    promise?.fail(error)
                }
                return
            case .failCurrentRequestTimeout:
                // Fail the current request promise with a timeout-like error without closing the channel.
                if let current = self.currentRequest, context.channel.isActive {
                    cleanupRequest(current, error: TDSError.protocolError("request timeout"))
                }
                promise?.succeed(())
                return
            }
        }
        promise?.succeed(())
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("TDSRequestHandler.channelInactive: draining \(queue.count) pending requests; pipelineState=\(pipelineCoordinator?.stateDescription ?? "<nil>")")
        // Fail any pending TLS handshake output promise to avoid leaking promises on loop shutdown.
        pipelineCoordinator?.failHandshakeIfPending()
        while !queue.isEmpty {
            cleanupRequest(queue[0], error: TDSError.connectionClosed)
        }
        // Diagnostic: dump any unresolved promises we created on this loop
        PromiseTracker.dumpUnresolved(context: "channelInactive loop=\(context.eventLoop)")
        context.fireChannelInactive()
    }

    deinit {
        // Defensive: if the handler is torn down while there are still
        // unresolved request promises, fail them to avoid leaking promises.
        if !queue.isEmpty {
            logger.debug("TDSRequestHandler.deinit: failing \(queue.count) pending requests to avoid leaks")
        }
        while !queue.isEmpty {
            let req = queue.removeFirst()
            req.promise.fail(TDSError.connectionClosed)
        }
    }

    // MARK: - Queue progression
    private func startNextIfQueued(context: ChannelHandlerContext) {
        guard let next = self.currentRequest else { return }
        guard context.channel.isActive else { return }
        guard !next.started else { return }
        do {
            next.started = true
            let packets = try next.delegate.start(allocator: context.channel.allocator)
            try write(context: context, packets: packets, promise: nil)
            context.flush()
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
}
