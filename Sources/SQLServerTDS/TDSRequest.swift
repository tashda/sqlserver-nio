import NIO
import NIOSSL
import NIOTLS
import Logging

extension TDSConnection: TDSClient {
    public func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void> {
        request.log(to: self.logger)
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let request = TDSRequestContext(delegate: request, promise: promise)
        self.channel.writeAndFlush(request).cascadeFailure(to: promise)
        return promise.futureResult
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
                cleanupRequest(request)
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
            switch state {
            case .sentPrelogin, .sslHandshakeComplete:
                state = .sentLogin
            case .start, .sslHandshakeStarted, .sentLogin, .loggedIn:
                throw TDSError.protocolError("LOGIN message must follow immediately after the PRELOGIN message or (if encryption is enabled) SSL negotiation and may only be sent once per connection.")
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
        self.queue.append(request)
        do {
            let packets = try request.delegate.start(allocator: context.channel.allocator)
            try write(context: context, packets: packets, promise: promise)
            context.flush()
        } catch {
            self.errorCaught(context: context, error: error)
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
        
    }

    func channelInactive(context: ChannelHandlerContext) {
        while !queue.isEmpty {
            cleanupRequest(queue[0], error: TDSError.connectionClosed)
        }
        context.fireChannelInactive()
    }
}
