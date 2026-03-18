@preconcurrency import NIO
import NIOConcurrencyHelpers
@preconcurrency import NIOSSL
@preconcurrency import NIOTLS
import Logging

public enum TDSUserEvent: Sendable {
    case attention
    case failCurrentRequestTimeout
}

extension TDSConnection: TDSClient {
    public func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void> {
        request.log(to: self.logger)
        let completionPromise: EventLoopPromise<Void> = PromiseTracker.makeTrackedPromise(on: self.channel.eventLoop, label: "TDSRequest.completion")
        let resultPromise: EventLoopPromise<[TDSData]>
        if let rawSqlRequest = request as? RawSqlRequest, let existingPromise = rawSqlRequest.resultPromise {
            resultPromise = existingPromise
        } else {
            resultPromise = PromiseTracker.makeTrackedPromise(on: self.channel.eventLoop, label: "TDSRequest.result")
        }
        let tokenHandler = RequestTokenHandler(
            promise: completionPromise,
            onRow: request.onRow,
            onMetadata: request.onMetadata,
            onDone: request.onDone,
            onMessage: request.onMessage,
            onReturnValue: request.onReturnValue
        )
        let context = TDSRequestContext(
            delegate: request,
            completionPromise: completionPromise,
            resultPromise: resultPromise,
            tokenHandler: tokenHandler
        )
        let didComplete = NIOLockedValueBox(false)
        completionPromise.futureResult.whenComplete { _ in
            didComplete.withLockedValue { $0 = true }
        }
        self.logger.debug("[TDSRequest.send] creating promises on loop=\(self.channel.eventLoop) channelActive=\(self.channel.isActive)")
        let writeFuture = self.channel.writeAndFlush(context)
        self.channel.closeFuture.whenComplete { _ in
            if !didComplete.withLockedValue({ $0 }) {
                completionPromise.fail(TDSError.connectionClosed)
                resultPromise.fail(TDSError.connectionClosed)
            }
        }
        writeFuture.cascadeFailure(to: completionPromise)
        writeFuture.cascadeFailure(to: resultPromise)
        return completionPromise.futureResult
    }
}

public protocol TDSRequest {
    var packetType: TDSPacket.HeaderType { get }
    func serialize(into buffer: inout ByteBuffer) throws
    func log(to logger: Logger)
    var onRow: (@Sendable (TDSRow) -> Void)? { get }
    var onMetadata: (@Sendable ([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? { get }
    var onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? { get }
    var onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? { get }
    var onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? { get }
    var stream: Bool { get }
    var storesRowsInContext: Bool { get }
}

extension TDSRequest {
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        var buffer = allocator.buffer(capacity: TDSPacket.maximumPacketDataLength)
        try self.serialize(into: &buffer)
        return try TDSMessage(from: &buffer, ofType: self.packetType, allocator: allocator).packets
    }

    public var stream: Bool { false }
    public var storesRowsInContext: Bool { false }
}

public enum TDSPacketResponse {
    case done
    case `continue`
    case respond(with: [TDSPacket])
    case kickoffSSL
}

final class TDSRequestContext: @unchecked Sendable {
    let delegate: TDSRequest
    let completionPromise: EventLoopPromise<Void>
    let resultPromise: EventLoopPromise<[TDSData]>
    let tokenHandler: TokenHandler
    var lastError: Error?
    var started: Bool = false
    var rows: [TDSRow] = []

    init(
        delegate: TDSRequest,
        completionPromise: EventLoopPromise<Void>,
        resultPromise: EventLoopPromise<[TDSData]>,
        tokenHandler: TokenHandler
    ) {
        self.delegate = delegate
        self.completionPromise = completionPromise
        self.resultPromise = resultPromise
        self.tokenHandler = tokenHandler
    }
}

protocol TokenHandler: AnyObject {
    var columns: [TDSTokens.ColMetadataToken.ColumnData] { get }
    func onColMetadata(_ token: TDSTokens.ColMetadataToken)
    func onRow(_ token: TDSTokens.RowToken)
    func onDone(_ token: TDSTokens.DoneToken)
    func onMessage(_ token: TDSTokens.ErrorInfoToken)
    func onReturnValue(_ token: TDSTokens.ReturnValueToken)
}

final class RequestTokenHandler: TokenHandler {
    private let promise: EventLoopPromise<Void>
    private let onRowCallback: (@Sendable (TDSRow) -> Void)?
    private let onMetadataCallback: (@Sendable ([TDSTokens.ColMetadataToken.ColumnData]) -> Void)?
    private let onDoneCallback: (@Sendable (TDSTokens.DoneToken) -> Void)?
    private let onMessageCallback: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)?
    private let onReturnValueCallback: (@Sendable (TDSTokens.ReturnValueToken) -> Void)?

    private(set) var columns: [TDSTokens.ColMetadataToken.ColumnData] = []

    init(
        promise: EventLoopPromise<Void>,
        onRow: (@Sendable (TDSRow) -> Void)?,
        onMetadata: (@Sendable ([TDSTokens.ColMetadataToken.ColumnData]) -> Void)?,
        onDone: (@Sendable (TDSTokens.DoneToken) -> Void)?,
        onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)?,
        onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)?
    ) {
        self.promise = promise
        self.onRowCallback = onRow
        self.onMetadataCallback = onMetadata
        self.onDoneCallback = onDone
        self.onMessageCallback = onMessage
        self.onReturnValueCallback = onReturnValue
    }

    func onColMetadata(_ token: TDSTokens.ColMetadataToken) {
        self.columns = token.colData
        self.onMetadataCallback?(token.colData)
    }

    func onRow(_ token: TDSTokens.RowToken) {
        self.onRowCallback?(TDSRow(token: token, columns: self.columns))
    }

    func onDone(_ token: TDSTokens.DoneToken) {
        self.onDoneCallback?(token)
    }

    func onMessage(_ token: TDSTokens.ErrorInfoToken) {
        self.onMessageCallback?(token, token.type == .error)
    }

    func onReturnValue(_ token: TDSTokens.ReturnValueToken) {
        self.onReturnValueCallback?(token)
    }
}

final class TDSRequestHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer  // Complete messages from Message Assembly Layer
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

    // Reference to the actual decoder for mode switching
    private let packetDecoder: TDSPacketDecoder
    private let streamParser: TDSStreamParser

    var pipelineCoordinator: PipelineOrganizationHandler!
    private weak var currentContext: ChannelHandlerContext?
    private var currentEventLoop: EventLoop?

    // Reference to the TDSConnection for updating transaction state
    private weak var connection: TDSConnection?
    
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
        pipelineCoordinatorName: String,
        connection: TDSConnection? = nil
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
        self.connection = connection

        // Create a reference to the decoder for mode switching
        // Since ByteToMessageHandler doesn't expose its decoder directly,
        // we'll create our own instance that we can control
        self.packetDecoder = TDSPacketDecoder(logger: logger)
        self.streamParser = TDSStreamParser()

        // Replace the provided decoder with our controllable one
        self.firstDecoder = ByteToMessageHandler(packetDecoder)
    }

    /// Simpler ENVCHANGE token processing - SAFER approach that was working yesterday
    /// Only handle explicit transaction state changes, ignore implicit operations
    private func processEnvchangeToken(_ envToken: TDSTokens.EnvchangeToken<[Byte]>) {
        guard let connection = self.connection else {
            logger.warning("Received ENVCHANGE token but no connection reference available")
            return
        }

        switch envToken.envchangeType {
        case .beginTransaction: // Type 8
            logger.debug("ENVCHANGE beginTransaction received, newValue length=\(envToken.newValue.count)")
            let descriptor = Array(envToken.newValue.prefix(8))
            if descriptor.count == 8 {
                let hex = descriptor.map { String(format: "%02x", $0) }.joined()
                logger.debug("Transaction begin detected, descriptor=\(hex)")
                connection.updateTransactionState(descriptor: descriptor, requestCount: 1)
            }

        case .commitTransaction, // Type 9
             .rollbackTransaction, // Type 10
             .defectTransaction, // Type 12
             .transactionEnded: // Type 17
            logger.debug("ENVCHANGE transaction completion received type=\(String(describing: envToken.envchangeType))")
            // Reset to AutoCommit mode for all transaction completions
            let currentDescriptor = connection.transactionDescriptor
            let isInAutoCommit = currentDescriptor.allSatisfy { $0 == 0 }

            if !isInAutoCommit {
                let autocommitDescriptor = [UInt8](repeating: 0, count: 8)
                connection.updateTransactionState(descriptor: autocommitDescriptor, requestCount: 1)
                let action = envToken.envchangeType == .commitTransaction ? "committed" :
                           envToken.envchangeType == .rollbackTransaction ? "rolled back" :
                           envToken.envchangeType == .defectTransaction ? "defected" : "ended"
                logger.debug("Transaction \(action), returning to AutoCommit mode")
            }

        default:
            // Ignore all other ENVCHANGE types to prevent connection state corruption
            break
        }
    }

    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        guard let request = self.currentRequest else {
            // discard data
            return
        }

        do {
            var data = self.unwrapInboundIn(data)

            if let preloginRequest = request.delegate as? PreloginRequest {
                logger.trace("Received PRELOGIN response chunk (\(data.readableBytes) bytes)")
                let response = try preloginRequest.handle(dataStream: data, allocator: context.channel.allocator)

                switch response {
                case .done:
                    logger.trace("PRELOGIN completed successfully; advancing to LOGIN")
                    cleanupRequest(request)
                    startNextIfQueued(context: context)
                case .continue:
                    break
                case .respond(let packets):
                    try write(context: context, packets: packets, promise: nil)
                    context.flush()
                case .kickoffSSL:
                    try sslKickoff(context: context)
                }

                return
            }

            streamParser.buffer.writeBuffer(&data)
            let tokenParser = TDSTokenOperations(streamParser: streamParser, logger: logger)
            let tokens = try tokenParser.parse()
            var loginAckReceived = false

            for token in tokens {
                switch token.type {
                case .colMetadata:
                    let colMetadataToken = token as! TDSTokens.ColMetadataToken
                    request.tokenHandler.onColMetadata(colMetadataToken)
                case .row:
                    let rowToken = token as! TDSTokens.RowToken
                    let row = TDSRow(token: rowToken, columns: request.tokenHandler.columns)
                    if request.delegate.storesRowsInContext {
                        request.rows.append(row)
                    }
                    request.tokenHandler.onRow(rowToken)
                case .nbcRow:
                    let nbcRowToken = token as! TDSTokens.NbcRowToken
                    let syntheticRow = TDSTokens.RowToken(
                        colMetadata: nbcRowToken.colMetadata,
                        colData: nbcRowToken.colData
                    )
                    let row = TDSRow(token: syntheticRow, columns: request.tokenHandler.columns)
                    if request.delegate.storesRowsInContext {
                        request.rows.append(row)
                    }
                    request.tokenHandler.onRow(syntheticRow)
                case .done, .doneInProc, .doneProc:
                    let doneToken = token as! TDSTokens.DoneToken
                    let hasMore = (doneToken.status & 0x0001) != 0
                    let hasCount = (doneToken.status & 0x0010) != 0
                    logger.debug("[TDS DONE] type=\(token.type) status=0x\(String(format: "%04X", doneToken.status)) hasMore=\(hasMore) hasCount=\(hasCount) rowCount=\(doneToken.doneRowCount) curCmd=\(doneToken.curCmd)")
                    request.tokenHandler.onDone(doneToken)
                    let doneMoreFlag: UShort = 0x0001
                    if (doneToken.status & doneMoreFlag) == 0 {
                        // If this is a LoginRequest that completed without a LOGINACK,
                        // the server rejected the login. Fail with the captured error message.
                        if let loginReq = request.delegate as? LoginRequest, !loginAckReceived {
                            let message = loginReq.serverErrorMessage ?? "Login failed"
                            cleanupRequest(request, error: TDSError.invalidCredentials(message))
                        } else {
                            cleanupRequest(request)
                        }
                        startNextIfQueued(context: context)
                    }
                case .info, .error:
                    let messageToken = token as! TDSTokens.ErrorInfoToken
                    // Capture server error messages on LoginRequest so login failures
                    // include the actual server error text (e.g. error 18456).
                    if token.type == .error, let loginReq = request.delegate as? LoginRequest {
                        loginReq.serverErrorMessage = messageToken.messageText
                    }
                    request.tokenHandler.onMessage(messageToken)
                case .returnValue:
                    let returnValueToken = token as! TDSTokens.ReturnValueToken
                    request.tokenHandler.onReturnValue(returnValueToken)
                case .sspi:
                    if let sspiToken = token as? TDSTokens.SSPIToken,
                       let loginReq = request.delegate as? LoginRequest,
                       let authenticator = loginReq.authenticator {
                        logger.debug("Received SSPI challenge (\(sspiToken.data.count) bytes), continuing authentication")
                        do {
                            let (responseToken, _) = try authenticator.continueAuthentication(serverToken: sspiToken.data)
                            if let responseData = responseToken {
                                let sspiRequest = SSPIRequest(tokenData: responseData)
                                let packets = try sspiRequest.start(allocator: ByteBufferAllocator())
                                for packet in packets {
                                    context.write(self.wrapOutboundOut(packet), promise: nil)
                                }
                                context.flush()
                            }
                        } catch {
                            logger.error("SSPI authentication continuation failed: \(error)")
                            cleanupRequest(request, error: error)
                            startNextIfQueued(context: context)
                            return
                        }
                    }
                case .loginAck:
                    loginAckReceived = true
                    logger.info("Received LOGINACK token; connection authenticated.")
                    self.state = .loggedIn
                case .envchange:
                    if let envToken = token as? TDSTokens.EnvchangeToken<[Byte]> {
                        processEnvchangeToken(envToken)
                    }
                default:
                    break
                }
            }

            if loginAckReceived,
               let current = self.currentRequest,
               current === request,
               request.delegate is LoginRequest {
                cleanupRequest(request)
                startNextIfQueued(context: context)
            }
        } catch {
            cleanupRequest(request, error: error)
        }
    }
    
    /// Set the TDSConnection reference after it's created
    internal func setConnection(_ connection: TDSConnection) {
        self.connection = connection
    }

    private func sslKickoff(context: ChannelHandlerContext) throws {
        guard let tlsConfig = tlsConfiguration else {
            throw TDSError.protocolError("Encryption was requested but a TLS Configuration was not provided.")
        }
        
        let sslContext: NIOSSLContext
        let sslHandler: NIOSSLClientHandler
        do {
            sslContext = try NIOSSLContext(configuration: tlsConfig)
            sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
        } catch {
            self.errorCaught(context: context, error: TDSError.sslError("Failed to initialize TLS: \(error)"))
            return
        }
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
            request.completionPromise.fail(error)
            request.resultPromise.fail(error)
        } else {
            request.completionPromise.succeed(())
            request.resultPromise.succeed(request.rows.flatMap { $0.data })
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
                request.completionPromise.succeed(())
                request.resultPromise.succeed([])
                promise?.succeed(())
                return
            } else if loginAlreadyQueued || self.state == .sentLogin {
                // A LOGIN is already in-flight or queued. Coalesce by dropping the new
                // duplicate request before it ever enters the queue. The original LOGIN
                // remains the head and will complete normally.
                self.logger.debug("Dropping duplicate queued LOGIN request")
                request.completionPromise.succeed(())
                request.resultPromise.succeed([])
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
                // Propagate the current transaction descriptor into RawSqlRequest batches so that
                // explicit transactions (BEGIN/COMMIT/ROLLBACK) use a valid MARS header.
                if let connection = self.connection,
                   let raw = request.delegate as? RawSqlRequest {
                    raw.transactionDescriptorOverride = connection.transactionDescriptor
                    raw.outstandingRequestCountOverride = connection.requestCount
                }
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
            current.completionPromise.fail(TDSError.connectionClosed)
            current.resultPromise.fail(TDSError.connectionClosed)
        }
        self.queue = []
    }
    
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.error("TDS pipeline error: \(error.localizedDescription)")
        context.fireErrorCaught(error)
        if !queue.isEmpty {
            let req = queue.removeFirst()
            req.completionPromise.fail(error)
            req.resultPromise.fail(error)
            while !queue.isEmpty {
                let nextReq = queue.removeFirst()
                nextReq.completionPromise.fail(TDSError.connectionClosed)
                nextReq.resultPromise.fail(TDSError.connectionClosed)
            }
        }
    }
    
    
    private func _userInboundEventTriggered(context: ChannelHandlerContext, event: Any) throws {
        self.currentContext = context
        self.currentEventLoop = context.eventLoop
        if sslClientHandler != nil, let sslHandshakeComplete = event as? TLSUserEvent, case .handshakeCompleted = sslHandshakeComplete {
            let fallbackEventLoop = context.eventLoop
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
                    let newDecoder = ByteToMessageHandler(self.packetDecoder)
                    let newEncoder = MessageToByteHandler(TDSPacketEncoder(logger: self.logger))
                    let ops = pipeline.syncOperations
                    try ops.addHandler(newDecoder, name: self.firstDecoderName, position: .last)
                    try ops.addHandler(newEncoder, name: self.firstEncoderName, position: .last)
                    self.firstDecoder = newDecoder
                    self.firstEncoder = newEncoder
                    self.pipelineCoordinator = nil
                    guard let eventLoop = self.currentEventLoop ?? self.connection?.eventLoop else {
                        throw TDSError.protocolError("Missing event loop during SSL pipeline reconfiguration")
                    }
                    return eventLoop.makeSucceededFuture(())
                } catch {
                    if let eventLoop = self.currentEventLoop ?? self.connection?.eventLoop {
                        return eventLoop.makeFailedFuture(error)
                    }
                    return fallbackEventLoop.makeFailedFuture(error)
                }
            }
            
            future.whenSuccess { _ in
                self.logger.debug("Done w/ SSL Handshake and pipeline organization")
                self.state = .sslHandshakeComplete
                if let request = self.currentRequest {
                    self.cleanupRequest(request)
                    // Kick off the next queued request (LOGIN) immediately after PRELOGIN completes
                    if let currentContext = self.currentContext {
                        self.startNextIfQueued(context: currentContext)
                    }
                }
            }
            
            future.whenFailure { error in
                if let currentContext = self.currentContext {
                    self.errorCaught(context: currentContext, error: error)
                }
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
                    current.completionPromise.fail(TDSError.protocolError("request timeout"))
                    current.resultPromise.fail(TDSError.protocolError("request timeout"))
                }
                promise?.succeed(())
                return
            }
        }
        promise?.succeed(())
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.currentContext = nil
        self.currentEventLoop = nil
        logger.debug("TDSRequestHandler.channelInactive: draining \(queue.count) pending requests; pipelineState=\(pipelineCoordinator?.stateDescription ?? "<nil>")")
        // Fail any pending TLS handshake output promise to avoid leaking promises on loop shutdown.
        pipelineCoordinator?.failHandshakeIfPending()
        while !queue.isEmpty {
            let req = queue.removeFirst()
            req.completionPromise.fail(TDSError.connectionClosed)
            req.resultPromise.fail(TDSError.connectionClosed)
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
            req.completionPromise.fail(TDSError.connectionClosed)
            req.resultPromise.fail(TDSError.connectionClosed)
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
            // Explicitly request reading to handle multi-packet responses
            context.read()
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
}
