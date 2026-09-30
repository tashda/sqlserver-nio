@preconcurrency import NIO
import NIOConcurrencyHelpers
@preconcurrency import NIOSSL
@preconcurrency import NIOTLS
import Logging

public enum TDSUserEvent: Sendable {
    /// Cancels the request currently executing on the server, if any.
    case attention
    /// Cancels the executing request and fails it with a timeout error.
    case failCurrentRequestTimeout
}

/// A handle to one submitted request.
///
/// `future` completes exactly once: when the server finishes the response,
/// when the request fails, or after a cancellation has been acknowledged by
/// the server. Cancellation of a request that has already completed, or that
/// belongs to another request, is ignored.
public final class TDSRequestHandle: @unchecked Sendable {
    let context: TDSRequestContext
    private weak var connection: TDSConnection?
    public let future: EventLoopFuture<Void>

    init(context: TDSRequestContext, connection: TDSConnection) {
        self.context = context
        self.connection = connection
        self.future = context.completionPromise.futureResult
    }

    /// Cancels this request. A request that has not been sent yet is removed
    /// from the queue; a request executing on the server is cancelled with a
    /// TDS ATTENTION and fails with `TDSError.cancelled` once the server
    /// acknowledges it. If the acknowledgement does not arrive in time, the
    /// connection is closed because its protocol state is unknown.
    public func cancel() {
        guard let connection else { return }
        connection.eventLoop.execute {
            connection.requestHandler.cancel(self.context, reason: TDSError.cancelled)
        }
    }

    /// Stops reading from the socket while this request is receiving rows.
    /// Used for streaming back-pressure. Has no effect once the request has
    /// completed, and reading resumes automatically when it completes.
    public func pauseReading() {
        guard let connection else { return }
        connection.eventLoop.execute {
            connection.requestHandler.setReadPaused(true, for: self.context)
        }
    }

    public func resumeReading() {
        guard let connection else { return }
        connection.eventLoop.execute {
            connection.requestHandler.setReadPaused(false, for: self.context)
        }
    }
}

extension TDSConnection: TDSClient {
    public func send(_ request: TDSRequest, logger: Logger) -> EventLoopFuture<Void> {
        start(request, timeout: nil).future
    }

    /// Submits a request and returns a handle that can cancel it.
    ///
    /// - Parameter timeout: Maximum time the request may run on the server,
    ///   measured from when it is sent. On expiry the request is cancelled and
    ///   fails with `TDSError.requestTimeout`.
    public func start(_ request: TDSRequest, timeout: TimeAmount?) -> TDSRequestHandle {
        request.log(to: self.logger)
        let completionPromise: EventLoopPromise<Void> = self.channel.eventLoop.makePromise()
        let resultPromise: EventLoopPromise<[TDSData]>
        if let rawSqlRequest = request as? RawSqlRequest, let existingPromise = rawSqlRequest.resultPromise {
            resultPromise = existingPromise
        } else {
            resultPromise = self.channel.eventLoop.makePromise()
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
        context.timeout = timeout
        // The request handler fails every queued or active request when the
        // channel closes, so no per-request close callback is registered here.
        // Such callbacks would accumulate for the lifetime of the connection.
        self.channel.writeAndFlush(context).whenFailure { context.fail($0) }
        return TDSRequestHandle(context: context, connection: self)
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
    /// When true, the first packet carries the RESETCONNECTION status bit so
    /// the server resets session state before running the request.
    var resetsConnection: Bool { get }
}

extension TDSRequest {
    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        var buffer = allocator.buffer(capacity: TDSPacket.maximumPacketDataLength)
        try self.serialize(into: &buffer)
        return try TDSMessage(from: &buffer, ofType: self.packetType, allocator: allocator).packets
    }

    public var stream: Bool { false }
    public var storesRowsInContext: Bool { false }
    public var resetsConnection: Bool { false }
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
    var started: Bool = false
    var rows: [TDSRow] = []
    var timeout: TimeAmount?
    var timeoutTask: Scheduled<Void>?
    /// Set once a cancellation (user request or timeout) has been sent to the
    /// server. The request fails with this error when the server acknowledges.
    var cancellationError: Error?
    var readPaused = false
    var loginAckReceived = false
    private let completionLock = NIOLock()
    private var completed = false

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

    var isCompleted: Bool { completionLock.withLock { completed } }

    private func claimCompletion() -> Bool {
        completionLock.withLock {
            guard !completed else { return false }
            completed = true
            return true
        }
    }

    func succeed(_ rows: [TDSData]) {
        guard claimCompletion() else { return }
        timeoutTask?.cancel()
        completionPromise.succeed(())
        resultPromise.succeed(rows)
    }

    func fail(_ error: Error) {
        guard claimCompletion() else { return }
        timeoutTask?.cancel()
        completionPromise.fail(error)
        resultPromise.fail(error)
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

/// Owns the request lifecycle of one TDS connection.
///
/// TDS without MARS carries exactly one request at a time. The handler keeps
/// submitted requests in `queue`, sends the head when the connection is free,
/// and routes response tokens to the single `active` request. All state is
/// confined to the channel's event loop.
///
/// Cancellation follows MS-TDS 2.2.1.7: the client sends ATTENTION and then
/// discards the response until a DONE token with the DONE_ATTN bit arrives.
/// No request is sent while an acknowledgement is outstanding, so a late
/// acknowledgement can never be attributed to a later request. If the
/// acknowledgement does not arrive in time, the connection is closed.
final class TDSRequestHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = TDSPacketChunk
    typealias OutboundIn = TDSRequestContext
    typealias OutboundOut = TDSPacket

    /// How long to wait for the server to acknowledge an ATTENTION before the
    /// connection is considered unusable and closed.
    static let defaultAttentionAcknowledgementTimeout: TimeAmount = .seconds(15)

    var firstDecoder: ByteToMessageHandler<TDSPacketDecoder>
    var firstEncoder: MessageToByteHandler<TDSPacketEncoder>
    var tlsConfiguration: TLSConfiguration?
    var serverHostname: String?
    /// SQL Server product major version captured from the LOGINACK token (e.g. 10 for 2008 R2, 13 for 2016, 16 for 2022).
    /// Zero until a LOGINACK has been received.
    internal var serverMajorVersion: UInt8 = 0
    var attentionAcknowledgementTimeout: TimeAmount = TDSRequestHandler.defaultAttentionAcknowledgementTimeout
    private let firstDecoderName: String
    private let firstEncoderName: String
    private let pipelineCoordinatorName: String

    var sslClientHandler: NIOSSLClientHandler?

    private let packetDecoder: TDSPacketDecoder
    private let streamParser: TDSStreamParser
    private var tokenParser: TDSTokenOperations
    /// True when the most recently received chunk ended a TDS message.
    private var atEndOfMessage = true
    /// Buffered byte count below which parsing is deferred for a large
    /// incomplete token. See `shouldParse()`.
    private var reparseThreshold = 0

    var pipelineCoordinator: PipelineOrganizationHandler!
    private var handlerContext: ChannelHandlerContext?

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

    /// Requests submitted but not yet sent.
    private var queue = CircularBuffer<TDSRequestContext>()
    /// The request whose response is currently being received.
    private var active: TDSRequestContext?
    /// Set while an ATTENTION has been sent and its acknowledgement is pending.
    private var attentionAckTimeout: Scheduled<Void>?
    private var attentionPending: Bool { attentionAckTimeout != nil }
    /// A read requested by the channel while reads were paused.
    private var pendingRead = false
    private var isFailed = false

    let logger: Logger

    var currentRequest: TDSRequestContext? { active }

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
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.tlsConfiguration = tlsConfiguration
        self.serverHostname = serverHostname
        self.firstDecoderName = firstDecoderName
        self.firstEncoderName = firstEncoderName
        self.pipelineCoordinatorName = pipelineCoordinatorName
        self.connection = connection
        self.packetDecoder = TDSPacketDecoder(logger: logger)
        self.streamParser = TDSStreamParser()
        self.tokenParser = TDSTokenOperations(streamParser: streamParser, logger: logger)
        self.firstDecoder = ByteToMessageHandler(packetDecoder)
    }

    /// Set the TDSConnection reference after it's created
    internal func setConnection(_ connection: TDSConnection) {
        self.connection = connection
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.handlerContext = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.handlerContext = nil
    }

    // MARK: - Outbound

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = self.unwrapOutboundIn(data)
        promise?.succeed(())
        guard !isFailed, context.channel.isActive else {
            request.fail(TDSError.connectionClosed)
            return
        }
        if request.delegate is LoginRequest, state == .loggedIn {
            // Login is coalesced by TDSConnection; a second LOGIN7 on an
            // authenticated session would be rejected by the server.
            request.succeed([])
            return
        }
        queue.append(request)
        startNextIfPossible(context: context)
    }

    func read(context: ChannelHandlerContext) {
        if let active, active.readPaused, !attentionPending {
            pendingRead = true
            return
        }
        context.read()
    }

    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        failAll(TDSError.connectionClosed)
        context.close(mode: mode, promise: promise)
    }

    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        guard let event = event as? TDSUserEvent else {
            context.triggerUserOutboundEvent(event, promise: promise)
            return
        }
        switch event {
        case .attention:
            if let active { cancel(active, reason: TDSError.cancelled) }
        case .failCurrentRequestTimeout:
            if let active { cancel(active, reason: TDSError.requestTimeout("request timeout")) }
        }
        promise?.succeed(())
    }

    // MARK: - Request control

    func cancel(_ request: TDSRequestContext, reason: Error) {
        guard !request.isCompleted else { return }
        if let index = queue.firstIndex(where: { $0 === request }) {
            // Not sent yet: nothing to cancel on the server.
            queue.remove(at: index)
            request.fail(reason)
            return
        }
        guard request === active, request.cancellationError == nil else { return }
        request.cancellationError = reason
        request.readPaused = false
        request.timeoutTask?.cancel()
        sendAttention()
    }

    func setReadPaused(_ paused: Bool, for request: TDSRequestContext) {
        guard request === active, !request.isCompleted else { return }
        request.readPaused = paused
        if !paused { flushPendingRead() }
    }

    private func flushPendingRead() {
        guard pendingRead, let context = handlerContext else { return }
        pendingRead = false
        context.read()
    }

    private func sendAttention() {
        guard !attentionPending, let context = handlerContext, context.channel.isActive else { return }
        var empty = context.channel.allocator.buffer(capacity: 0)
        let packet = TDSPacket(
            from: &empty,
            ofType: .attentionSignal,
            isLastPacket: true,
            packetId: 1,
            allocator: context.channel.allocator
        )
        context.writeAndFlush(self.wrapOutboundOut(packet), promise: nil)
        scheduleAttentionAcknowledgementTimeout(context: context)
        logger.debug("ATTENTION sent; discarding response until acknowledgement")
        flushPendingRead()
    }

    /// The acknowledgement follows everything the server had already sent,
    /// which can be a lot after cancelling a large result. The deadline
    /// therefore measures silence: it restarts whenever data arrives, and
    /// only a server that stops sending is treated as unresponsive.
    private func scheduleAttentionAcknowledgementTimeout(context: ChannelHandlerContext) {
        attentionAckTimeout?.cancel()
        let deadline = attentionAcknowledgementTimeout
        attentionAckTimeout = context.eventLoop.scheduleTask(in: deadline) { [weak self] in
            guard let self, let context = self.handlerContext else { return }
            self.logger.warning("SQL Server sent nothing for \(deadline) while a cancellation was pending; closing connection")
            self.fatal(TDSError.protocolError("cancellation was not acknowledged by the server"), context: context)
        }
    }

    private func startNextIfPossible(context: ChannelHandlerContext) {
        guard active == nil, !attentionPending, !isFailed, context.channel.isActive else { return }
        guard let next = queue.popFirst() else { return }
        guard !next.isCompleted else {
            startNextIfPossible(context: context)
            return
        }
        active = next
        next.started = true
        resetResponseParser()
        if next.delegate is RawSqlRequest || next.delegate is RpcRequest {
            connection?.updateDataClassificationPayload([])
        }
        do {
            if let connection = self.connection, let raw = next.delegate as? RawSqlRequest {
                // Explicit transactions (BEGIN/COMMIT/ROLLBACK) must carry the
                // current descriptor in the ALL_HEADERS block.
                raw.transactionDescriptorOverride = connection.transactionDescriptor
                raw.outstandingRequestCountOverride = connection.requestCount
            }
            var packets = try next.delegate.start(allocator: context.channel.allocator)
            try trackState(for: packets.first?.type)
            let reset = next.delegate.resetsConnection || (connection?.consumeConnectionResetRequest() ?? false)
            if reset, !packets.isEmpty {
                // MS-TDS 2.2.3.1.2: RESETCONNECTION is carried by the first
                // packet of a request message.
                packets[0].applyResetConnectionFlag()
            }
            if let timeout = next.timeout {
                next.timeoutTask = context.eventLoop.scheduleTask(in: timeout) { [weak self, weak next] in
                    guard let self, let next else { return }
                    self.cancel(next, reason: TDSError.requestTimeout("request exceeded its \(Self.describe(timeout)) timeout"))
                }
            }
            for packet in packets {
                context.write(self.wrapOutboundOut(packet), promise: nil)
            }
            context.flush()
        } catch {
            clearActive()
            next.fail(error)
            startNextIfPossible(context: context)
        }
    }

    private static func describe(_ amount: TimeAmount) -> String {
        let seconds = Double(amount.nanoseconds) / 1_000_000_000
        return seconds == seconds.rounded() ? "\(Int(seconds))s" : String(format: "%.3fs", seconds)
    }

    private func trackState(for type: TDSPacket.HeaderType?) throws {
        switch type {
        case .some(.prelogin):
            guard state == .start else {
                throw TDSError.protocolError("PRELOGIN may only be sent once per connection.")
            }
            state = .sentPrelogin
        case .some(.tds7Login):
            state = .sentLogin
        default:
            break
        }
    }

    // MARK: - Inbound

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let chunk = self.unwrapInboundIn(data)
        do {
            try handle(chunk, context: context)
        } catch {
            fatal(error, context: context)
        }
    }

    private func handle(_ chunk: TDSPacketChunk, context: ChannelHandlerContext) throws {
        if let active, let prelogin = active.delegate as? PreloginRequest, !attentionPending {
            let response = try prelogin.handle(dataStream: chunk.payload, isEndOfMessage: chunk.isEndOfMessage)
            switch response {
            case .done:
                complete(active, context: context)
            case .continue:
                break
            case .respond(let packets):
                for packet in packets { context.write(self.wrapOutboundOut(packet), promise: nil) }
                context.flush()
            case .kickoffSSL:
                try sslKickoff(context: context)
            }
            return
        }

        guard active != nil || attentionPending else {
            // Without MARS the server only speaks in response to a request.
            // Unsolicited bytes mean the stream is no longer aligned.
            throw TDSError.protocolError("Received \(chunk.payload.readableBytes) unexpected bytes with no request outstanding")
        }

        if attentionPending {
            scheduleAttentionAcknowledgementTimeout(context: context)
        }
        var payload = chunk.payload
        streamParser.buffer.writeBuffer(&payload)
        atEndOfMessage = chunk.isEndOfMessage
        guard shouldParse() else { return }
        try processTokens(context: context)
    }

    /// A large token that has not fully arrived is re-examined only after the
    /// buffered data has doubled, so a multi-megabyte value spread across
    /// thousands of packets is decoded in linear rather than quadratic time.
    private func shouldParse() -> Bool {
        atEndOfMessage || streamParser.buffer.writerIndex - streamParser.position >= reparseThreshold
    }

    private func processTokens(context: ChannelHandlerContext) throws {
        let (tokens, error) = tokenParser.parseAvailable(messageComplete: atEndOfMessage)
        for token in tokens {
            try dispatch(token, context: context)
            if isFailed { return }
        }
        if let error { throw error }

        let remaining = streamParser.buffer.writerIndex - streamParser.position
        if remaining == 0 {
            streamParser.buffer.clear()
            streamParser.position = streamParser.buffer.readerIndex
            reparseThreshold = 0
        } else {
            if atEndOfMessage {
                throw TDSError.protocolError("TDS message ended inside a token (\(remaining) bytes left)")
            }
            if streamParser.position > streamParser.buffer.readerIndex {
                // Drop consumed bytes so a long result does not accumulate.
                var rest = streamParser.buffer.getSlice(at: streamParser.position, length: remaining)!
                var compacted = context.channel.allocator.buffer(capacity: max(remaining * 2, 4096))
                compacted.writeBuffer(&rest)
                streamParser.buffer = compacted
                streamParser.position = compacted.readerIndex
            }
            reparseThreshold = remaining >= 64 * 1024 ? remaining * 2 : 0
        }
    }

    private func dispatch(_ token: TDSToken, context: ChannelHandlerContext) throws {
        // While a cancellation is outstanding, the rest of the response is
        // discarded (MS-TDS 2.2.1.7). Environment changes are still applied
        // because the server may have rolled back an open transaction.
        let discarding = attentionPending
        let request = active

        switch token.type {
        case .done, .doneInProc, .doneProc:
            let done = token as! TDSTokens.DoneToken
            let isAttentionAck = done.status & 0x0020 != 0
            let isFinal = done.status & 0x0001 == 0
            logger.trace("DONE type=\(token.type) status=0x\(String(format: "%04X", done.status)) rows=\(done.doneRowCount)")
            if discarding {
                if isAttentionAck {
                    acknowledgeAttention(context: context)
                } else if isFinal, let request {
                    // The request finished before the server saw ATTENTION.
                    // Its acknowledgement follows in a separate message.
                    clearActive()
                    fail(request, with: request.cancellationError ?? TDSError.cancelled)
                }
                return
            }
            guard let request else { return }
            request.tokenHandler.onDone(done)
            if isFinal {
                if request.delegate is LoginRequest, !request.loginAckReceived {
                    let message = (request.delegate as? LoginRequest)?.serverErrorMessage ?? "Login failed"
                    clearActive()
                    request.fail(TDSError.invalidCredentials(message))
                    startNextIfPossible(context: context)
                } else {
                    complete(request, context: context)
                }
            }

        case .envchange:
            if let envToken = token as? TDSTokens.EnvchangeToken<[Byte]> {
                processEnvchangeToken(envToken)
            } else if let envToken = token as? TDSTokens.EnvchangeToken<String> {
                processEnvchangeToken(envToken)
            }

        case .loginAck:
            if let ackToken = token as? TDSTokens.LoginAckToken {
                self.serverMajorVersion = ackToken.majorVer
            }
            request?.loginAckReceived = true
            state = .loggedIn
            logger.debug("Received LOGINACK; connection authenticated")

        case .dataClassification:
            if !discarding, let classification = token as? TDSTokens.DataClassificationToken {
                var payload = classification.payload
                connection?.updateDataClassificationPayload(payload.readBytes(length: payload.readableBytes) ?? [])
            }

        case .sessionState:
            if let sessionState = token as? TDSTokens.SessionStateToken {
                var payload = sessionState.payload
                connection?.updateSessionStatePayload(payload.readBytes(length: payload.readableBytes) ?? [])
            }

        default:
            guard !discarding, let request, request.cancellationError == nil else { return }
            try deliver(token, to: request, context: context)
        }
    }

    private func deliver(_ token: TDSToken, to request: TDSRequestContext, context: ChannelHandlerContext) throws {
        switch token.type {
        case .colMetadata:
            request.tokenHandler.onColMetadata(token as! TDSTokens.ColMetadataToken)
        case .row:
            let rowToken = token as! TDSTokens.RowToken
            if request.delegate.storesRowsInContext {
                request.rows.append(TDSRow(token: rowToken, columns: request.tokenHandler.columns))
            }
            request.tokenHandler.onRow(rowToken)
        case .nbcRow:
            let nbcRowToken = token as! TDSTokens.NbcRowToken
            let rowToken = TDSTokens.RowToken(colMetadata: nbcRowToken.colMetadata, colData: nbcRowToken.colData)
            if request.delegate.storesRowsInContext {
                request.rows.append(TDSRow(token: rowToken, columns: request.tokenHandler.columns))
            }
            request.tokenHandler.onRow(rowToken)
        case .info, .error:
            let messageToken = token as! TDSTokens.ErrorInfoToken
            if token.type == .error, let login = request.delegate as? LoginRequest {
                login.serverErrorMessage = messageToken.messageText
            }
            request.tokenHandler.onMessage(messageToken)
        case .returnValue:
            request.tokenHandler.onReturnValue(token as! TDSTokens.ReturnValueToken)
        case .sspi:
            guard let sspiToken = token as? TDSTokens.SSPIToken,
                  let login = request.delegate as? LoginRequest,
                  let authenticator = login.authenticator else { return }
            logger.debug("Received SSPI challenge (\(sspiToken.data.count) bytes), continuing authentication")
            let (responseToken, _) = try authenticator.continueAuthentication(serverToken: sspiToken.data)
            if let responseData = responseToken {
                let packets = try SSPIRequest(tokenData: responseData).start(allocator: context.channel.allocator)
                for packet in packets {
                    context.write(self.wrapOutboundOut(packet), promise: nil)
                }
                context.flush()
            }
        default:
            break
        }
    }

    private func acknowledgeAttention(context: ChannelHandlerContext) {
        attentionAckTimeout?.cancel()
        attentionAckTimeout = nil
        logger.debug("ATTENTION acknowledged by server")
        if let request = active {
            clearActive()
            fail(request, with: request.cancellationError ?? TDSError.cancelled)
        }
        flushPendingRead()
        resetResponseParser()
        startNextIfPossible(context: context)
    }

    private func complete(_ request: TDSRequestContext, context: ChannelHandlerContext) {
        clearActive()
        if let error = request.cancellationError {
            // Cancelled requests fail even when the response completed first;
            // the acknowledgement is still outstanding.
            request.fail(error)
        } else {
            request.succeed(request.rows.flatMap { $0.data })
        }
        request.rows = []
        startNextIfPossible(context: context)
    }

    private func fail(_ request: TDSRequestContext, with error: Error) {
        request.rows = []
        request.fail(error)
    }

    private func resetResponseParser() {
        streamParser.buffer.clear()
        streamParser.position = streamParser.buffer.readerIndex
        tokenParser = TDSTokenOperations(streamParser: streamParser, logger: logger)
        reparseThreshold = 0
        atEndOfMessage = true
    }

    /// Clears the active request. A read swallowed while that request was
    /// paused is re-issued; otherwise the channel would stop reading.
    private func clearActive() {
        active = nil
        flushPendingRead()
    }

    // MARK: - ENVCHANGE

    private func processEnvchangeToken(_ envToken: TDSTokens.EnvchangeToken<String>) {
        switch envToken.envchangeType {
        case .database:
            connection?.updateCurrentDatabase(envToken.newValue)
        case .packetSize:
            if let size = Int(envToken.newValue), size != TDSPacket.defaultPacketLength {
                logger.warning("Server negotiated packet size \(size); requests are sent in \(TDSPacket.defaultPacketLength)-byte packets")
            }
        default:
            break
        }
    }

    private func processEnvchangeToken(_ envToken: TDSTokens.EnvchangeToken<[Byte]>) {
        guard let connection = self.connection else { return }
        switch envToken.envchangeType {
        case .beginTransaction:
            let descriptor = Array(envToken.newValue.prefix(8))
            if descriptor.count == 8 {
                connection.updateTransactionState(descriptor: descriptor, requestCount: 1)
            }
        case .commitTransaction, .rollbackTransaction, .defectTransaction, .transactionEnded:
            if !connection.transactionDescriptor.allSatisfy({ $0 == 0 }) {
                connection.updateTransactionState(descriptor: [UInt8](repeating: 0, count: 8), requestCount: 1)
            }
        case .resetConnectionAck:
            connection.updateTransactionState(descriptor: [UInt8](repeating: 0, count: 8), requestCount: 1)
        case .routing:
            connection.updateRouting(Self.parseRouting(envToken.newValue))
        default:
            break
        }
    }

    /// Parses an ENVCHANGE type 20 value: USHORT length, BYTE protocol (0 =
    /// TCP), USHORT port, US_VARCHAR server name.
    static func parseRouting(_ bytes: [UInt8]) -> TDSRoutingTarget? {
        guard bytes.count >= 7, bytes[2] == 0 else { return nil }
        let port = Int(UInt16(bytes[3]) | UInt16(bytes[4]) << 8)
        let chars = Int(UInt16(bytes[5]) | UInt16(bytes[6]) << 8)
        let start = 7
        guard bytes.count >= start + chars * 2 else { return nil }
        var units: [UInt16] = []
        units.reserveCapacity(chars)
        for i in 0..<chars {
            units.append(UInt16(bytes[start + i * 2]) | UInt16(bytes[start + i * 2 + 1]) << 8)
        }
        let server = String(decoding: units, as: UTF16.self)
        guard !server.isEmpty, port > 0 else { return nil }
        return TDSRoutingTarget(server: server, port: port)
    }

    // MARK: - Failure

    /// A decode or protocol failure leaves the byte stream in an unknown
    /// position. The connection cannot be reused and is closed.
    private func fatal(_ error: Error, context: ChannelHandlerContext) {
        guard !isFailed else { return }
        logger.error("TDS connection failed: \(error)")
        isFailed = true
        // Report the connection as closed before callers see the failure;
        // closing a TLS channel on a dead network can take seconds.
        connection?.markUnusable()
        failAll(error)
        context.close(promise: nil)
    }

    private func failAll(_ error: Error) {
        attentionAckTimeout?.cancel()
        attentionAckTimeout = nil
        if let active {
            self.active = nil
            fail(active, with: error)
        }
        while let next = queue.popFirst() {
            // Queued requests never reached the server.
            next.fail(TDSError.connectionClosed)
        }
    }

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        if let sslError = error as? NIOSSLError,
           case .uncleanShutdown = sslError,
           !context.channel.isActive,
           active == nil, queue.isEmpty {
            // SQL Server commonly closes TLS without close_notify after the
            // client closes the connection; no request is affected.
            logger.debug("TLS peer closed without close_notify after the TDS channel became inactive")
        } else {
            logger.error("TDS pipeline error: \(error)")
        }
        isFailed = true
        connection?.markUnusable()
        failAll(error)
        context.fireErrorCaught(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("TDS channel inactive; failing \(queue.count + (active == nil ? 0 : 1)) pending request(s)")
        pipelineCoordinator?.failHandshakeIfPending()
        isFailed = true
        failAll(TDSError.connectionClosed)
        context.fireChannelInactive()
    }

    deinit {
        if let active { active.fail(TDSError.connectionClosed) }
        while let next = queue.popFirst() {
            next.fail(TDSError.connectionClosed)
        }
    }

    // MARK: - TLS (TDS 7.x)

    private func sslKickoff(context: ChannelHandlerContext) throws {
        guard let tlsConfig = tlsConfiguration else {
            throw TDSError.protocolError("Encryption was requested but a TLS Configuration was not provided.")
        }

        let sslContext: NIOSSLContext
        let sslHandler: NIOSSLClientHandler
        do {
            sslContext = try NIOSSLContext(configuration: tlsConfig)
            sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: tdsTLSHostnameForSNI(serverHostname))
        } catch {
            throw TDSError.sslError("Failed to initialize TLS: \(error)")
        }
        self.sslClientHandler = sslHandler

        let coordinator = PipelineOrganizationHandler(logger: logger, firstDecoder, firstEncoder, sslHandler)
        self.pipelineCoordinator = coordinator

        let ops = context.channel.pipeline.syncOperations
        try ops.addHandler(coordinator, name: pipelineCoordinatorName, position: .before(self))
        try ops.addHandler(sslHandler, position: .after(coordinator))
        self.state = .sslHandshakeStarted
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        guard sslClientHandler != nil,
              state == .sslHandshakeStarted,
              let tlsEvent = event as? TLSUserEvent,
              case .handshakeCompleted = tlsEvent else {
            context.fireUserInboundEventTriggered(event)
            return
        }

        if let error = TDSCertificateIdentity.verify(handler: sslClientHandler, configuration: tlsConfiguration, expectedHost: serverHostname) {
            fatal(error, context: context)
            return
        }

        // The TLS handshake tunnelled in PRELOGIN packets is complete. Remove
        // the tunnelling handlers and place fresh TDS framing after TLS.
        let pipeline = context.channel.pipeline
        // Remove the encoder before the coordinator so pending outbound TLS
        // bytes are never handed to an encoder that expects TDSPacket.
        let removals = pipeline.removeHandler(name: self.firstEncoderName)
            .flatMap { pipeline.removeHandler(name: self.pipelineCoordinatorName) }
            .flatMap { pipeline.removeHandler(name: self.firstDecoderName) }

        removals.flatMapThrowing {
            let newDecoder = ByteToMessageHandler(self.packetDecoder)
            let newEncoder = MessageToByteHandler(TDSPacketEncoder(logger: self.logger))
            let ops = pipeline.syncOperations
            // Decoder and encoder go between TLS and this handler.
            try ops.addHandler(newDecoder, name: self.firstDecoderName, position: .before(self))
            try ops.addHandler(newEncoder, name: self.firstEncoderName, position: .before(self))
            self.firstDecoder = newDecoder
            self.firstEncoder = newEncoder
            self.pipelineCoordinator = nil
        }.whenComplete { result in
            switch result {
            case .success:
                self.logger.debug("TLS handshake complete; TDS framing moved inside TLS")
                self.state = .sslHandshakeComplete
                if let request = self.active, request.delegate is PreloginRequest {
                    self.complete(request, context: context)
                }
            case .failure(let error):
                self.fatal(error, context: context)
            }
        }
        context.fireUserInboundEventTriggered(event)
    }
}

/// An alternate server named by an ENVCHANGE routing token (MS-TDS 2.2.7.9,
/// type 20). Azure SQL gateways and availability-group listeners with
/// read-only routing use it to redirect a login.
public struct TDSRoutingTarget: Sendable, Equatable {
    public var server: String
    public var port: Int

    public init(server: String, port: Int) {
        self.server = server
        self.port = port
    }
}
