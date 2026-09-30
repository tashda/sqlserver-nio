import Logging
import NIO
import NIOSSL
import NIOTLS
import Foundation

extension TDSConnection {
    public static func defaultTLSConfiguration() -> TLSConfiguration {
        var configuration = TLSConfiguration.makeClientConfiguration()
        configuration.minimumTLSVersion = .tlsv12
        return configuration
    }

    /// Note about TLS Support:
    ///
    /// If a `TLSConfiguration` is provided, it will be used to negotiate encryption, signaling to the server that encryption is enabled (ENCRYPT_ON).
    /// A TLS configuration is required. The driver refuses an unencrypted login
    /// because LOGIN7 password obfuscation does not protect credentials.
    /// Strict/TDS 8.0 establishes TLS before sending PRELOGIN.
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = TDSConnection.defaultTLSConfiguration(),
        serverHostname: String? = nil,
        encryptionMode: TDSEncryptionMode = .mandatory,
        connectTimeout: TimeAmount = .seconds(10),
        on eventLoop: EventLoop
    ) -> EventLoopFuture<TDSConnection> {
        connect(
            to: socketAddress,
            tlsConfiguration: tlsConfiguration,
            serverHostname: serverHostname,
            encryptionMode: encryptionMode,
            connectTimeout: connectTimeout,
            on: eventLoop,
            logger: Logger(label: "swift-tds")
        )
    }

    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = TDSConnection.defaultTLSConfiguration(),
        serverHostname: String? = nil,
        encryptionMode: TDSEncryptionMode = .mandatory,
        on eventLoop: EventLoop,
        logger: Logger
    ) -> EventLoopFuture<TDSConnection> {
        connect(
            to: socketAddress,
            tlsConfiguration: tlsConfiguration,
            serverHostname: serverHostname,
            encryptionMode: encryptionMode,
            connectTimeout: .seconds(10),
            on: eventLoop,
            logger: logger
        )
    }

    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = TDSConnection.defaultTLSConfiguration(),
        serverHostname: String? = nil,
        encryptionMode: TDSEncryptionMode = .mandatory,
        connectTimeout: TimeAmount = .seconds(10),
        on eventLoop: EventLoop,
        logger: Logger
    ) -> EventLoopFuture<TDSConnection> {
        guard let tlsConfiguration else {
            return eventLoop.makeFailedFuture(TDSError.protocolError("A TLS configuration is required; unencrypted login is not supported"))
        }
        switch tlsConfiguration.minimumTLSVersion {
        case .tlsv1, .tlsv11:
            return eventLoop.makeFailedFuture(TDSError.sslError("TLS 1.2 or newer is required"))
        case .tlsv12, .tlsv13:
            break
        }
        if tlsConfiguration.certificateVerification == .fullVerification,
           (serverHostname == nil || serverHostname?.isEmpty == true) {
            return eventLoop.makeFailedFuture(TDSError.sslError("Certificate verification requires a server hostname"))
        }
        if case .strict = encryptionMode {
            guard tlsConfiguration.certificateVerification == .fullVerification,
                  let serverHostname, !serverHostname.isEmpty else {
                return eventLoop.makeFailedFuture(TDSError.protocolError("Strict encryption requires a hostname and full server certificate verification"))
            }
        }
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .connectTimeout(connectTimeout)

        let firstDecoderName = "tds.firstDecoder"
        let firstEncoderName = "tds.firstEncoder"
        let requestHandlerName = "tds.requestHandler"
        let errorHandlerName = "tds.errorHandler"
        let pipelineCoordinatorName = "tds.pipelineCoordinator"
        logger.info("TDS channel connecting to \(socketAddress)")
        return bootstrap.connect(to: socketAddress).flatMap { (channel: Channel) -> EventLoopFuture<TDSConnection> in
            channel.eventLoop.assertInEventLoop()
            let firstDecoder = ByteToMessageHandler(TDSPacketDecoder(logger: logger))
            let firstEncoder = MessageToByteHandler(TDSPacketEncoder(logger: logger))
            let requestHandler = TDSRequestHandler(
                logger: logger,
                firstDecoder: firstDecoder,
                firstEncoder: firstEncoder,
                tlsConfiguration: tlsConfiguration,
                serverHostname: serverHostname,
                firstDecoderName: firstDecoderName,
                firstEncoderName: firstEncoderName,
                pipelineCoordinatorName: pipelineCoordinatorName
            )
            let errorHandler = TDSErrorHandler(logger: logger)
            var strictHandshake: EventLoopFuture<Void>?
            do {
                let ops = channel.pipeline.syncOperations
                if case .strict = encryptionMode {
                    var strictConfiguration = tlsConfiguration
                    strictConfiguration.applicationProtocols = ["tds/8.0"]
                    let context = try NIOSSLContext(configuration: strictConfiguration)
                    let tlsHandler = try NIOSSLClientHandler(context: context, serverHostname: serverHostname)
                    let observer = TDSStrictHandshakeObserver(on: channel.eventLoop)
                    try ops.addHandler(tlsHandler, name: "tds.strictTLS")
                    try ops.addHandler(observer, name: "tds.strictHandshake")
                    let timeout = channel.eventLoop.scheduleTask(in: connectTimeout) {
                        observer.fail(TDSError.sslError("TDS 8.0 TLS handshake timed out"))
                        channel.close(promise: nil)
                    }
                    strictHandshake = observer.future.always { _ in timeout.cancel() }
                }
                try ops.addHandler(firstDecoder, name: firstDecoderName)
                try ops.addHandler(firstEncoder, name: firstEncoderName)
                try ops.addHandler(requestHandler, name: requestHandlerName)
                try ops.addHandler(errorHandler, name: errorHandlerName)
            } catch {
                return channel.close().flatMap {
                    channel.eventLoop.makeFailedFuture(error)
                }
            }
            let connection = TDSConnection(
                channel: channel,
                requestHandler: requestHandler,
                tlsConfiguration: tlsConfiguration,
                serverHostname: serverHostname,
                firstDecoderName: firstDecoderName,
                firstEncoderName: firstEncoderName,
                pipelineCoordinatorName: pipelineCoordinatorName,
                logger: logger
            )

            // Set the connection reference in the request handler for ENVCHANGE token processing
            requestHandler.setConnection(connection)

            // Start reading immediately to handle multi-packet responses
            channel.read()
            logger.info("TDS channel created to \(socketAddress)")
            if let strictHandshake {
                return strictHandshake.flatMapError { error in
                    channel.close().flatMapThrowing { throw error }
                }.map { connection }
            }
            return channel.eventLoop.makeSucceededFuture(connection)
        }.flatMap { (conn: TDSConnection) -> EventLoopFuture<TDSConnection> in
            let attemptedTLS = true
            return conn.prelogin(encryptionMode: encryptionMode, hasTLSConfiguration: attemptedTLS)
                .flatMapError { error in
                    let translated = translatePreloginError(error, attemptedTLS: attemptedTLS)
                    return conn.close().flatMap {
                        conn.channel.eventLoop.makeFailedFuture(translated)
                    }
                }.map { conn }
        }
    }
}

/// Observes the outer TLS handshake used by TDS 8.0. TDS packet handlers are
/// installed after this handler and only see authenticated plaintext.
private final class TDSStrictHandshakeObserver: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let promise: EventLoopPromise<Void>
    private var completed = false
    var future: EventLoopFuture<Void> { promise.futureResult }

    init(on eventLoop: EventLoop) {
        promise = eventLoop.makePromise(of: Void.self)
    }

    func fail(_ error: Error) {
        guard !completed else { return }
        completed = true
        promise.fail(error)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case .some(.handshakeCompleted(let negotiatedProtocol)) = event as? TLSUserEvent {
            if let negotiatedProtocol, negotiatedProtocol != "tds/8.0" {
                fail(TDSError.sslError("Server negotiated unexpected TLS application protocol \(negotiatedProtocol)"))
                context.close(promise: nil)
            } else if !completed {
                completed = true
                promise.succeed(())
            }
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.fireChannelRead(data)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error)
        context.fireErrorCaught(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        fail(TDSError.connectionClosed)
        context.fireChannelInactive()
    }
}

/// Translates raw NIOSSL handshake failures that occur during PRELOGIN into a
/// `TDSError.sslError` with an actionable message. Without this, a self-signed
/// or otherwise-untrusted server certificate surfaces to the caller as the
/// opaque string "uncleanShutdown" — which gives the user no hint that they
/// can enable `trustServerCertificate` to bypass verification.
internal func translatePreloginError(_ error: Error, attemptedTLS: Bool) -> Error {
    guard attemptedTLS else { return error }

    if let sslError = error as? NIOSSLError {
        switch sslError {
        case .handshakeFailed(let reason):
            return TDSError.sslError(
                "TLS handshake failed: \(reason). If the server uses a self-signed or internal-CA certificate, enable 'Trust Server Certificate' to connect anyway."
            )
        case .uncleanShutdown:
            // During PRELOGIN, an unclean shutdown almost always means the
            // server (or our own SSL handler) tore down the connection because
            // the certificate could not be verified. The peer often closes
            // without sending close_notify, which is what produces this error.
            return TDSError.sslError(
                "TLS handshake aborted by peer (unclean shutdown). The server certificate is likely not trusted by the system. Enable 'Trust Server Certificate' to connect anyway."
            )
        default:
            return TDSError.sslError("TLS error during handshake: \(sslError)")
        }
    }
    return error
}

private final class TDSErrorHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    let logger: Logger
    init(logger: Logger) {
        self.logger = logger
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.error("Uncaught error: \(error)")
        context.close(promise: nil)
        context.fireErrorCaught(error)
    }
}
