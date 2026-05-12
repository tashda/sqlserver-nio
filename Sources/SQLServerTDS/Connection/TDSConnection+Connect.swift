import Logging
import NIO
import NIOSSL
import Foundation

extension TDSConnection {
    /// Note about TLS Support:
    ///
    /// If a `TLSConfiguration` is provided, it will be used to negotiate encryption, signaling to the server that encryption is enabled (ENCRYPT_ON).
    /// If no `TLSConfiguration` is provided, it is assumed that a standard configuration will work and signals to the server that encryption is enabled (ENCRYPT_ON).
    /// If the user explicitly passes a nil `TLSconfiguration`, it will signal to the server that encryption is not supported (ENCRYPT_NOT_SUP).
    ///
    /// Supporting the case for only encrypting login packets provides little benefit and makes it impossible to provide a default (valid) TLSConfiguration.
    public static func connect(
        to socketAddress: SocketAddress,
        tlsConfiguration: TLSConfiguration? = .makeClientConfiguration(),
        serverHostname: String? = nil,
        encryptionMode: TDSEncryptionMode = .optional,
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
        tlsConfiguration: TLSConfiguration? = .makeClientConfiguration(),
        serverHostname: String? = nil,
        encryptionMode: TDSEncryptionMode = .optional,
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
        tlsConfiguration: TLSConfiguration? = .makeClientConfiguration(),
        serverHostname: String? = nil,
        encryptionMode: TDSEncryptionMode = .optional,
        connectTimeout: TimeAmount = .seconds(10),
        on eventLoop: EventLoop,
        logger: Logger
    ) -> EventLoopFuture<TDSConnection> {
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
            do {
                let ops = channel.pipeline.syncOperations
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
            return channel.eventLoop.makeSucceededFuture(connection)
        }.flatMap { (conn: TDSConnection) -> EventLoopFuture<TDSConnection> in
            let attemptedTLS = tlsConfiguration != nil
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
        switch error {
        case NIOSSLError.uncleanShutdown:
            // SQL Server with "optional" encryption uses TLS only during login,
            // then drops to raw TDS without sending a TLS close_notify. The NIO
            // SSL handler interprets subsequent unencrypted data as an unclean
            // shutdown. This is expected and should not close the connection.
            logger.debug("SSL unclean shutdown (expected with SQL Server optional encryption)")
        default:
            self.logger.error("Uncaught error: \(error)")
            context.close(promise: nil)
            context.fireErrorCaught(error)
        }
    }
}
