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
        on eventLoop: EventLoop
    ) -> EventLoopFuture<TDSConnection> {
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
        
        let logger = Logger(label: "swift-tds")
        
        let firstDecoderName = "tds.firstDecoder"
        let firstEncoderName = "tds.firstEncoder"
        let requestHandlerName = "tds.requestHandler"
        let errorHandlerName = "tds.errorHandler"
        let pipelineCoordinatorName = "tds.pipelineCoordinator"
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
            let connection = TDSConnection(channel: channel, logger: logger)
            // Start reading immediately to handle multi-packet responses
            channel.read()
            return channel.eventLoop.makeSucceededFuture(connection)
        }.flatMap { (conn: TDSConnection) -> EventLoopFuture<TDSConnection> in
            return conn.prelogin(shouldNegotiateEncryption: tlsConfiguration != nil ? true : false)
                .flatMapError { error in
                    conn.close().flatMap {
                        conn.channel.eventLoop.makeFailedFuture(error)
                    }
                }.map { conn }
        }
    }
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
            // TODO: Verify this is because the server didn't reply with a CLOSE_NOTIFY
            // Ignore this only if the channel is already shut down
//            if context.channel.isActive {
//                fallthrough
//            }
        fallthrough
        default:
            self.logger.error("Uncaught error: \(error)")
            context.close(promise: nil)
            context.fireErrorCaught(error)
        }
    }
}
