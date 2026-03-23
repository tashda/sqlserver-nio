import Foundation
import NIO
import NIOSSL
import NIOTLS
import Logging

/// Wraps TLS handshake data in TDS PRELOGIN packets during the SSL negotiation phase.
///
/// During TDS connection setup with encryption, the TLS handshake is tunneled inside
/// TDS PRELOGIN packets (type 0x12). This handler sits between the TDS packet decoder
/// and the NIOSSLClientHandler to perform the wrapping/unwrapping:
///
/// **Inbound** (server → client): The TDS decoder reassembles PRELOGIN packets into
/// a ByteBuffer payload. This handler forwards that raw TLS data to the SSL handler.
///
/// **Outbound** (client → server): The SSL handler emits raw TLS data. This handler
/// wraps it in TDS PRELOGIN packets before sending to the encoder/network.
///
/// After the SSL handshake completes, this handler is removed from the pipeline
/// by TDSRequestHandler and replaced with fresh decoder/encoder handlers that sit
/// after the SSL handler (so all TDS traffic flows through TLS transparently).
public final class PipelineOrganizationHandler: ChannelDuplexHandler, RemovableChannelHandler {
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = TDSPacket

    let logger: Logger

    /// `TDSMessage` decoders/encoders
    var firstDecoder: ByteToMessageHandler<TDSPacketDecoder>
    var firstEncoder: MessageToByteHandler<TDSPacketEncoder>
    var secondEncoder: MessageToByteHandler<TDSPacketEncoder>?
    var secondDecoder: ByteToMessageHandler<TDSPacketDecoder>?
    var sslClientHandler: NIOSSLClientHandler

    enum State {
        case start
        case sslHandshake(SSLHandshakeState)
        case allDone
    }

    var state = State.start
    private var tlsHandshakeStart: Date?

    public init(
        logger: Logger,
        _ firstDecoder: ByteToMessageHandler<TDSPacketDecoder>,
        _ firstEncoder: MessageToByteHandler<TDSPacketEncoder>,
        _ sslClientHandler: NIOSSLClientHandler
    ) {
        self.logger = logger
        self.firstDecoder = firstDecoder
        self.firstEncoder = firstEncoder
        self.sslClientHandler = sslClientHandler
    }

    // Inbound: decoder already reassembles PRELOGIN packets into a ByteBuffer payload.
    // We just forward the raw TLS bytes to the SSL handler.
    private func _channelRead(context: ChannelHandlerContext, data: NIOAny) throws {
        switch self.state {
        case .sslHandshake(var sslHandshakeState):
            let buffer = self.unwrapInboundIn(data)
            sslHandshakeState.addReceivedData(buffer)
            logger.debug("Forwarding \(buffer.readableBytes) bytes of TLS data from PRELOGIN response")
            self.state = .sslHandshake(sslHandshakeState)
            context.fireChannelRead(self.wrapInboundOut(sslHandshakeState.inputBuffer))
            sslHandshakeState.inputBuffer.clear()
            state = .sslHandshake(sslHandshakeState)
        default:
            break
        }
    }

    // Outbound: wrap raw TLS data from the SSL handler into TDS PRELOGIN packets.
    private func _write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) throws {
        switch self.state {
        case .start:
            tlsHandshakeStart = Date()
            logger.debug("TLS handshake started")
            let sslHandshakeState = SSLHandshakeState(
                inputBuffer: context.channel.allocator.buffer(capacity: 1024),
                outputBuffer: context.channel.allocator.buffer(capacity: 1024),
                outputPromise: PromiseTracker.makeTrackedPromise(on: context.eventLoop, label: "PipelineHandshake.output")
            )
            updateSSLHandshakeState(sslHandshakeState, data: data, promise: promise)
        case .sslHandshake(let sslHandshakeState):
            updateSSLHandshakeState(sslHandshakeState, data: data, promise: promise)
        default:
            break
        }
    }

    private func _flush(context: ChannelHandlerContext) throws {
        switch self.state {
        case .sslHandshake(var sslHandshakeState):
            let message = try TDSMessage(from: &sslHandshakeState.outputBuffer, ofType: .prelogin, allocator: context.channel.allocator)
            for packet in message.packets {
                context.write(self.wrapOutboundOut(packet), promise: sslHandshakeState.outputPromise)
            }
            context.flush()
            sslHandshakeState.outputBuffer.clear()
            state = .sslHandshake(sslHandshakeState)
            logger.debug("Flushed Prelogin TLS message")
        default:
            context.flush()
        }
    }

    private func updateSSLHandshakeState(_ sslHandshakeState: SSLHandshakeState, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let recievedBuffer = self.unwrapOutboundIn(data)
        var handshakeState = sslHandshakeState
        handshakeState.addPendingOutputData(recievedBuffer)
        handshakeState.outputPromise.futureResult.cascade(to: promise)
        self.state = .sslHandshake(handshakeState)
    }

    public func removeHandler(context: ChannelHandlerContext, removalToken: ChannelHandlerContext.RemovalToken) {
        // Drain any buffered outbound TLS data before removal to prevent raw
        // IOData from being forwarded to the next handler (firstEncoder) which
        // expects TDSPacket.
        switch self.state {
        case .sslHandshake(var sslHandshakeState):
            if sslHandshakeState.outputBuffer.readableBytes > 0 {
                do {
                    let message = try TDSMessage(from: &sslHandshakeState.outputBuffer, ofType: .prelogin, allocator: context.channel.allocator)
                    for packet in message.packets {
                        context.write(self.wrapOutboundOut(packet), promise: nil)
                    }
                    context.flush()
                } catch {
                    logger.debug("Failed to flush pending TLS data during handler removal: \(error)")
                }
                sslHandshakeState.outputBuffer.clear()
                state = .sslHandshake(sslHandshakeState)
            }
        default:
            break
        }
        self.state = .allDone
        if let start = tlsHandshakeStart {
            let elapsed = Date().timeIntervalSince(start)
            let elapsedMs = String(format: "%.1fms", elapsed * 1000)
            logger.debug("TLS handshake completed in \(elapsedMs)")
        }
        context.leavePipeline(removalToken: removalToken)
    }

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        do {
            try self._channelRead(context: context, data: data)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }

    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        do {
            try self._write(context: context, data: data, promise: promise)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }

    public func flush(context: ChannelHandlerContext) {
        do {
            try self._flush(context: context)
        } catch {
            self.errorCaught(context: context, error: error)
        }
    }
}

public struct SSLHandshakeState {
    var inputBuffer: ByteBuffer
    var outputBuffer: ByteBuffer
    var outputPromise: EventLoopPromise<Void>

    enum State {
        case start
        case clientHelloSent
        case serverHelloRecieved
        case keyExchangeSent
        case keyExchangeRecieved
    }

    var state = State.start

    mutating func addReceivedData(_ buffer: ByteBuffer) {
        var buffer = buffer
        self.inputBuffer.writeBuffer(&buffer)
    }

    mutating func addPendingOutputData(_ buffer: ByteBuffer) {
        var buffer = buffer
        self.outputBuffer.writeBuffer(&buffer)
    }
}
