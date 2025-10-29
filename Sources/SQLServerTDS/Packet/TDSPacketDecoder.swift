import NIO
import Logging

public final class TDSPacketDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = TDSPacket

    let logger: Logger
    
    /// Creates a new `TDSPacketDecoder`.
    public init(logger: Logger) {
        self.logger = logger
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        while let packet = TDSPacket(from: &buffer) {
            context.fireChannelRead(wrapInboundOut(packet))
            return .continue
        }
        
        return .needMoreData
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        logger.trace("Decoding last")
        // Drain any remaining complete packets before the channel becomes inactive
        while let packet = TDSPacket(from: &buffer) {
            context.fireChannelRead(wrapInboundOut(packet))
        }
        // No more data left to decode. Signal that we are done to avoid
        // re-entering decodeLast in a tight loop when the buffer is empty.
        return .needMoreData
    }
}
