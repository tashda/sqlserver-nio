import NIO
import Logging

public final class TDSPacketDecoder: ByteToMessageDecoder {
    public typealias InboundOut = ByteBuffer

    let logger: Logger
    private var streamParser: TDSStreamParser

    public init(logger: Logger) {
        self.logger = logger
        self.streamParser = TDSStreamParser()
    }

    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        streamParser.buffer.writeBuffer(&buffer)
        context.fireChannelRead(wrapInboundOut(streamParser.buffer))
        return .continue
    }

    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        streamParser.buffer.writeBuffer(&buffer)
        context.fireChannelRead(wrapInboundOut(streamParser.buffer))
        return .needMoreData
    }
}