import NIO
import Logging

/// The payload of one received TDS packet.
///
/// Packets are delivered as they arrive instead of being reassembled into a
/// complete message first. A large result set can span millions of packets,
/// so buffering until end-of-message would hold the whole response in memory
/// and defeat streaming and back-pressure. Consumers that need a complete
/// message, such as the PRELOGIN response parser, accumulate chunks until
/// `isEndOfMessage` is set.
public struct TDSPacketChunk: Sendable {
    public var type: TDSPacket.HeaderType
    public var payload: ByteBuffer
    public var isEndOfMessage: Bool

    public init(type: TDSPacket.HeaderType, payload: ByteBuffer, isEndOfMessage: Bool) {
        self.type = type
        self.payload = payload
        self.isEndOfMessage = isEndOfMessage
    }
}

public final class TDSPacketDecoder: ByteToMessageDecoder {
    public typealias InboundOut = TDSPacketChunk

    /// Test hook: when greater than zero, each packet payload is split into
    /// fragments of at most this many bytes. This exercises the incremental
    /// token parser against real server traffic with tokens split at every
    /// possible boundary. Set through `TDS_DEBUG_FRAGMENT_SIZE`.
    internal static let debugFragmentSize: Int = {
        guard let raw = getenv("TDS_DEBUG_FRAGMENT_SIZE"), let value = Int(String(cString: raw)), value > 0 else {
            return 0
        }
        return value
    }()

    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        while true {
            // Validate the length field before waiting for more bytes. A length
            // shorter than the header can never complete and would otherwise
            // stall the connection forever.
            if buffer.readableBytes >= TDSPacket.headerLength,
               let length: UInt16 = buffer.getInteger(at: buffer.readerIndex + 2),
               Int(length) < TDSPacket.headerLength {
                throw TDSError.protocolError("Invalid TDS packet length \(length)")
            }
            guard let packet = TDSPacket(from: &buffer) else {
                return .needMoreData
            }
            let status = packet.header?.status.value ?? 0
            let isEOM = status & TDSPacket.Status.eom.value != 0
            emit(TDSPacketChunk(type: packet.type, payload: packet.messageBuffer, isEndOfMessage: isEOM), context: context)
        }
    }

    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        // A trailing partial packet cannot be used once the stream has ended.
        _ = try decode(context: context, buffer: &buffer)
        return .needMoreData
    }

    private func emit(_ chunk: TDSPacketChunk, context: ChannelHandlerContext) {
        let fragmentSize = Self.debugFragmentSize
        guard fragmentSize > 0, chunk.payload.readableBytes > fragmentSize else {
            context.fireChannelRead(wrapInboundOut(chunk))
            return
        }
        var payload = chunk.payload
        while payload.readableBytes > 0 {
            let length = min(fragmentSize, payload.readableBytes)
            let fragment = payload.readSlice(length: length)!
            let isLast = payload.readableBytes == 0
            context.fireChannelRead(wrapInboundOut(TDSPacketChunk(
                type: chunk.type,
                payload: fragment,
                isEndOfMessage: isLast && chunk.isEndOfMessage
            )))
        }
    }
}
