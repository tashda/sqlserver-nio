import NIO
import Logging

public final class TDSPacketDecoder: ByteToMessageDecoder {
    public typealias InboundOut = ByteBuffer

    private let logger: Logger
    private var pendingPackets: [TDSPacket] = []
    private var pendingType: TDSPacket.HeaderType?

    public init(logger: Logger) {
        self.logger = logger
    }

    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        while true {
            let readerIndex = buffer.readerIndex
            guard let packet = TDSPacket(from: &buffer) else {
                buffer.moveReaderIndex(to: readerIndex)
                break
            }

            if pendingPackets.isEmpty {
                pendingType = packet.type
            } else if let currentType = pendingType, currentType != packet.type {
                emitPendingMessage(in: context)
                pendingType = packet.type
            }

            pendingPackets.append(packet)

            let statusValue = packet.header?.status.value ?? 0
            if statusValue & TDSPacket.Status.eom.value != 0 {
                emitPendingMessage(in: context)
            }
        }

        return .needMoreData
    }

    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        _ = try decode(context: context, buffer: &buffer)
        if !pendingPackets.isEmpty {
            emitPendingMessage(in: context)
        }
        return .needMoreData
    }

    private func emitPendingMessage(in context: ChannelHandlerContext) {
        guard !pendingPackets.isEmpty else { return }
        logger.trace("Emitting TDS message composed of \(pendingPackets.count) packet(s)")
        let messageBuffer = ByteBuffer(from: pendingPackets, allocator: context.channel.allocator)
        pendingPackets.removeAll(keepingCapacity: true)
        pendingType = nil
        context.fireChannelRead(wrapInboundOut(messageBuffer))
    }
}
