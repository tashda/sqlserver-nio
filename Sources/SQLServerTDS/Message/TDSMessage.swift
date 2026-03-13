import NIOCore

/// Namespace for messages
public enum TDSMessages {}

/// Client or Server Message
public struct TDSMessage: Sendable {
    public var packetType: TDSPacket.HeaderType {
        packets[0].type
    }
    
    public internal(set) var packets: [TDSPacket]
    
    public init(packets: [TDSPacket]) {
        assert(!packets.isEmpty, "Invalid message")
        self.packets = packets
    }

    public init<P: TDSMessagePayload>(payload: P, allocator: ByteBufferAllocator) throws {
        var buffer = allocator.buffer(capacity: TDSPacket.maximumPacketDataLength)
        try payload.serialize(into: &buffer)
        self = try TDSMessage(from: &buffer, ofType: P.packetType, allocator: allocator)
    }
    
    public func writeToByteBuffer(_ data: inout ByteBuffer) {
        for var packet in packets {
            data.writeBuffer(&packet.buffer)
        }
    }
}
