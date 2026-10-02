import NIO

import NIO

extension SQLServerTDS.TDSMessage {
    /// Splits a serialized message into packets of at most `packetLength` bytes (header included).
    init(
        from buffer: inout ByteBuffer,
        ofType type: TDSPacket.HeaderType,
        allocator: ByteBufferAllocator,
        packetLength: Int = TDSPacket.defaultPacketLength
    ) throws {
        var packets = [TDSPacket]()
        let dataLength = packetLength - TDSPacket.headerLength
        
        var packetId: UInt8 = 1
        while buffer.readableBytes > dataLength {
            guard var packetData = buffer.readSlice(length: dataLength) else {
                throw TDSError.protocolError("Serialization Error: Expected")
            }
            
            packets.append(TDSPacket(from: &packetData, ofType: type, isLastPacket: false, packetId: packetId, allocator: allocator))
            packetId = packetId &+ 1
        }
        
        var lastPacket = buffer.slice()
        packets.append(TDSPacket(from: &lastPacket, ofType: type, isLastPacket: true, packetId: packetId, allocator: allocator))
        
        self.init(packets: packets)
    }
}
