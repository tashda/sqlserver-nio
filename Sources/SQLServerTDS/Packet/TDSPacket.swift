import NIO
import Foundation

/// Packet
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/e5ea8520-1ea3-4a75-a2a9-c17e63e9ee19
public struct TDSPacket {
    /// Packet Header
    var header: Header! {
        Header(from: buffer)
    }
    
    let type: HeaderType
    
    public var messageBuffer: ByteBuffer {
        buffer.getSlice(at: Header.length, length: buffer.readableBytes - Header.length)!
    }
    
    /// Packet Data
    internal var buffer: ByteBuffer
    
    init?(from buffer: inout ByteBuffer) {
        guard buffer.readableBytes >= Header.length else {
            // Debug: Not enough data for packet header
            return nil
        }

        // **MICROSOFT JDBC COMPATIBILITY**: Handle fragmented TDS packets properly
        // Microsoft JDBC accumulates TCP data until it has complete TDS packets
        guard let typeByte: UInt8 = buffer.getInteger(at: 0) else {
            return nil
        }

        guard let length: UInt16 = buffer.getInteger(at: 2) else { // After type and status
            return nil
        }

        // **CRITICAL FIX**: TDS packets are typically 4096 bytes, but TCP can fragment them
        // We must wait until we have the COMPLETE packet, not just parse partial data
        let packetLength = Int(length)
        if buffer.readableBytes < packetLength {
            print("🔍 TDSPacket: Fragmented packet detected - have \(buffer.readableBytes) bytes, need \(packetLength) bytes")
            return nil
        }

        // **MICROSOFT JDBC COMPATIBILITY**: Validate packet header consistency
        // Microsoft JDBC validates that the packet length field matches actual data size
        // to prevent stream corruption from inconsistent headers
        if packetLength > 0 && packetLength <= TDSPacket.defaultPacketLength {
            // Additional validation: ensure packet type is valid
            let validPacketTypes: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x06, 0x07, 0x08, 0x12, 0x13, 0x14, 0x15, 0x16]
            if !validPacketTypes.contains(typeByte) {
                print("🔥 TDSPacket: Invalid packet type 0x\(String(typeByte, radix: 16)) - rejecting to prevent corruption")
                return nil
            }

            // Validate packet length is reasonable
            if packetLength < TDSPacket.headerLength || packetLength > TDSPacket.defaultPacketLength {
                print("🔥 TDSPacket: Invalid packet length \(packetLength) - rejecting to prevent corruption")
                return nil
            }
        } else {
            print("🔥 TDSPacket: Packet length \(packetLength) out of valid range - rejecting to prevent corruption")
            return nil
        }

        // Debug: Log packet header bytes for analysis
        if buffer.readableBytes >= 8 {
            let headerBytes = buffer.getBytes(at: 0, length: 8) ?? []
            let hexString = headerBytes.map { String(format: "%02x", $0) }.joined()
            print("🔍 TDSPacket: Complete packet available: \(hexString), total readable: \(buffer.readableBytes) bytes")
        }

        print("🔍 TDSPacket: type=0x\(String(typeByte, radix: 16)), length=\(length), available=\(buffer.readableBytes)")

        guard let slice = buffer.readSlice(length: packetLength) else {
            return nil
        }

        self.type = .init(integerLiteral: typeByte)
        self.buffer = slice
        print("🔍 TDSPacket: Successfully parsed complete packet, type=0x\(String(typeByte, radix: 16)), length=\(length)")
    }
    
    init<M: TDSMessagePayload>(message: M, allocator: ByteBufferAllocator) throws {
        var buffer = allocator.buffer(capacity: 4_096)
        
        buffer.writeInteger(M.packetType.value)
        buffer.writeInteger(0x00 as UInt8) // status
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(0x01 as UInt8) // PacketID must start at 1
        buffer.writeInteger(0x00 as UInt8) // Window
        
        try message.serialize(into: &buffer)
        
        // Update length
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.type = M.packetType
        self.buffer = buffer
    }
    
    init(from inputBuffer: inout ByteBuffer, ofType type: HeaderType, isLastPacket: Bool, packetId: UInt8, allocator: ByteBufferAllocator) {
        var buffer = allocator.buffer(capacity: inputBuffer.readableBytes + TDSPacket.headerLength)
        
        buffer.writeInteger(type.value)
        buffer.writeInteger(isLastPacket ? TDSPacket.Status.eom.value : TDSPacket.Status.normal.value) // status
        
        // Skip length, it will be set later
        buffer.moveWriterIndex(forwardBy: 2)
        buffer.writeInteger(0x00 as UInt16) // SPID
        buffer.writeInteger(packetId) // PacketID
        buffer.writeInteger(0x00 as UInt8) // Window
        
        buffer.writeBuffer(&inputBuffer)
        
        // Update length
        buffer.setInteger(UInt16(buffer.writerIndex), at: 2)
        
        self.type = type
        self.buffer = buffer
    }
}

extension TDSPacket {
    public static let defaultPacketLength = 4096
    public static let headerLength = 8
    public static let maximumPacketDataLength = TDSPacket.defaultPacketLength - 8
}
