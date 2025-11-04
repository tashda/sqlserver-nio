import Foundation
import NIO
import Logging

/// **MICROSOFT JDBC COMPATIBLE**: Packet-agnostic TDS Reader
///
/// Microsoft JDBC handles fragmented TDS data at the transport layer, so token parsers
/// never see incomplete data. This TDSReader emulates that approach by providing
/// packet-agnostic reading methods that automatically read across packet boundaries.
///
/// Key insights from Microsoft JDBC IOBuffer:
/// - `readBytes()` method automatically reads across packet boundaries
/// - `ensurePayload()` blocks until enough data arrives
/// - Token parsers never see fragmented data
/// - All packet reassembly happens transparently at the transport layer
public class TDSReader {
    private var packets: [TDSPacket] = []
    private var currentPacketIndex: Int = 0
    private var currentPacketOffset: Int = 0
    private let logger: Logger
    private let tokenRing: TDSTokenRing?

    init(logger: Logger, tokenRing: TDSTokenRing? = nil) {
        self.logger = logger
        self.tokenRing = tokenRing
    }

    /// Add a new TDS packet to the reader
    public func addPacket(_ packet: TDSPacket) {
        packets.append(packet)
        tokenRing?.record("tds_reader: packet_added type=\(packet.type) data_size=\(packet.messageBuffer.readableBytes)")
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Packet-agnostic byte reading
    /// Reads exactly `length` bytes, automatically reading across packet boundaries
    public func readBytes(length: Int) throws -> ByteBuffer {
        guard length > 0 else {
            return ByteBufferAllocator().buffer(capacity: 0)
        }

        let allocator = ByteBufferAllocator()
        var result = allocator.buffer(capacity: length)
        var bytesRead = 0

        tokenRing?.record("tds_reader: readBytes start length=\(length) available=\(availableBytes)")

        while bytesRead < length {
            guard ensurePayload() else {
                throw TDSError.protocolError("TDS Reader: No more data available to read \(length) bytes (read \(bytesRead))")
            }

            let currentPacket = packets[currentPacketIndex]
            let remainingInCurrentPacket = currentPacket.messageBuffer.readableBytes - currentPacketOffset
            let bytesToRead = min(length - bytesRead, remainingInCurrentPacket)

            // Copy bytes from current packet
            var packetSlice = currentPacket.messageBuffer
            packetSlice.moveReaderIndex(to: currentPacketOffset)
            guard let slice = packetSlice.readSlice(length: bytesToRead) else {
                throw TDSError.protocolError("TDS Reader: Failed to read \(bytesToRead) bytes from packet")
            }

            var mutableSlice = slice
            result.writeBuffer(&mutableSlice)
            bytesRead += bytesToRead
            currentPacketOffset += bytesToRead

            tokenRing?.record("tds_reader: readBytes progress packet=\(currentPacketIndex) offset=\(currentPacketOffset) total=\(bytesRead)/\(length)")

            // If we've consumed the current packet, move to the next one
            if currentPacketOffset >= currentPacket.messageBuffer.readableBytes {
                currentPacketIndex += 1
                currentPacketOffset = 0
                tokenRing?.record("tds_reader: moved_to_next_packet index=\(currentPacketIndex)")
            }
        }

        result.moveReaderIndex(to: 0)
        return result
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Read a single byte
    public func readByte() throws -> UInt8 {
        let bytes = try readBytes(length: 1)
        guard let byte = bytes.getInteger(at: 0, as: UInt8.self) else {
            throw TDSError.protocolError("TDS Reader: Failed to read single byte")
        }
        return byte
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Read a 16-bit integer (little endian)
    public func readShort() throws -> UInt16 {
        let bytes = try readBytes(length: 2)
        guard let value = bytes.getInteger(at: 0, endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("TDS Reader: Failed to read short (2 bytes)")
        }
        return value
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Read a 32-bit integer (little endian)
    public func readInt() throws -> UInt32 {
        let bytes = try readBytes(length: 4)
        guard let value = bytes.getInteger(at: 0, endianness: .little, as: UInt32.self) else {
            throw TDSError.protocolError("TDS Reader: Failed to read int (4 bytes)")
        }
        return value
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Read a 64-bit integer (little endian)
    public func readLong() throws -> UInt64 {
        let bytes = try readBytes(length: 8)
        guard let value = bytes.getInteger(at: 0, endianness: .little, as: UInt64.self) else {
            throw TDSError.protocolError("TDS Reader: Failed to read long (8 bytes)")
        }
        return value
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Peek at next byte without consuming it
    public func peekByte() throws -> UInt8 {
        guard ensurePayload() else {
            throw TDSError.protocolError("TDS Reader: No data available to peek")
        }

        let currentPacket = packets[currentPacketIndex]
        var packetSlice = currentPacket.messageBuffer
        packetSlice.moveReaderIndex(to: currentPacketOffset)
        guard let byte = packetSlice.getInteger(at: 0, as: UInt8.self) else {
            throw TDSError.protocolError("TDS Reader: Failed to peek at byte")
        }
        return byte
    }

    /// Check if we have any data available to read
    public func hasData() -> Bool {
        return availableBytes > 0
    }

    /// Get the total number of bytes available across all packets
    public var availableBytes: Int {
        var total = 0
        if currentPacketIndex < packets.count {
            // Add remaining bytes in current packet
            total += packets[currentPacketIndex].messageBuffer.readableBytes - currentPacketOffset
            // Add bytes in subsequent packets
            for i in (currentPacketIndex + 1)..<packets.count {
                total += packets[i].messageBuffer.readableBytes
            }
        }
        return total
    }

    /// **MICROSOFT JDBC COMPATIBLE**: Ensure we have payload to read from
    /// Returns true if data is available, false if no more packets
    private func ensurePayload() -> Bool {
        // Check if we have data in the current packet
        if currentPacketIndex < packets.count {
            let currentPacket = packets[currentPacketIndex]
            if currentPacketOffset < currentPacket.messageBuffer.readableBytes {
                return true
            }
        }

        // Try to move to next packet
        if currentPacketIndex + 1 < packets.count {
            currentPacketIndex += 1
            currentPacketOffset = 0
            tokenRing?.record("tds_reader: ensurePayload moved_to_next_packet index=\(currentPacketIndex)")
            return true
        }

        // No more packets available
        tokenRing?.record("tds_reader: ensurePayload no_more_packets_available")
        return false
    }

    /// Reset the reader state (clear all packets)
    public func reset() {
        packets.removeAll()
        currentPacketIndex = 0
        currentPacketOffset = 0
        tokenRing?.record("tds_reader: reset")
    }

    /// Get diagnostic information about the reader state
    public var debugInfo: String {
        return """
        TDSReader Debug Info:
        Total Packets: \(packets.count)
        Current Packet: \(currentPacketIndex)/\(packets.count)
        Current Offset: \(currentPacketOffset)
        Available Bytes: \(availableBytes)
        """
    }
}