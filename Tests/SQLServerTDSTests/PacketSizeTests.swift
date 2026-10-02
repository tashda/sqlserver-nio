import XCTest
@testable import SQLServerTDS
import NIOCore

/// The network packet size: what LOGIN7 asks for, and how requests are split into packets.
final class PacketSizeTests: XCTestCase {
    private func loginPacketSize(_ requested: Int) throws -> UInt32? {
        var message = TDSMessages.Login7Message(username: "sa", password: "p", serverName: "s", database: "master")
        message.packetSize = requested
        var buffer = ByteBufferAllocator().buffer(capacity: 512)
        try message.serialize(into: &buffer)
        // Length (4), TDS version (4), then PacketSize (4, little-endian) at offset 8.
        return buffer.getInteger(at: 8, endianness: .little, as: UInt32.self)
    }

    func testLoginAsksForTheConfiguredSize() throws {
        XCTAssertEqual(try loginPacketSize(TDSPacket.requestedPacketLength), 8000)
        XCTAssertEqual(try loginPacketSize(32767), 32767)
        XCTAssertEqual(try loginPacketSize(4096), 4096)
    }

    func testLoginClampsSizesSQLServerWouldRefuse() throws {
        XCTAssertEqual(try loginPacketSize(100), 512)
        XCTAssertEqual(try loginPacketSize(65535), 32767)
    }

    private func split(_ byteCount: Int, packetLength: Int) throws -> [TDSPacket] {
        var buffer = ByteBufferAllocator().buffer(capacity: byteCount)
        buffer.writeBytes((0..<byteCount).map { UInt8(truncatingIfNeeded: $0) })
        return try TDSMessage(from: &buffer, ofType: .sqlBatch, allocator: ByteBufferAllocator(), packetLength: packetLength).packets
    }

    func testRequestsAreSplitIntoPacketsOfTheNegotiatedSize() throws {
        let packets = try split(20_000, packetLength: 8000)
        XCTAssertEqual(packets.map { $0.buffer.readableBytes }, [8000, 8000, 20_000 - 2 * 7992 + 8])
        // Header: type (0), status (1, bit 0 = end of message), length (2-3), SPID (4-5), packet ID (6).
        let status = packets.map { $0.buffer.getInteger(at: $0.buffer.readerIndex + 1, as: UInt8.self) ?? 0 }
        XCTAssertEqual(status.map { $0 & 0x01 != 0 }, [false, false, true])
        XCTAssertEqual(packets.map { $0.buffer.getInteger(at: $0.buffer.readerIndex + 6, as: UInt8.self) }, [1, 2, 3])
        let lengths = packets.map { $0.buffer.getInteger(at: $0.buffer.readerIndex + 2, as: UInt16.self) }
        XCTAssertEqual(lengths, packets.map { UInt16($0.buffer.readableBytes) })
    }

    func testAMessageThatFillsAPacketExactlyHasNoEmptyTrailingPacket() throws {
        let packets = try split(7992, packetLength: 8000)
        XCTAssertEqual(packets.count, 1)
        XCTAssertEqual(packets.first?.buffer.readableBytes, 8000)
    }

    func testBeforeLoginRequestsUse4096BytePackets() throws {
        let packets = try split(5000, packetLength: TDSPacket.defaultPacketLength)
        XCTAssertEqual(packets.map { $0.buffer.readableBytes }, [4096, 5000 - 4088 + 8])
    }
}
