import XCTest
import NIO
@testable import SQLServerTDS

final class TDSTests: XCTestCase, @unchecked Sendable {
    func testTDSPacketCreation() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 1024)
        // Correct length is 16 (0x10) bytes: 8 for the header + 8 for the data.
        buffer.writeBytes([0x04, 0x01, 0x00, 0x10, 0x00, 0x00, 0x01, 0x00]) // Header
        buffer.writeBytes([0xAD, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]) // Data

        var packetBuffer = buffer
        let packet = TDSPacket(from: &packetBuffer)

        XCTAssertNotNil(packet)
        XCTAssertEqual(packet?.type, .loginResponse)
        XCTAssertEqual(packet?.header.length, 16)
    }

    func testTDSMessageSerialization() throws {
        let allocator = ByteBufferAllocator()
        let message = TDSMessages.RawSqlBatchMessage(sqlText: "SELECT 1")
        let tdsMessage = try TDSMessage(payload: message, allocator: allocator)

        var out = allocator.buffer(capacity: 1024)
        tdsMessage.writeToByteBuffer(&out)

        XCTAssertGreaterThan(out.readableBytes, 0)
        // Packet type byte must be sqlBatch (0x01)
        XCTAssertEqual(out.getBytes(at: 0, length: 1), [0x01])
        // Output must contain the UTF-16LE encoding of "SELECT 1"
        let sqlUTF16: [UInt8] = "SELECT 1".utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] }
        let outBytes = out.getBytes(at: 0, length: out.readableBytes)!
        let found = (0...(outBytes.count - sqlUTF16.count)).contains { i in
            outBytes[i..<(i + sqlUTF16.count)].elementsEqual(sqlUTF16)
        }
        XCTAssertTrue(found, "Serialized packet must contain UTF-16LE encoded SQL text")
    }
}
