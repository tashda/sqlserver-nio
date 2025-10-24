import XCTest
import NIO
@testable import SQLServerTDS

final class TDSTests: XCTestCase {
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
    }
}
