@testable import SQLServerTDS
import NIOCore
import XCTest

final class TDSStreamParserTests: XCTestCase, @unchecked Sendable {
    func testReadUInt8DoesNotAdvanceWhenDataMissing() {
        let parser = TDSStreamParser()
        XCTAssertEqual(parser.position, 0)
        XCTAssertNil(parser.readUInt8())
        XCTAssertEqual(parser.position, 0, "Position should not advance without data")

        var chunk = ByteBufferAllocator().buffer(capacity: 1)
        chunk.writeInteger(UInt8(0x12))
        parser.buffer.writeBuffer(&chunk)

        XCTAssertEqual(parser.readUInt8(), 0x12)
        XCTAssertEqual(parser.position, 1)
    }

    func testReadUInt16LEResumesAfterPartialData() {
        let parser = TDSStreamParser()
        var firstByte = ByteBufferAllocator().buffer(capacity: 1)
        firstByte.writeInteger(UInt8(0xCD))
        parser.buffer.writeBuffer(&firstByte)

        XCTAssertNil(parser.readUInt16LE())
        XCTAssertEqual(parser.position, 0, "Position should reset when insufficient data is available")

        var secondByte = ByteBufferAllocator().buffer(capacity: 1)
        secondByte.writeInteger(UInt8(0xAB))
        parser.buffer.writeBuffer(&secondByte)

        XCTAssertEqual(parser.readUInt16LE(), 0xABCD)
        XCTAssertEqual(parser.position, 2)
    }
}
