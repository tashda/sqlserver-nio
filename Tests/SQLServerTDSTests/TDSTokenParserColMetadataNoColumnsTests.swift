import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserColMetadataNoColumnsTests: XCTestCase {
    func testColMetadataNoColumnsSentinel() throws {
        var buf = ByteBufferAllocator().buffer(capacity: 2)
        // COUNT = 0xFFFF means no columns
        buf.writeInteger(UInt16(0xFFFF), endianness: .little)
        var copy = buf
        let token = try TDSTokenParser.parseColMetadataToken(from: &copy)
        XCTAssertEqual(token.count, 0)
        XCTAssertTrue(token.colData.isEmpty)
    }
}

