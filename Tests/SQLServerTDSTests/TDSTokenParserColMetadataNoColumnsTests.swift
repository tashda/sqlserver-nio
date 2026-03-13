import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenOperationsColMetadataNoColumnsTests: XCTestCase, @unchecked Sendable {
    func testColMetadataNoColumnsSentinel() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeInteger(TDSTokens.TokenType.colMetadata.rawValue)
        // COUNT = 0xFFFF means no columns
        buffer.writeInteger(UInt16(0xFFFF), endianness: .little)
        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        var bufferCopy = stream.buffer
        let token = try TDSTokenOperations.parseColMetadataToken(from: &bufferCopy)
        XCTAssertEqual(token.count, 0)
        XCTAssertTrue(token.colData.isEmpty)
    }
}
