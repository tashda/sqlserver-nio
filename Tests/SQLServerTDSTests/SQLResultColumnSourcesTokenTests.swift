@testable import SQLServerTDS
import NIOCore
import XCTest

final class SQLResultColumnSourcesTokenTests: XCTestCase, @unchecked Sendable {
    func testParserConsumesToken() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 10)
        buffer.writeInteger(TDSTokens.TokenType.sqlResultColumnSources.rawValue)
        buffer.writeInteger(UInt32(4), endianness: .little)
        buffer.writeBytes([0x01, 0x02, 0x03, 0x04])

        let parser = TDSTokenOperations(streamParser: TDSStreamParser(), logger: .init(label: "test"))
        parser.streamParser.buffer.writeBuffer(&buffer)

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let token = try XCTUnwrap(tokens.first as? TDSTokens.SQLResultColumnSourcesToken)
        XCTAssertEqual(token.payload.readableBytes, 4)
        XCTAssertEqual(token.payload.getBytes(at: token.payload.readerIndex, length: 4), [0x01, 0x02, 0x03, 0x04])
    }
}
