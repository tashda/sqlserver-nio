@testable import SQLServerTDS
import NIOCore
import XCTest

final class ReturnStatusTokenTests: XCTestCase {
    func testParserConsumesReturnStatusToken() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 6)
        buffer.writeInteger(TDSTokens.TokenType.returnStatus.rawValue)
        buffer.writeInteger(Int32(42), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)

        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)

        guard let statusToken = tokens.first as? TDSTokens.ReturnStatusToken else {
            XCTFail("Expected ReturnStatusToken")
            return
        }
        XCTAssertEqual(statusToken.value, 42)
    }
}
