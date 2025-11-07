import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserReturnValueTests: XCTestCase {
    func testParseReturnValueInt() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        buffer.writeInteger(TDSTokens.TokenType.returnValue.rawValue)
        // BVARCHAR name: length=3, UTF-16LE "out"
        buffer.writeInteger(UInt8(3))
        let nameUTF16: [UInt8] = Array("out".utf16.flatMap { [UInt8($0 & 0xFF), UInt8(($0 >> 8) & 0xFF)] })
        buffer.writeBytes(nameUTF16)
        // status
        buffer.writeInteger(UInt8(0))
        // userType
        buffer.writeInteger(UInt32(0), endianness: .little)
        // flags
        buffer.writeInteger(UInt16(0), endianness: .little)
        // TYPE_INFO: INT (0x38)
        buffer.writeInteger(UInt8(0x38))
        // value: 4-byte little endian 123
        buffer.writeInteger(Int32(123), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)

        let token = try XCTUnwrap(tokens[0] as? TDSTokens.ReturnValueToken)
        XCTAssertEqual(token.name, "out")
        XCTAssertEqual(token.metadata.dataType, TDSDataType.int)
        var v = token.value!
        XCTAssertEqual(v.readInteger(endianness: .little, as: Int32.self), 123)
    }
}

