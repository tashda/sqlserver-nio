import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserReturnValueTests: XCTestCase {
    func testParseReturnValueInt() throws {
        var buf = ByteBufferAllocator().buffer(capacity: 64)
        // BVARCHAR name: length=3, UTF-16LE "out"
        buf.writeInteger(UInt8(3))
        let nameUTF16: [UInt8] = Array("out".utf16.flatMap { [UInt8($0 & 0xFF), UInt8(($0 >> 8) & 0xFF)] })
        buf.writeBytes(nameUTF16)
        // status
        buf.writeInteger(UInt8(0))
        // userType
        buf.writeInteger(UInt32(0), endianness: .little)
        // flags
        buf.writeInteger(UInt16(0), endianness: .little)
        // TYPE_INFO: INT (0x38)
        buf.writeInteger(UInt8(0x38))
        // value: 4-byte little endian 123
        buf.writeInteger(Int32(123), endianness: .little)

        var copy = buf
        let token = try TDSTokenParser.parseReturnValueToken(from: &copy)
        XCTAssertEqual(token.name, "out")
        XCTAssertEqual(token.metadata.dataType, .int)
        var v = token.value!
        XCTAssertEqual(v.readInteger(endianness: .little, as: Int32.self), 123)
    }
}

