import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserReturnValueTests: XCTestCase {

    func testParseReturnValueInt() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        buffer.writeInteger(TDSTokens.TokenType.returnValue.rawValue)
        // ParamOrdinal: USHORT (ordinal 1)
        buffer.writeInteger(UInt16(1), endianness: .little)
        // ParamName: B_VARCHAR — length=3 chars, UTF-16LE "out"
        buffer.writeInteger(UInt8(3))
        let nameUTF16: [UInt8] = Array("out".utf16.flatMap { [UInt8($0 & 0xFF), UInt8(($0 >> 8) & 0xFF)] })
        buffer.writeBytes(nameUTF16)
        // Status: 0x01 = output parameter
        buffer.writeInteger(UInt8(0x01))
        // UserType: ULONG (4 bytes, TDS 7.2+)
        buffer.writeInteger(UInt32(0), endianness: .little)
        // Flags: USHORT
        buffer.writeInteger(UInt16(0), endianness: .little)
        // TYPE_INFO: INT4TYPE (0x38)
        buffer.writeInteger(UInt8(0x38))
        // Value: 4-byte little-endian 123
        buffer.writeInteger(Int32(123), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()

        XCTAssertEqual(tokens.count, 1)
        let token = try XCTUnwrap(tokens[0] as? TDSTokens.ReturnValueToken)
        XCTAssertEqual(token.paramOrdinal, 1)
        XCTAssertEqual(token.name, "out")
        XCTAssertEqual(token.status, 0x01)
        XCTAssertEqual(token.metadata.dataType, TDSDataType.int)
        var v = try XCTUnwrap(token.value)
        XCTAssertEqual(v.readInteger(endianness: .little, as: Int32.self), 123)
    }

    func testParseReturnValueNVarChar() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 128)
        buffer.writeInteger(TDSTokens.TokenType.returnValue.rawValue)
        // ParamOrdinal: USHORT
        buffer.writeInteger(UInt16(1), endianness: .little)
        // ParamName: B_VARCHAR "@result"
        let paramName = "@result"
        buffer.writeInteger(UInt8(paramName.count))
        buffer.writeUTF16String(paramName)
        // Status: 0x01
        buffer.writeInteger(UInt8(0x01))
        // UserType: ULONG
        buffer.writeInteger(UInt32(0), endianness: .little)
        // Flags: USHORT
        buffer.writeInteger(UInt16(0), endianness: .little)
        // TYPE_INFO: NVARCHARTYPE (0xE7), USHORTLEN max = 100 chars = 200 bytes
        buffer.writeInteger(UInt8(0xE7))
        buffer.writeInteger(UInt16(200), endianness: .little) // max length in bytes
        // Collation: 5 bytes
        buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x34])
        // Value: USHORTLEN + UTF-16LE "hi"
        let valueStr = "hi"
        let valueBytes = Array(valueStr.utf16.flatMap { [UInt8($0 & 0xFF), UInt8(($0 >> 8) & 0xFF)] })
        buffer.writeInteger(UInt16(valueBytes.count), endianness: .little)
        buffer.writeBytes(valueBytes)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()

        XCTAssertEqual(tokens.count, 1)
        let token = try XCTUnwrap(tokens[0] as? TDSTokens.ReturnValueToken)
        XCTAssertEqual(token.name, "@result")
        XCTAssertEqual(token.metadata.dataType, TDSDataType.nvarchar)
        var v = try XCTUnwrap(token.value)
        let decoded = v.readUTF16String(length: v.readableBytes)
        XCTAssertEqual(decoded, "hi")
    }
}
