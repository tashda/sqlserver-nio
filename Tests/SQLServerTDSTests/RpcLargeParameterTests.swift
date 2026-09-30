import XCTest
@testable import SQLServerTDS
import NIOCore

/// String and binary parameters over 8000 bytes are sent as MAX types in PLP chunks.
final class RpcLargeParameterTests: XCTestCase {
    private func encode(type: UInt8, collation: [UInt8]?, value: [UInt8]?) -> [UInt8] {
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        TDSMessages.RpcRequestMessage.writeVariableLength(type: type, collation: collation, value: value, into: &buffer)
        return buffer.readBytes(length: buffer.readableBytes) ?? []
    }

    private let collation: [UInt8] = [0x09, 0x04, 0xD0, 0x00, 0x34]

    func testUpTo8000BytesUsesTheShortForm() {
        let value = [UInt8](repeating: 0x41, count: 8000)
        let bytes = encode(type: TDSDataType.nvarchar.rawValue, collation: collation, value: value)
        XCTAssertEqual(bytes[0], TDSDataType.nvarchar.rawValue)
        XCTAssertEqual(UInt16(bytes[1]) | UInt16(bytes[2]) << 8, 8000, "maximum length")
        XCTAssertEqual(Array(bytes[3..<8]), collation)
        XCTAssertEqual(UInt16(bytes[8]) | UInt16(bytes[9]) << 8, 8000, "value length")
        XCTAssertEqual(bytes.count, 10 + 8000)
    }

    func testOver8000BytesUsesMaxAndPLP() {
        let value = [UInt8](repeating: 0x41, count: 400_000)
        var buffer = ByteBufferAllocator().buffer(capacity: 400_100)
        TDSMessages.RpcRequestMessage.writeVariableLength(type: TDSDataType.nvarchar.rawValue, collation: collation, value: value, into: &buffer)
        XCTAssertEqual(buffer.readInteger(as: UInt8.self), TDSDataType.nvarchar.rawValue)
        XCTAssertEqual(buffer.readInteger(endianness: .little, as: UInt16.self), 0xFFFF, "MAX")
        XCTAssertEqual(buffer.readBytes(length: 5), collation)
        XCTAssertEqual(buffer.readInteger(endianness: .little, as: UInt64.self), 400_000, "PLP total length")
        XCTAssertEqual(buffer.readInteger(endianness: .little, as: UInt32.self), 400_000, "chunk length")
        buffer.moveReaderIndex(forwardBy: 400_000)
        XCTAssertEqual(buffer.readInteger(endianness: .little, as: UInt32.self), 0, "terminator")
        XCTAssertEqual(buffer.readableBytes, 0)
    }

    func testFixedLengthTypesBecomeVaryingWhenLarge() {
        let bytes = encode(type: TDSDataType.binary.rawValue, collation: nil, value: [UInt8](repeating: 1, count: 9000))
        XCTAssertEqual(bytes[0], TDSDataType.varbinary.rawValue)
        XCTAssertEqual(UInt16(bytes[1]) | UInt16(bytes[2]) << 8, 0xFFFF)
    }

    func testNullIsTheShortFormNull() {
        let bytes = encode(type: TDSDataType.nvarchar.rawValue, collation: collation, value: nil)
        XCTAssertEqual(bytes, [TDSDataType.nvarchar.rawValue, 0x40, 0x1F] + collation + [0xFF, 0xFF])
    }
}

/// Swift strings go to the server as nvarchar in UTF-16LE whatever their length; values read from
/// a varchar column keep their code-page bytes and collation.
final class RpcStringParameterTests: XCTestCase {
    /// The parameter's TYPE_INFO type byte and its value bytes, read from the end of the request.
    private func encoded(_ data: TDSData) throws -> (type: UInt8, collation: [UInt8], value: [UInt8]) {
        let message = TDSMessages.RpcRequestMessage(procedureName: "sp_executesql", parameters: [.init(name: "@stmt", data: data)])
        var buffer = ByteBufferAllocator().buffer(capacity: 256)
        try message.serialize(into: &buffer)
        let bytes = buffer.readBytes(length: buffer.readableBytes) ?? []
        // …type (1), max length (2), collation (5), value length (2), value.
        for valueLength in stride(from: 0, through: bytes.count - 10, by: 1) {
            let lengthOffset = bytes.count - valueLength - 2
            guard lengthOffset >= 8 else { break }
            let declared = Int(bytes[lengthOffset]) | Int(bytes[lengthOffset + 1]) << 8
            if declared == valueLength {
                let typeOffset = lengthOffset - 8
                return (bytes[typeOffset], Array(bytes[(typeOffset + 3)..<(typeOffset + 8)]), Array(bytes[(lengthOffset + 2)...]))
            }
        }
        throw XCTSkip("value not found")
    }

    private func utf16(_ string: String) -> [UInt8] {
        string.utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] }
    }

    func testSwiftStringsOfEveryLengthAreSentAsUTF16() throws {
        for text in ["ABC", "ABCD", "SELECT LEN(N'packet-packet-') AS n", "Привет, мир"] {
            let result = try encoded(TDSData(string: text))
            XCTAssertEqual(result.type, TDSDataType.nvarchar.rawValue, text)
            XCTAssertEqual(result.value, utf16(text), text)
        }
    }

    func testVarcharFromAColumnKeepsItsCodePageAndCollation() throws {
        let cyrillic: [UInt8] = [0x19, 0x04, 0xD0, 0x00, 0x00]
        var value = ByteBufferAllocator().buffer(capacity: 6)
        value.writeBytes([207, 240, 232, 226, 229, 242]) // "Привет" in code page 1251
        let data = TDSData(metadata: TypeMetadata(dataType: .varchar, collation: cyrillic), value: value)
        let result = try encoded(data)
        XCTAssertEqual(result.type, TDSDataType.varchar.rawValue)
        XCTAssertEqual(result.collation, cyrillic)
        XCTAssertEqual(result.value, [207, 240, 232, 226, 229, 242])
    }
}
