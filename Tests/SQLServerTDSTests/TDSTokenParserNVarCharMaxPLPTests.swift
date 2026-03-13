import XCTest
import NIOCore
@testable import SQLServerTDS

/// Tests for NVARCHAR(MAX) values encoded as PLP (PARTLENTYPE) streams.
final class TDSTokenOperationsNVarCharMaxPLPTests: XCTestCase, @unchecked Sendable {
    private func makeNVarCharMaxMeta() -> TDSTokens.ColMetadataToken {
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .nvarchar,
            length: Int32(UInt16.max), // 0xFFFF => NVARCHAR(MAX) / PARTLENTYPE
            collation: [],
            tableName: nil,
            colName: "definition",
            precision: nil,
            scale: nil
        )
        return TDSTokens.ColMetadataToken(count: 1, colData: [col])
    }

    func testNVarCharMaxPlpSingleChunk() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 128)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)

        let text = "NVARCHAR MAX PLP"
        let utf16Bytes = Array(text.utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] })
        let byteCount = UInt64(utf16Bytes.count)

        // PLP: total length (UInt64 LE)
        buffer.writeInteger(byteCount, endianness: .little)
        // One chunk with the full payload
        buffer.writeInteger(UInt32(utf16Bytes.count), endianness: .little)
        buffer.writeBytes(utf16Bytes)
        // Terminator chunk
        buffer.writeInteger(UInt32(0), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeNVarCharMaxMeta()

        let row = try XCTUnwrap(parser.parseRowToken())
        XCTAssertEqual(row.colData.count, 1)

        var data = try XCTUnwrap(row.colData[0].data)
        XCTAssertEqual(data.readableBytes, utf16Bytes.count)
        let decoded = data.readUTF16String(length: data.readableBytes)
        XCTAssertEqual(decoded, text)
    }

    func testNVarCharMaxPlpUnknownLengthMultiChunk() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 128)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)

        let part1 = "Hello, "
        let part2 = "NVARCHAR(MAX)" 
        let full = part1 + part2

        func utf16Bytes(_ s: String) -> [UInt8] {
            Array(s.utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] })
        }

        let bytes1 = utf16Bytes(part1)
        let bytes2 = utf16Bytes(part2)

        // PLP with unknown length: totalLength = 0xFFFFFFFFFFFFFFFE
        buffer.writeInteger(UInt64.max &- 1, endianness: .little)
        // First chunk
        buffer.writeInteger(UInt32(bytes1.count), endianness: .little)
        buffer.writeBytes(bytes1)
        // Second chunk
        buffer.writeInteger(UInt32(bytes2.count), endianness: .little)
        buffer.writeBytes(bytes2)
        // Terminator
        buffer.writeInteger(UInt32(0), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeNVarCharMaxMeta()

        let row = try XCTUnwrap(parser.parseRowToken())
        XCTAssertEqual(row.colData.count, 1)

        var data = try XCTUnwrap(row.colData[0].data)
        let decoded = data.readUTF16String(length: data.readableBytes)
        XCTAssertEqual(decoded, full)
    }
}
