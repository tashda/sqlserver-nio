import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserGuidTests: XCTestCase {
    private func makeGuidMeta(colName: String = "uniqueidentifier_value") -> TDSTokens.ColMetadataToken {
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .guid,
            length: 16,
            collation: [],
            tableName: nil,
            colName: colName,
            precision: nil,
            scale: nil
        )
        return TDSTokens.ColMetadataToken(count: 1, colData: [col])
    }

    private func expectedGuidBytes() -> [UInt8] {
        // Matches pattern: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
        // Raw bytes: 4x AA, 2x BB, 2x CC, 2x DD, 6x EE = 16
        return [
            0xAA, 0xAA, 0xAA, 0xAA,
            0xBB, 0xBB,
            0xCC, 0xCC,
            0xDD, 0xDD,
            0xEE, 0xEE, 0xEE, 0xEE, 0xEE, 0xEE,
        ]
    }

    func testRowGuid_WithBytelenPrefix() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt8(0x10)) // BYTELEN (16)
        buffer.writeBytes(expectedGuidBytes())

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeGuidMeta()

        let row = try XCTUnwrap(parser.parseRowToken())

        XCTAssertEqual(stream.buffer.readableBytes, 0, "row should consume all bytes")
        XCTAssertEqual(row.colData.count, 1)
        guard let payload = row.colData[0].data else { return XCTFail("GUID should be non-nil") }
        XCTAssertEqual(payload.readableBytes, 16)
        let bytes = payload.getBytes(at: payload.readerIndex, length: payload.readableBytes)!
        XCTAssertEqual(bytes, expectedGuidBytes())
    }

    func testRowGuid_NullSentinel() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 1)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt8(0xFF)) // BYTELEN null

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeGuidMeta()

        let row = try XCTUnwrap(parser.parseRowToken())

        XCTAssertEqual(stream.buffer.readableBytes, 0)
        XCTAssertEqual(row.colData.count, 1)
        XCTAssertNil(row.colData[0].data)
    }

    func testRowGuid_NoPrefix_FallbackFixed16() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 17)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeBytes(expectedGuidBytes()) // no BYTELEN prefix present

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeGuidMeta()

        let row = try XCTUnwrap(parser.parseRowToken())

        XCTAssertEqual(stream.buffer.readableBytes, 0)
        guard let payload = row.colData[0].data else { return XCTFail("GUID should be non-nil") }
        let bytes = payload.getBytes(at: payload.readerIndex, length: payload.readableBytes)!
        XCTAssertEqual(bytes, expectedGuidBytes())
    }

    func testNbcRowGuid_WithBytelenPrefix_NotNull() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 33)
        buffer.writeInteger(TDSTokens.TokenType.nbcRow.rawValue)
        // Null bitmap for 1 column -> 1 byte (bit 0 = 0 means not null)
        buffer.writeInteger(UInt8(0x00)) // null bitmap
        buffer.writeInteger(UInt8(0x10)) // BYTELEN (16)
        buffer.writeBytes(expectedGuidBytes())

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeGuidMeta()

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)

        let nbc = try XCTUnwrap(tokens[0] as? TDSTokens.NbcRowToken)
        XCTAssertEqual(stream.buffer.readableBytes, 0)
        XCTAssertEqual(nbc.colData.count, 1)
        guard let payload = nbc.colData[0].data else { return XCTFail("GUID should be non-nil") }
        XCTAssertEqual(payload.readableBytes, 16)
        let bytes = payload.getBytes(at: payload.readerIndex, length: payload.readableBytes)!
        XCTAssertEqual(bytes, expectedGuidBytes())
    }

    func testNbcRowGuid_Null_ViaBitmap() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 2)
        buffer.writeInteger(TDSTokens.TokenType.nbcRow.rawValue)
        // Null bitmap: bit 0 = 1 means NULL; no value bytes should follow
        buffer.writeInteger(UInt8(0x01)) // null bitmap (column 0 is NULL)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = makeGuidMeta()

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)

        let nbc = try XCTUnwrap(tokens[0] as? TDSTokens.NbcRowToken)
        XCTAssertEqual(stream.buffer.readableBytes, 0)
        XCTAssertEqual(nbc.colData.count, 1)
        XCTAssertNil(nbc.colData[0].data)
    }
}

