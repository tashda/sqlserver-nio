import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenOperationsMiscTokensTests: XCTestCase, @unchecked Sendable {
    func testParseFeatureExtAckAndFedAuthInfo() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        buffer.writeInteger(TDSTokens.TokenType.featureExtAck.rawValue)
        buffer.writeInteger(UInt8(0x01))
        buffer.writeInteger(UInt16(4), endianness: .little)
        buffer.writeBytes([0xDE, 0xAD, 0xBE, 0xEF])
        buffer.writeInteger(UInt8(0xFF))
        buffer.writeInteger(TDSTokens.TokenType.fedAuthInfo.rawValue)
        buffer.writeInteger(UInt32(1), endianness: .little)
        buffer.writeBytes([0xAA])
        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 2)

        let feat = try XCTUnwrap(tokens[0] as? TDSTokens.FeatureExtAckToken)
        XCTAssertEqual(feat.payload.readableBytes, 8)

        let fed = try XCTUnwrap(tokens[1] as? TDSTokens.FedAuthInfoToken)
        XCTAssertEqual(fed.payload.readableBytes, 1)
    }

    func testParseSessionStateAndDataClassification() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 5)
        buffer.writeInteger(TDSTokens.TokenType.sessionState.rawValue)
        buffer.writeInteger(UInt32(3), endianness: .little)
        buffer.writeBytes([1,2,3])
        buffer.writeInteger(TDSTokens.TokenType.dataClassification.rawValue)
        buffer.writeInteger(UInt16(3), endianness: .little)
        buffer.writeBytes([4,5,6])
        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 2)

        let ss = try XCTUnwrap(tokens[0] as? TDSTokens.SessionStateToken)
        XCTAssertEqual(ss.payload.readableBytes, 3)

        let dc = try XCTUnwrap(tokens[1] as? TDSTokens.DataClassificationToken)
        XCTAssertEqual(dc.payload.readableBytes, 3)
    }

    func testParseOffsetToken() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 4)
        buffer.writeInteger(TDSTokens.TokenType.offset.rawValue)
        buffer.writeInteger(UInt16(2), endianness: .little)
        buffer.writeBytes([0x00, 0x01])
        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)

        let off = try XCTUnwrap(tokens[0] as? TDSTokens.OffsetToken)
        XCTAssertEqual(off.data, [0x00, 0x01])
    }

    func testParseTVPRow() throws {
        // COLMETADATA for one INT column
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .int, length: 4, collation: [], tableName: nil, colName: "c1", precision: nil, scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [col])

        var buffer = ByteBufferAllocator().buffer(capacity: 6)
        buffer.writeInteger(TDSTokens.TokenType.tvpRow.rawValue)
        // TVP_ROW: bitmap byte (0 -> not null), followed by 4 bytes little endian = 42
        buffer.writeInteger(UInt8(0))
        buffer.writeInteger(Int32(42), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta

        let tokens = try parser.parse()

        XCTAssertEqual(tokens.count, 1)

        let tvp = try XCTUnwrap(tokens[0] as? TDSTokens.TVPRowToken)
        XCTAssertEqual(tvp.colData.count, 1)
        var v = tvp.colData[0].data!
        XCTAssertEqual(v.readInteger(endianness: Endianness.little, as: Int32.self), 42)
    }

    func testSkipsUndocumentedTokenBytesWithoutInventedPayloadFormats() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 6)
        buffer.writeBytes([0x61, 0x02, 0x00, 0x01, 0x02, TDSTokens.TokenType.done.rawValue])
        buffer.writeInteger(UInt16(0), endianness: .little)
        buffer.writeInteger(UInt16(0), endianness: .little)
        buffer.writeInteger(UInt64(0), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()

        XCTAssertEqual(tokens.count, 1)
        XCTAssertTrue(tokens[0] is TDSTokens.DoneToken)
    }
}
