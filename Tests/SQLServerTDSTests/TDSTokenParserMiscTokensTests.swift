import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserMiscTokensTests: XCTestCase {
    func testParseFeatureExtAckAndFedAuthInfo() throws {
        var f = ByteBufferAllocator().buffer(capacity: 6)
        f.writeInteger(UInt16(4), endianness: .little)
        f.writeBytes([0xDE, 0xAD, 0xBE, 0xEF])
        var fcopy = f
        let feat = try TDSTokenParser.parseFeatureExtAckToken(from: &fcopy)
        XCTAssertEqual(feat.payload.readableBytes, 4)

        var a = ByteBufferAllocator().buffer(capacity: 3)
        a.writeInteger(UInt16(1), endianness: .little)
        a.writeInteger(UInt8(0xAA))
        var acopy = a
        let fed = try TDSTokenParser.parseFedAuthInfoToken(from: &acopy)
        XCTAssertEqual(fed.payload.readableBytes, 1)
    }

    func testParseSessionStateAndDataClassification() throws {
        var s = ByteBufferAllocator().buffer(capacity: 5)
        s.writeInteger(UInt16(3), endianness: .little)
        s.writeBytes([1,2,3])
        var scopy = s
        let ss = try TDSTokenParser.parseSessionStateToken(from: &scopy)
        XCTAssertEqual(ss.payload.readableBytes, 3)

        var d = ByteBufferAllocator().buffer(capacity: 5)
        d.writeInteger(UInt16(3), endianness: .little)
        d.writeBytes([4,5,6])
        var dcopy = d
        let dc = try TDSTokenParser.parseDataClassificationToken(from: &dcopy)
        XCTAssertEqual(dc.payload.readableBytes, 3)
    }

    func testParseOffsetToken() throws {
        var b = ByteBufferAllocator().buffer(capacity: 4)
        b.writeInteger(UInt16(2), endianness: .little)
        b.writeBytes([0x00, 0x01])
        var copy = b
        let off = try TDSTokenParser.parseOffsetToken(from: &copy)
        XCTAssertEqual(off.data, [0x00, 0x01])
    }

    func testParseTVPRow() throws {
        // COLMETADATA for one INT column
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .int, length: 4, collation: [], tableName: nil, colName: "c1", precision: nil, scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [col])

        // TVP_ROW: bitmap byte (0 -> not null), followed by 4 bytes little endian = 42
        var buf = ByteBufferAllocator().buffer(capacity: 5)
        buf.writeInteger(UInt8(0))
        buf.writeInteger(Int32(42), endianness: .little)
        var copy = buf
        let tvp = try TDSTokenParser.parseTVPRowToken(from: &copy, with: meta)
        XCTAssertEqual(tvp.colData.count, 1)
        var v = tvp.colData[0].data!
        XCTAssertEqual(v.readInteger(endianness: .little, as: Int32.self), 42)
    }
}

