import XCTest
@testable import SQLServerTDS
import NIO

final class TDSTokenParserVarlenNullTests: XCTestCase {
    private func makeMeta() -> TDSTokens.ColMetadataToken {
        let col0 = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .nvarchar,
            length: 256, // maxLen from TYPE_INFO
            collation: [],
            tableName: nil,
            colName: "DATABASE_NAME",
            precision: nil,
            scale: nil
        )
        let col1 = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .intn,
            length: 4,
            collation: [],
            tableName: nil,
            colName: "DATABASE_SIZE",
            precision: nil,
            scale: nil
        )
        let col2 = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .varchar,
            length: 254,
            collation: [],
            tableName: nil,
            colName: "REMARKS",
            precision: nil,
            scale: nil
        )
        return TDSTokens.ColMetadataToken(count: 3, colData: [col0, col1, col2])
    }

    func testVarcharNullSentinelLen16ConsumesAndReturnsNil() throws {
        // Arrange row: NVARCHAR("Adventure"), INTN(4->123), VARCHAR(NULL via 0xFFFF)
        var buf = ByteBufferAllocator().buffer(capacity: 128)
        // nvarchar value: length in bytes (UInt16) then UTF-16LE payload
        let s = "Adventure"
        let utf16ByteCount = s.utf16.count * 2
        buf.writeInteger(UInt16(utf16ByteCount), endianness: .little)
        buf.writeUTF16String(s)
        // intn: ByteLen=4 + Int32 value
        buf.writeInteger(UInt8(4))
        buf.writeInteger(Int32(123), endianness: .little)
        // varchar: USHORTCHARBINLEN = 0xFFFF indicates NULL
        buf.writeInteger(UInt16.max, endianness: .little)

        var copy = buf
        let meta = makeMeta()
        let row = try TDSTokenParser.parseRowToken(from: &copy, with: meta)
        XCTAssertEqual(row.colData.count, 3)

        // col0 string
        var c0 = row.colData[0].data!
        let len0 = c0.readableBytes
        XCTAssertEqual(len0, utf16ByteCount)
        // sanity decode back to string
        let str0 = String(bytes: c0.readBytes(length: len0)!, encoding: .utf16LittleEndian)
        XCTAssertEqual(str0, s)

        // col1 intn
        var c1 = row.colData[1].data!
        let v1 = c1.readInteger(endianness: .little, as: Int32.self)
        XCTAssertEqual(v1, 123)

        // col2 NULL
        XCTAssertNil(row.colData[2].data)
    }

    func testVarcharLegacyNullSentinelLen8ConsumesAndReturnsNil() throws {
        // Same shape but with legacy VARCHAR (1-byte length with 0xFF = NULL)
        var buf = ByteBufferAllocator().buffer(capacity: 64)
        // nvarchar("A") -> len=2 bytes + UTF-16LE 'A'
        buf.writeInteger(UInt16(2), endianness: .little)
        buf.writeUTF16String("A")
        // intn: ByteLen=4 + value 0
        buf.writeInteger(UInt8(4))
        buf.writeInteger(Int32(0), endianness: .little)
        // varcharLegacy: len8=0xFF => NULL
        buf.writeInteger(UInt8(0xFF))

        let col0 = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .nvarchar, length: 256,
            collation: [], tableName: nil, colName: "s", precision: nil, scale: nil
        )
        let col1 = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .intn, length: 4,
            collation: [], tableName: nil, colName: "i", precision: nil, scale: nil
        )
        let col2 = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .varcharLegacy, length: 255,
            collation: [], tableName: nil, colName: "v", precision: nil, scale: nil
        )
        var copy = buf
        let meta = TDSTokens.ColMetadataToken(count: 3, colData: [col0, col1, col2])
        let row = try TDSTokenParser.parseRowToken(from: &copy, with: meta)
        XCTAssertEqual(row.colData.count, 3)
        XCTAssertNil(row.colData[2].data)
    }
}

