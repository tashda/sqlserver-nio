@testable import SQLServerTDS
import XCTest
import NIO

final class TDSTokenParserVariantNullTests: XCTestCase {
    func testSQLVariantZeroLengthIsConsumedAndAlignmentPreserved() throws {
        // Build COLMETADATA for three columns: sql_variant, bit, nvarchar(max)
        let cols: [TDSTokens.ColMetadataToken.ColumnData] = [
            .init(userType: 0, flags: 0, dataType: .sqlVariant, length: 8009, collation: [], tableName: nil, colName: "v", precision: nil, scale: nil),
            .init(userType: 0, flags: 0, dataType: .bit, length: 1, collation: [], tableName: nil, colName: "b", precision: nil, scale: nil),
            .init(userType: 0, flags: 0, dataType: .nvarchar, length: 0xFFFF, collation: [], tableName: nil, colName: "s", precision: nil, scale: nil),
        ]
        let meta = TDSTokens.ColMetadataToken(count: 3, colData: cols)

        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        // sql_variant: 4-byte totalLength. Historically we observed servers emitting 0 here for catalog views.
        // Ensure the parser consumes these 4 bytes and returns nil without misalignment.
        buffer.writeInteger(UInt32(0), endianness: .little)
        // bit: fixed 1 byte payload, value = 1
        buffer.writeInteger(UInt8(1))
        // nvarchar(max): PLP unknown length (0xFE...FF) followed by 0-length chunk terminator
        buffer.writeInteger(UInt64.max &- 1, endianness: .little) // PLP_UNKNOWN total length
        buffer.writeInteger(UInt32(0), endianness: .little) // zero-length chunk -> end of PLP

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta
        let row = try XCTUnwrap(parser.parseRowToken())

        XCTAssertEqual(row.colData.count, 3)
        // sql_variant -> nil
        XCTAssertNil(row.colData[0].data)
        // bit -> 0x01
        XCTAssertEqual(row.colData[1].data?.readableBytes, 1)
        var b = row.colData[1].data!
        XCTAssertEqual(b.readInteger(as: UInt8.self), 1)
        // nvarchar(max) -> empty (non-nil) buffer
        XCTAssertEqual(row.colData[2].data?.readableBytes, 0)
        // and the overall buffer should be fully consumed
        XCTAssertEqual(stream.buffer.readableBytes, 0)
    }
}

