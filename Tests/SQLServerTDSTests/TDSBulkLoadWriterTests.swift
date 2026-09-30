import XCTest
import NIOCore
@testable import SQLServerTDS

/// The bulk-load payload uses the same COLMETADATA, ROW and DONE tokens the server sends, so the
/// driver's own parser reads it back.
final class TDSBulkLoadWriterTests: XCTestCase {
    private func column(_ name: String, _ type: TDSDataType, length: Int32 = 0, precision: UInt8? = nil, scale: UInt8? = nil,
                        collation: [UInt8] = []) -> TDSColumnMetadata {
        TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0x0009, dataType: type, length: length, collation: collation,
            tableName: nil, colName: name, precision: precision, scale: scale
        )
    }

    private let latin1: [UInt8] = [0x09, 0x04, 0xD0, 0x00, 0x34]

    private func readBack(_ payload: ByteBuffer) throws -> (columns: [TDSColumnMetadata], rows: [[ByteBuffer?]], done: TDSTokens.DoneToken) {
        var buffer = payload
        let metadata = try TDSTokenOperations.parseColMetadataToken(from: &buffer)
        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = metadata
        var rows: [[ByteBuffer?]] = []
        while stream.buffer.getInteger(at: stream.position, as: UInt8.self) == TDSTokens.TokenType.row.rawValue {
            rows.append(try XCTUnwrap(parser.parseRowToken()).colData.map(\.data))
        }
        let done = try XCTUnwrap(parser.parseDoneToken())
        return (metadata.colData, rows, done)
    }

    func testColumnsRowsAndDoneReadBack() throws {
        let columns = [
            column("id", .intn, length: 4),
            column("name", .nvarchar, length: 100, collation: latin1),
            column("code", .varchar, length: 10, collation: latin1),
            column("amount", .decimal, length: 9, precision: 10, scale: 2),
            column("at", .datetime2, precision: 0, scale: 7),
            column("doc", .varbinary, length: 0xFFFF),
            column("notes", .nvarchar, length: 0xFFFF, collation: latin1),
        ]
        var writer = TDSBulkLoadWriter(columns: columns)
        let name = Array("Ærø".utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] })
        try writer.appendRow([[1, 0, 0, 0], name, Array("AB".utf8), [1, 0x39, 0x30, 0, 0, 0, 0, 0, 0],
                              [0, 0, 0, 0, 0, 1, 2, 3], [0xDE, 0xAD], []])
        try writer.appendRow([nil, nil, nil, nil, nil, nil, nil])
        let (readColumns, rows, done) = try readBack(writer.finished())

        XCTAssertEqual(readColumns.map(\.colName), columns.map(\.colName))
        XCTAssertEqual(readColumns.map(\.dataType), columns.map(\.dataType))
        XCTAssertEqual(readColumns[1].collation, latin1)
        XCTAssertEqual(readColumns[3].precision, 10)
        XCTAssertEqual(readColumns[4].scale, 7)
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows[0].map { $0.map { Array($0.readableBytesView) } },
                       [[1, 0, 0, 0], name, Array("AB".utf8), [1, 0x39, 0x30, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 1, 2, 3], [0xDE, 0xAD], []])
        XCTAssertTrue(rows[1].allSatisfy { $0 == nil })
        XCTAssertEqual(done.status & 0x0010, 0x0010)
        XCTAssertEqual(done.doneRowCount, 2)
    }

    func testRowWithTheWrongNumberOfValuesIsRefused() {
        var writer = TDSBulkLoadWriter(columns: [column("id", .intn, length: 4)])
        XCTAssertThrowsError(try writer.appendRow([[1, 0, 0, 0], nil]))
    }

    func testValueLongerThanTheColumnIsRefused() {
        var writer = TDSBulkLoadWriter(columns: [column("code", .varchar, length: 2, collation: latin1)])
        XCTAssertThrowsError(try writer.appendRow([Array("ABC".utf8)]))
    }

    func testTypesThatNeedTextPointersOrClrValuesAreNotSupported() {
        XCTAssertFalse(TDSBulkLoadWriter.supports(column("t", .text, length: 0x7FFFFFFF)))
        XCTAssertFalse(TDSBulkLoadWriter.supports(column("v", .sqlVariant, length: 8016)))
        XCTAssertFalse(TDSBulkLoadWriter.supports(column("g", .clrUdt, length: 0xFFFF)))
        XCTAssertTrue(TDSBulkLoadWriter.supports(column("x", .xml)))
        XCTAssertTrue(TDSBulkLoadWriter.supports(column("n", .nvarchar, length: 0xFFFF)))
    }

    func testUTF8CollationsUseUTF8() {
        // Latin1_General_100_CI_AS_SC_UTF8: fUTF8 is bit 26 (byte 3, 0x04).
        XCTAssertEqual(TDSCollation.encoding(from: [0x09, 0x04, 0xD0, 0x04, 0x00]), .utf8)
        XCTAssertEqual(TDSCollation.encoding(from: [0x09, 0x04, 0xD0, 0x00, 0x00]), .windowsCP1252)
    }
}
