import Foundation
@testable import SQLServerKit
import SQLServerTDS
import Testing

/// Values converted on the client for a bulk load, checked against the wire format SQL Server uses.
@Suite struct BulkValueEncoderTests {
    private static let latin1: [UInt8] = [0x09, 0x04, 0xD0, 0x00, 0x34]
    private static let utf8Collation: [UInt8] = [0x09, 0x04, 0xD0, 0x04, 0x00]

    private func column(_ type: TDSDataType, length: Int32 = 0, precision: UInt8 = 0, scale: UInt8 = 0, collation: [UInt8] = []) -> TDSColumnMetadata {
        TDSColumnMetadata(userType: 0, flags: 0x0009, dataType: type, length: length, precision: precision, scale: scale,
                          collation: collation, colName: "c")
    }

    private func encode(_ value: SQLServerLiteralValue, _ column: TDSColumnMetadata) throws -> [UInt8]? {
        try SQLServerBulkValueEncoder.encode(value, for: column)
    }

    @Test func integersFromNumbersAndText() throws {
        #expect(try encode(.int(12345), column(.intn, length: 4)) == [0x39, 0x30, 0, 0])
        #expect(try encode(.string(" 12345 "), column(.int)) == [0x39, 0x30, 0, 0])
        #expect(try encode(.string("-1"), column(.intn, length: 8)) == [UInt8](repeating: 0xFF, count: 8))
        #expect(try encode(.string("7.0"), column(.intn, length: 2)) == [7, 0])
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("256"), column(.intn, length: 1)) }
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("7.5"), column(.int)) }
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("abc"), column(.int)) }
    }

    @Test func nullIsNilWhateverTheType() throws {
        #expect(try encode(.null, column(.intn, length: 4)) == nil)
        #expect(try encode(.null, column(.nvarchar, length: 20)) == nil)
    }

    @Test func bits() throws {
        #expect(try encode(.string("true"), column(.bitn, length: 1)) == [1])
        #expect(try encode(.string("0"), column(.bitn, length: 1)) == [0])
        #expect(try encode(.bool(true), column(.bit)) == [1])
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("maybe"), column(.bitn, length: 1)) }
    }

    @Test func decimalsAreScaledAndRounded() throws {
        let money10_2 = column(.decimal, length: 9, precision: 10, scale: 2)
        #expect(try encode(.decimal("123.45"), money10_2) == [1, 0x39, 0x30, 0, 0, 0, 0, 0, 0])
        #expect(try encode(.string("-0.005"), money10_2) == [0, 1, 0, 0, 0, 0, 0, 0, 0])
        #expect(try encode(.string("1.5e2"), money10_2) == [1, 0x98, 0x3A, 0, 0, 0, 0, 0, 0]) // 15000
        #expect(try encode(.string("-0.001"), money10_2) == [1, 0, 0, 0, 0, 0, 0, 0, 0]) // no negative zero
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("123456789.00"), money10_2) }
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("1,5"), money10_2) }
        // decimal(38, 0) fills 16 bytes.
        let big = try #require(try encode(.string("99999999999999999999999999999999999999"), column(.decimal, length: 17, precision: 38)))
        #expect(big.count == 17)
    }

    @Test func money() throws {
        // 12.3456 is 123456 units of 1/10000: the high 32 bits, then the low 32 bits.
        #expect(try encode(.string("12.3456"), column(.moneyn, length: 8)) == [0, 0, 0, 0, 0x40, 0xE2, 0x01, 0x00])
        #expect(try encode(.string("$1,000"), column(.smallMoney)) == [0x80, 0x96, 0x98, 0x00])
        #expect(try encode(.string("-1"), column(.money)) == [0xFF, 0xFF, 0xFF, 0xFF, 0xF0, 0xD8, 0xFF, 0xFF])
    }

    @Test func floats() throws {
        #expect(try encode(.string("1.5"), column(.floatn, length: 8)) == Array(withUnsafeBytes(of: 1.5.bitPattern.littleEndian) { Array($0) }))
        #expect(try encode(.double(1.5), column(.real)) == Array(withUnsafeBytes(of: Float(1.5).bitPattern.littleEndian) { Array($0) }))
    }

    @Test func datesAndTimes() throws {
        #expect(try encode(.string("0001-01-01"), column(.date)) == [0, 0, 0])
        #expect(try encode(.string("2000-01-01"), column(.date)) == [0x07, 0x24, 0x0B]) // 730119
        #expect(try encode(.string("9999-12-31"), column(.date)) == [0xDA, 0xB9, 0x37]) // 3652058
        // time(7): 45296 seconds and 1234567 ticks of 100 ns.
        let ticks: UInt64 = 45_296 * 10_000_000 + 1_234_567
        #expect(try encode(.string("12:34:56.1234567"), column(.time, scale: 7)) == (0..<5).map { UInt8(truncatingIfNeeded: ticks >> (8 * $0)) })
        // time(0) rounds to the second.
        #expect(try encode(.string("00:00:01.6"), column(.time, scale: 0)) == [2, 0, 0])
        #expect(try encode(.string("2000-01-01T00:00:00"), column(.datetime2, scale: 0)) == [0, 0, 0, 0x07, 0x24, 0x0B])
        // datetimeoffset stores UTC plus the offset in minutes.
        let offset = try #require(try encode(.string("2000-01-02 01:00:00 +02:00"), column(.datetimeOffset, scale: 0)))
        // 23:00 on 2000-01-01, then +120 minutes. Typed explicitly: on Linux the concatenated
        // literals type-check differently and the comparison failed with equal-looking bytes.
        let expectedOffset: [UInt8] = [0x70, 0x43, 0x01, 0x07, 0x24, 0x0B, 120, 0]
        #expect(offset == expectedOffset)
        // datetime: days since 1900 and 1/300 s ticks.
        #expect(try encode(.string("1900-01-02 00:00:01"), column(.datetimen, length: 8)) == [1, 0, 0, 0, 0x2C, 0x01, 0, 0])
        #expect(try encode(.string("1900-01-01 00:00:30"), column(.smallDateTime)) == [0, 0, 1, 0])
        // Rounding up to midnight carries into the next day (or wraps, for time).
        #expect(try encode(.string("2000-01-01 23:59:59.9"), column(.datetime2, scale: 0)) == [0, 0, 0, 0x08, 0x24, 0x0B])
        #expect(try encode(.string("23:59:59.9"), column(.time, scale: 0)) == [0, 0, 0])
        #expect(try encode(.string("1900-01-01 23:59:59.999"), column(.datetime)) == [1, 0, 0, 0, 0, 0, 0, 0])
        #expect(try encode(.string("1900-01-01 23:59:45"), column(.smallDateTime)) == [1, 0, 0, 0])
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("2023-02-29"), column(.date)) }
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("31/12/2023"), column(.date)) }
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("1700-01-01"), column(.datetime)) }
    }

    @Test func dateValuesAreWrittenInUTC() throws {
        let date = Date(timeIntervalSince1970: 946_684_800) // 2000-01-01T00:00:00Z
        #expect(try encode(.date(date), column(.datetime2, scale: 0)) == [0, 0, 0, 0x07, 0x24, 0x0B])
    }

    @Test func uniqueidentifierUsesTheMixedEndianLayout() throws {
        let expected: [UInt8] = [0xFF, 0x19, 0x96, 0x6F, 0x86, 0x8B, 0x11, 0xD0, 0xB4, 0x2D, 0x00, 0xC0, 0x4F, 0xC9, 0x64, 0xFF]
        #expect(try encode(.string("6F9619FF-8B86-D011-B42D-00C04FC964FF"), column(.guid, length: 16)) == expected)
        #expect(try encode(.string("{6f9619ff-8b86-d011-b42d-00c04fc964ff}"), column(.guid, length: 16)) == expected)
    }

    @Test func textInTheColumnsEncoding() throws {
        #expect(try encode(.nString("é"), column(.nvarchar, length: 20)) == [0xE9, 0x00])
        #expect(try encode(.string("é"), column(.varchar, length: 20, collation: Self.latin1)) == [0xE9])
        #expect(try encode(.string("é"), column(.varchar, length: 20, collation: Self.utf8Collation)) == [0xC3, 0xA9])
        #expect(try encode(.string(""), column(.nvarchar, length: 20)) == [])
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("abc"), column(.nvarchar, length: 4)) }
        #expect(try encode(.string(String(repeating: "x", count: 10_000)), column(.nvarchar, length: 0xFFFF))?.count == 20_000)
    }

    @Test func binaryFromBytesOrHex() throws {
        #expect(try encode(.bytes([1, 2]), column(.varbinary, length: 10)) == [1, 2])
        #expect(try encode(.string("0xDEAD"), column(.varbinary, length: 10)) == [0xDE, 0xAD])
        #expect(throws: SQLServerBulkValueEncoder.ConversionError.self) { try encode(.string("0xABC"), column(.varbinary, length: 10)) }
    }

    @Test func insertBulkDeclaresEachColumnsType() {
        let cases: [(TDSColumnMetadata, String)] = [
            (column(.intn, length: 2), "smallint"),
            (column(.nvarchar, length: 100), "nvarchar(50)"),
            (column(.nvarchar, length: 0xFFFF), "nvarchar(max)"),
            (column(.varchar, length: 8000), "varchar(8000)"),
            (column(.decimal, length: 9, precision: 10, scale: 2), "decimal(10, 2)"),
            (column(.datetimeOffset, scale: 3), "datetimeoffset(3)"),
            (column(.moneyn, length: 4), "smallmoney"),
            (column(.guid, length: 16), "uniqueidentifier"),
        ]
        for (column, expected) in cases {
            #expect(SQLServerConnection.bulkTypeName(column) == expected)
        }
        #expect(SQLServerConnection.bulkTypeName(column(.sqlVariant, length: 8016)) == nil)
    }

    @Test func insertBulkHints() {
        var options = SQLServerBulkCopyOptions(table: "t", columns: ["a"])
        #expect(SQLServerConnection.insertBulkStatement(options, declarations: ["[a] int"])
            == "INSERT BULK [dbo].[t] ([a] int) WITH (CHECK_CONSTRAINTS, FIRE_TRIGGERS, KEEP_NULLS)")
        options.checkConstraints = false
        options.fireTriggers = false
        options.keepNulls = false
        #expect(SQLServerConnection.insertBulkStatement(options, declarations: ["[a] int"]) == "INSERT BULK [dbo].[t] ([a] int)")
        options.tableLock = true
        options.identityInsert = true
        #expect(SQLServerConnection.insertBulkStatement(options, declarations: ["[a] int"])
            == "INSERT BULK [dbo].[t] ([a] int) WITH (TABLOCK)")
    }

    @Test func textTheClientCannotReadGoesToTheServerAsText() throws {
        var date = column(.date)
        date.colName = "booked"
        var id = column(.intn, length: 4)
        id.colName = "id"
        let rows: [[SQLServerLiteralValue]] = [[.string("2024-02-29"), .int(1)], [.string("12/31/2023"), .int(2)]]
        let wire = try SQLServerConnection.wireColumns(for: rows, columns: [date, id])
        #expect(wire.map(\.sentAsText) == [true, false])
        #expect(wire[0].column.dataType == .nvarchar)
        #expect(wire[0].column.length == 8000)
        #expect(SQLServerConnection.bulkTypeName(wire[0].column) == "nvarchar(4000)")
        #expect(try wire[0].encode(.string("12/31/2023")) == Array("12/31/2023".utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] }))
        #expect(try wire[0].encode(.null) == nil)
        #expect(try wire[1].encode(.int(2)) == [2, 0, 0, 0])
    }

    @Test func aValueThatIsNotTextAndDoesNotConvertNamesTheRowAndColumn() {
        var price = column(.intn, length: 4)
        price.colName = "price"
        do {
            _ = try SQLServerConnection.wireColumns(for: [[.int(1)], [.bytes([1, 2])]], columns: [price])
            Issue.record("expected a conversion error")
        } catch {
            #expect((error as? LocalizedError)?.errorDescription == "Row 2, column price: '0x0102' is not a whole number.")
        }
    }
}
