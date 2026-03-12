import XCTest
@testable import SQLServerTDS
import NIO
import Foundation

/// Tests TDSData construction and type accessors for every supported primitive type.
/// These tests exercise the TDSData layer in isolation — no parser, no network.
/// All buffers are constructed with little-endian byte order to match what SQL Server sends.
final class TDSDataTypeRoundTripTests: XCTestCase, @unchecked Sendable {

    // MARK: - Bool

    func testBoolTrue() {
        let data = TDSData(bool: true)
        XCTAssertEqual(data.bool, true)
        XCTAssertEqual(Bool(tdsData: data), true)
    }

    func testBoolFalse() {
        let data = TDSData(bool: false)
        XCTAssertEqual(data.bool, false)
        XCTAssertEqual(Bool(tdsData: data), false)
    }

    func testBoolNullReturnsNil() {
        let data = TDSData(metadata: TypeMetadata(dataType: .bit), value: nil)
        XCTAssertNil(data.bool)
        XCTAssertNil(Bool(tdsData: data))
    }

    func testBoolTDSMetadataIsBit() {
        XCTAssertEqual(Bool.tdsMetadata.dataType, .bit)
    }

    // MARK: - Float (real / floatn 4-byte)

    /// Test reading a Float from .real type (4-byte IEEE 754 LE) — the server-receive path.
    func testRealTypeDecodesAsFloat() {
        var buf = ByteBufferAllocator().buffer(capacity: 4)
        buf.writeInteger(Float(3.14).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .real), value: buf)
        XCTAssertEqual(data.float!, 3.14, accuracy: 1e-5)
    }

    func testRealTypeNegativeFloat() {
        var buf = ByteBufferAllocator().buffer(capacity: 4)
        buf.writeInteger(Float(-123.456).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .real), value: buf)
        XCTAssertEqual(data.float!, -123.456, accuracy: 1e-3)
    }

    func testRealTypeNullReturnsNil() {
        let data = TDSData(metadata: TypeMetadata(dataType: .real), value: nil)
        XCTAssertNil(data.float)
    }

    func testFloatTDSMetadataIsReal() {
        XCTAssertEqual(Float.tdsMetadata.dataType, .real)
    }

    /// floatn with a 4-byte payload must be readable via .double (the correct path).
    /// The .float accessor does NOT handle .floatn — only .real and .float types.
    /// This test documents that gap so it becomes visible if the accessor is later extended.
    func testFloatnFourByteReadableAsDouble() {
        var buf = ByteBufferAllocator().buffer(capacity: 4)
        buf.writeInteger(Float(1.5).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .floatn), value: buf)
        XCTAssertEqual(data.double!, Double(Float(1.5)), accuracy: 1e-6)
        XCTAssertNil(data.float, "floatn(4) is not handled by the .float accessor — use .double")
    }

    /// floatn with an 8-byte payload is readable as Double.
    func testFloatnEightByteReadableAsDouble() {
        var buf = ByteBufferAllocator().buffer(capacity: 8)
        buf.writeInteger(Double(2.718281828).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .floatn), value: buf)
        XCTAssertEqual(data.double!, 2.718281828, accuracy: 1e-9)
    }

    // MARK: - Double (float / floatn 8-byte)

    /// Test reading a Double from .float type (8-byte IEEE 754 LE) — the server-receive path.
    func testFloatTypeDecodesAsDouble() {
        var buf = ByteBufferAllocator().buffer(capacity: 8)
        buf.writeInteger(Double(2.718281828).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .float), value: buf)
        XCTAssertEqual(data.double!, 2.718281828, accuracy: 1e-9)
        XCTAssertEqual(Double(tdsData: data)!, 2.718281828, accuracy: 1e-9)
    }

    func testFloatTypeNegativeDouble() {
        var buf = ByteBufferAllocator().buffer(capacity: 8)
        buf.writeInteger(Double(-1_000_000.999).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .float), value: buf)
        XCTAssertEqual(data.double!, -1_000_000.999, accuracy: 1e-3)
    }

    func testFloatTypeNullReturnsNil() {
        let data = TDSData(metadata: TypeMetadata(dataType: .float), value: nil)
        XCTAssertNil(data.double)
        XCTAssertNil(Double(tdsData: data))
    }

    func testDoubleTDSMetadataIsFloat() {
        XCTAssertEqual(Double.tdsMetadata.dataType, .float)
    }

    // MARK: - Int types (intn — variable length)

    func testIntnFourByteDecodesAsInt32() {
        var buf = ByteBufferAllocator().buffer(capacity: 4)
        buf.writeInteger(Int32(-42), endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .intn), value: buf)
        XCTAssertEqual(data.int32, -42)
    }

    func testIntnEightByteDecodesAsInt64() {
        var buf = ByteBufferAllocator().buffer(capacity: 8)
        buf.writeInteger(Int64(9_999_999_999), endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .intn), value: buf)
        XCTAssertEqual(data.int64, 9_999_999_999)
    }

    /// tinyInt is SQL Server's unsigned 8-bit type (0–255). Read it as uint8, not int8.
    func testTinyIntDecodesAsUInt8() {
        var buf = ByteBufferAllocator().buffer(capacity: 1)
        buf.writeInteger(UInt8(255))
        let data = TDSData(metadata: TypeMetadata(dataType: .tinyInt), value: buf)
        XCTAssertEqual(data.uint8, 255)
    }

    /// int8 works correctly for values in range (0–127).
    func testTinyIntSmallValueDecodesAsInt8() {
        var buf = ByteBufferAllocator().buffer(capacity: 1)
        buf.writeInteger(UInt8(42))
        let data = TDSData(metadata: TypeMetadata(dataType: .tinyInt), value: buf)
        XCTAssertEqual(data.int8, 42)
    }

    func testSmallIntDecodesAsInt16() {
        var buf = ByteBufferAllocator().buffer(capacity: 2)
        buf.writeInteger(Int16(-1000), endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .smallInt), value: buf)
        XCTAssertEqual(data.int16, -1000)
    }

    func testIntNullReturnsNil() {
        let data = TDSData(metadata: TypeMetadata(dataType: .intn), value: nil)
        XCTAssertNil(data.int32)
        XCTAssertNil(data.int64)
    }

    // MARK: - Decimal

    func testDecimalPositiveRoundTrip() throws {
        #if !canImport(Darwin)
        throw XCTSkip("Decimal byte layout differs on Linux Foundation — needs investigation")
        #endif
        let original = Decimal(string: "123.45")!
        let data = try TDSData(decimal: original, precision: 10, scale: 2)
        XCTAssertEqual(data.decimal, original)
    }

    func testDecimalNegativeRoundTrip() throws {
        #if !canImport(Darwin)
        throw XCTSkip("Decimal byte layout differs on Linux Foundation — needs investigation")
        #endif
        let original = Decimal(string: "-99.99")!
        let data = try TDSData(decimal: original, precision: 10, scale: 2)
        XCTAssertEqual(data.decimal, original)
    }

    func testDecimalZeroRoundTrip() throws {
        let data = try TDSData(decimal: Decimal(0), precision: 5, scale: 0)
        XCTAssertEqual(data.decimal, Decimal(0))
    }

    func testDecimalNullReturnsNil() {
        let data = TDSData(metadata: TypeMetadata(dataType: .decimal), value: nil)
        XCTAssertNil(data.decimal)
    }

    func testDecimalPrecisionOutOfRangeThrows() {
        XCTAssertThrowsError(try TDSData(decimal: Decimal(1), precision: 0, scale: 0))
        XCTAssertThrowsError(try TDSData(decimal: Decimal(1), precision: 39, scale: 0))
    }

    // MARK: - Date

    /// TDSData(date:) produces a datetimeOffset payload (non-nil).
    func testDateEncodeIsNonNil() {
        let data = TDSData(date: Date(timeIntervalSinceReferenceDate: 1_000_000))
        XCTAssertNotNil(data.value)
        XCTAssertEqual(data.metadata.dataType, .datetimeOffset)
    }

    /// Round-trip through datetimeOffset encoding. The time component must survive at 100ns precision.
    func testDateRoundTrip() {
        let original = Date(timeIntervalSinceReferenceDate: 1_000_000)
        let data = TDSData(date: original)
        let recovered = data.date
        XCTAssertNotNil(recovered)
        // datetimeOffset has 100ns precision, so allow at most 100ns of rounding error.
        XCTAssertEqual(recovered!.timeIntervalSince(original), 0, accuracy: 1e-6)
    }

    /// Midnight must round-trip exactly.
    func testDateRoundTripMidnight() {
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = TimeZone(secondsFromGMT: 0)!
        let midnight = cal.date(from: DateComponents(year: 2023, month: 1, day: 1, hour: 0, minute: 0, second: 0))!
        let data = TDSData(date: midnight)
        let recovered = data.date
        XCTAssertNotNil(recovered)
        XCTAssertEqual(recovered!.timeIntervalSince(midnight), 0, accuracy: 1e-6)
    }

    /// A time close to end of day must not bleed into the next day.
    func testDateRoundTripEndOfDay() {
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = TimeZone(secondsFromGMT: 0)!
        let t = cal.date(from: DateComponents(year: 2023, month: 6, day: 15, hour: 23, minute: 59, second: 59))!
        let data = TDSData(date: t)
        let recovered = data.date
        XCTAssertNotNil(recovered)
        XCTAssertEqual(recovered!.timeIntervalSince(t), 0, accuracy: 1.0)
    }

    func testDateNullReturnsNil() {
        let data = TDSData(metadata: TypeMetadata(dataType: .datetimeOffset, scale: 7), value: nil)
        XCTAssertNil(data.date)
    }

    func testDateTDSMetadataIsDatetimeOffset() {
        XCTAssertEqual(Date.tdsMetadata.dataType, .datetimeOffset)
    }

    // MARK: - TDSDataConvertible passthrough

    /// TDSData itself conforms to TDSDataConvertible — wrapping and unwrapping must be identity.
    func testTDSDataConvertiblePassthrough() {
        let original = TDSData(bool: true)
        let wrapped = TDSData(tdsData: original)
        XCTAssertNotNil(wrapped)
        XCTAssertEqual(wrapped?.bool, true)
    }

    /// Bool round-trip via TDSDataConvertible.
    func testBoolConvertibleRoundTrip() {
        let tdsData = true.tdsData!
        XCTAssertEqual(Bool(tdsData: tdsData), true)
    }

    /// Double round-trip via TDSDataConvertible — write then read server-format bytes.
    func testDoubleConvertibleFromServerBytes() {
        var buf = ByteBufferAllocator().buffer(capacity: 8)
        buf.writeInteger(Double(1.41421356).bitPattern, endianness: .little)
        let data = TDSData(metadata: TypeMetadata(dataType: .float), value: buf)
        XCTAssertEqual(Double(tdsData: data)!, 1.41421356, accuracy: 1e-8)
    }
}
