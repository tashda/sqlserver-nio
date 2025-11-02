@testable import SQLServerKit
import XCTest
import SQLServerTDS

final class SQLServerReturnValueDecodeTests: XCTestCase {
    func testTypedDecodersFromTDSData() {
        // Build a TDSData representing INT32(456)
        var buf = ByteBufferAllocator().buffer(capacity: 4)
        buf.writeInteger(Int32(456), endianness: .little)
        let meta = TypeMetadata(userType: 0, flags: 0, dataType: .int, collation: [], precision: nil, scale: nil)
        let data = TDSData(metadata: meta, value: buf)
        let rv = SQLServerReturnValue(name: "@X", status: 0, value: data)
        XCTAssertEqual(rv.int, 456)

        // NVARCHAR string
        var s = ByteBufferAllocator().buffer(capacity: 16)
        let str = "hello"
        let sBytes = Array(str.utf16.flatMap { [UInt8($0 & 0xFF), UInt8(($0 >> 8) & 0xFF)] })
        s.writeBytes(sBytes)
        let smeta = TypeMetadata(userType: 0, flags: 0, dataType: .nvarchar, collation: [0,0,0,0,0], precision: nil, scale: nil)
        let sdata = TDSData(metadata: smeta, value: s)
        let srv = SQLServerReturnValue(name: "@S", status: 0, value: sdata)
        XCTAssertEqual(srv.string, str)
    }
}

