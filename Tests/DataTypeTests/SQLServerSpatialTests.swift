import XCTest
import NIOCore
import SQLServerKit

final class SQLServerSpatialTests: XCTestCase {
    func testDecodePoint() throws {
        // [MS-SSCLRSPS] Version 1, Single Point
        // SRID (4 bytes) = 4326
        // Version (1 byte) = 1
        // SerializationProps (1 byte) = 0x08 (IsSinglePoint)
        // X (8 bytes) = 10.0
        // Y (8 bytes) = 20.0
        
        var buffer = ByteBufferAllocator().buffer(capacity: 22)
        buffer.writeInteger(Int32(4326), endianness: .little)
        buffer.writeInteger(UInt8(1))
        buffer.writeInteger(UInt8(0x08))
        buffer.writeInteger(10.0.bitPattern, endianness: .little)
        buffer.writeInteger(20.0.bitPattern, endianness: .little)
        
        let spatial = try XCTUnwrap(SQLServerSpatial.decode(from: &buffer))
        
        XCTAssertEqual(spatial.srid, 4326)
        XCTAssertEqual(spatial.type, .point)
        XCTAssertEqual(spatial.points.count, 1)
        XCTAssertEqual(spatial.points[0].x, 10.0)
        XCTAssertEqual(spatial.points[0].y, 20.0)
        XCTAssertEqual(spatial.wkt, "POINT(10 20)")
    }
}
