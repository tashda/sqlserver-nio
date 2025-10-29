import XCTest
import NIO
@testable import SQLServerTDS

final class TDSTokenParserClrUdtFallbackTests: XCTestCase {
    private struct TimeoutError: Error {}
    private func withTimeout<T>(_ seconds: TimeInterval, _ op: @escaping () throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try op() }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
                throw TimeoutError()
            }
            let result = try await group.next()!
            group.cancelAll()
            return result
        }
    }
    func testRowTokenClrUdtFallbackReadsVarbinaryPayload() async throws {
        // Build a single-column COLMETADATA with CLR UDT type
        let column = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .clrUdt,
            length: 0, // not used by fallback
            collation: [],
            tableName: nil,
            colName: "udt",
            precision: nil,
            scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [column])

        // Value payload: USHORTCHARBINLEN(3) + 3 bytes
        var buf = ByteBufferAllocator().buffer(capacity: 5)
        buf.writeInteger(UInt16(3), endianness: .little)
        buf.writeBytes([0x01, 0x02, 0x03])

        let row = try await withTimeout(5) {
            var copy = buf
            return try TDSTokenParser.parseRowToken(from: &copy, with: meta)
        }
        XCTAssertEqual(row.colData.count, 1)
        guard var data = row.colData[0].data else {
            return XCTFail("Expected non-nil CLR UDT payload")
        }
        XCTAssertEqual(data.readableBytes, 3)
        let bytes = data.readBytes(length: 3)
        XCTAssertEqual(bytes, [0x01, 0x02, 0x03])
    }

    func testRowTokenClrUdtNull() async throws {
        let column = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .clrUdt,
            length: 0,
            collation: [],
            tableName: nil,
            colName: "udt",
            precision: nil,
            scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [column])

        // NULL payload for CLR UDT fallback: USHORTCHARBINLEN(0xFFFF)
        var buf = ByteBufferAllocator().buffer(capacity: 2)
        buf.writeInteger(UInt16(0xFFFF), endianness: .little)

        let row = try await withTimeout(5) {
            var copy = buf
            return try TDSTokenParser.parseRowToken(from: &copy, with: meta)
        }
        XCTAssertNil(row.colData[0].data)
    }
}
