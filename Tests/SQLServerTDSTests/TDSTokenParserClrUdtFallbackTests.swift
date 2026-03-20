import XCTest
import NIO
@testable import SQLServerTDS

final class TDSTokenOperationsClrUdtFallbackTests: XCTestCase, @unchecked Sendable {
    private struct TimeoutError: Error {}
    private func withTimeout<T: Sendable>(_ seconds: TimeInterval, _ op: @escaping @Sendable () throws -> T) async throws -> T {
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
        var buffer = ByteBufferAllocator().buffer(capacity: 6)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt16(3), endianness: .little)
        buffer.writeBytes([0x01, 0x02, 0x03])
        let encoded = buffer
        let row = try await withTimeout(5) {
            var localBuffer = encoded
            let stream = TDSStreamParser()
            stream.buffer.writeBuffer(&localBuffer)
            let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
            parser.colMetadata = meta
            return try XCTUnwrap(parser.parseRowToken())
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
        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt16(0xFFFF), endianness: .little)

        let encoded = buffer
        let row = try await withTimeout(5) {
            var localBuffer = encoded
            let stream = TDSStreamParser()
            stream.buffer.writeBuffer(&localBuffer)
            let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
            parser.colMetadata = meta
            return try XCTUnwrap(parser.parseRowToken())
        }
        XCTAssertNil(row.colData[0].data)
    }

    func testColMetadataClrUdtConsumesFullTypeInfo() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 256)
        buffer.writeInteger(TDSTokens.TokenType.colMetadata.rawValue)
        buffer.writeInteger(UInt16(2), endianness: .little)

        // Column 1: hierarchyid CLR UDT
        buffer.writeInteger(UInt32(0), endianness: .little) // userType
        buffer.writeInteger(UInt16(0), endianness: .little) // flags
        buffer.writeInteger(TDSDataType.clrUdt.rawValue)
        buffer.writeInteger(UInt16(128), endianness: .little) // max length
        writeBUsVar("sys", to: &buffer)
        writeBUsVar("dbo", to: &buffer)
        writeBUsVar("hierarchyid", to: &buffer)
        writeUSVar("Microsoft.SqlServer.Types", to: &buffer)
        writeBUsVar("OrganizationNode", to: &buffer)

        // Column 2: regular INT column that previously got desynchronized
        buffer.writeInteger(UInt32(0), endianness: .little)
        buffer.writeInteger(UInt16(0), endianness: .little)
        buffer.writeInteger(TDSDataType.int.rawValue)
        writeBUsVar("BusinessEntityID", to: &buffer)

        var copy = buffer
        let token = try TDSTokenOperations.parseColMetadataToken(from: &copy)

        XCTAssertEqual(token.colData.count, 2)
        XCTAssertEqual(token.colData[0].dataType, .clrUdt)
        XCTAssertEqual(token.colData[0].colName, "OrganizationNode")
        XCTAssertEqual(token.colData[0].udtInfo?.typeName, "hierarchyid")
        XCTAssertEqual(token.colData[0].udtInfo?.schemaName, "dbo")
        XCTAssertEqual(token.colData[1].dataType, .int)
        XCTAssertEqual(token.colData[1].colName, "BusinessEntityID")
    }

    private func writeBUsVar(_ value: String, to buffer: inout ByteBuffer) {
        buffer.writeInteger(UInt8(value.utf16.count))
        buffer.writeBytes(utf16leBytes(for: value))
    }

    private func writeUSVar(_ value: String, to buffer: inout ByteBuffer) {
        buffer.writeInteger(UInt16(value.utf16.count), endianness: .little)
        buffer.writeBytes(utf16leBytes(for: value))
    }

    private func utf16leBytes(for value: String) -> [UInt8] {
        value.utf16.flatMap { codeUnit in
            [UInt8(codeUnit & 0x00ff), UInt8((codeUnit & 0xff00) >> 8)]
        }
    }
}
