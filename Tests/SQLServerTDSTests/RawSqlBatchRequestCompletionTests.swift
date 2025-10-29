@testable import SQLServerTDS
import XCTest
import NIO
import Logging

final class RawSqlBatchRequestCompletionTests: XCTestCase {
    func testRawSqlCompletesOnFinalDone() throws {
        let logger = Logger(label: "tds.rawsql.done.test")
        let request = RawSqlBatchRequest(
            sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: "SELECT 1;"),
            logger: logger,
            onRow: nil,
            connection: nil
        )

        // Build a packet that carries only a final DONE token
        var payload = ByteBufferAllocator().buffer(capacity: 16)
        payload.writeInteger(TDSTokens.TokenType.done.rawValue, as: UInt8.self)
        payload.writeInteger(UInt16(0), endianness: .little) // status: no more results
        payload.writeInteger(UInt16(0), endianness: .little) // curCmd
        payload.writeInteger(UInt64(0), endianness: .little) // rowcount

        let packet = TDSPacket(from: &payload, ofType: .tabularResult, isLastPacket: true, packetId: 1, allocator: ByteBufferAllocator())

        let response = try request.handle(packet: packet, allocator: ByteBufferAllocator())
        switch response {
        case .done:
            break // expected
        default:
            XCTFail("Expected .done response, got: \(response)")
        }
    }

    func testRawSqlCompletesAcrossPartialDoneFrames() throws {
        let logger = Logger(label: "tds.rawsql.partial.test")
        let request = RawSqlBatchRequest(
            sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: "SELECT 1;"),
            logger: logger,
            onRow: nil,
            connection: nil
        )

        // First packet: only token byte + 1 byte of status (insufficient)
        var p1 = ByteBufferAllocator().buffer(capacity: 2)
        p1.writeInteger(TDSTokens.TokenType.done.rawValue, as: UInt8.self)
        p1.writeInteger(UInt8(0)) // only low byte of status
        let pkt1 = TDSPacket(from: &p1, ofType: .tabularResult, isLastPacket: false, packetId: 1, allocator: ByteBufferAllocator())
        let r1 = try request.handle(packet: pkt1, allocator: ByteBufferAllocator())
        switch r1 {
        case .continue:
            break // expected until we receive the rest
        default:
            XCTFail("Expected .continue for partial frame, got: \(r1)")
        }

        // Second packet: remaining bytes for DONE token
        var p2 = ByteBufferAllocator().buffer(capacity: 16)
        // Remaining one status byte + curCmd + rowcount
        p2.writeInteger(UInt8(0)) // high byte of status
        p2.writeInteger(UInt16(0), endianness: .little) // curCmd
        p2.writeInteger(UInt64(0), endianness: .little) // rowcount
        let pkt2 = TDSPacket(from: &p2, ofType: .tabularResult, isLastPacket: true, packetId: 2, allocator: ByteBufferAllocator())
        let r2 = try request.handle(packet: pkt2, allocator: ByteBufferAllocator())
        switch r2 {
        case .done:
            break // expected now that token is complete
        default:
            XCTFail("Expected .done after completing token, got: \(r2)")
        }
    }
}
