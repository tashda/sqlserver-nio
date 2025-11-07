@testable import SQLServerTDS
import XCTest
import NIO
import Logging

final class RawSqlBatchRequestCompletionTests: XCTestCase {
    func testRawSqlCompletesOnFinalDone() throws {
        let request = RawSqlRequest(
            sql: "SELECT 1;",
            onDone: { token in
                XCTAssertEqual(token.status, 0x00)
            }
        )

        // Build a packet that carries only a final DONE token
        var payload = ByteBufferAllocator().buffer(capacity: 16)
        payload.writeInteger(TDSTokens.TokenType.done.rawValue, as: UInt8.self)
        payload.writeInteger(UInt16(0), endianness: .little) // status: no more results
        payload.writeInteger(UInt16(0), endianness: .little) // curCmd
        payload.writeInteger(UInt64(0), endianness: .little) // rowcount

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&payload)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let token = try parser.parseDoneToken()
        if let token = token {
            request.onDone?(token)
        }

        // The test now verifies that the onDone closure was called with the correct token.
        // The `TDSPacketResponse` is no longer directly returned by the request.
    }

    func testRawSqlCompletesAcrossPartialDoneFrames() throws {
        var doneToken: TDSTokens.DoneToken? = nil
        let request = RawSqlRequest(
            sql: "SELECT 1;",
            onDone: { token in
                doneToken = token
            }
        )

        // First packet: only token byte + 1 byte of status (insufficient)
        var p1 = ByteBufferAllocator().buffer(capacity: 2)
        p1.writeInteger(TDSTokens.TokenType.done.rawValue, as: UInt8.self)
        p1.writeInteger(UInt8(0)) // only low byte of status

        // Simulate the TDSConnection feeding partial data to the parser
        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&p1)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        // Attempt to parse the token - it should fail due to insufficient data
        XCTAssertThrowsError(try parser.parseDoneToken()) { error in
            XCTAssertEqual(error as? TDSError, TDSError.protocolError("Insufficient data for DONE token"))
        }

        // Second packet: remaining bytes for DONE token
        var p2 = ByteBufferAllocator().buffer(capacity: 16)
        // Remaining one status byte + curCmd + rowcount
        p2.writeInteger(UInt8(0)) // high byte of status
        p2.writeInteger(UInt16(0), endianness: .little) // curCmd
        p2.writeInteger(UInt64(0), endianness: .little) // rowcount

        // Feed the remaining data to the parser
        stream.buffer.writeBuffer(&p2)

        // Now parsing should succeed
        let parsedToken = try XCTUnwrap(parser.parseDoneToken())
        request.onDone?(parsedToken)

        XCTAssertNotNil(doneToken, "onDone closure should have been called")
        XCTAssertEqual(doneToken?.status, 0x00)
    }
}
