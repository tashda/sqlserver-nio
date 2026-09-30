import XCTest
import NIOCore
@testable import SQLServerTDS

final class PreloginResponseSafetyTests: XCTestCase, @unchecked Sendable {
    func testMalformedOffsetFailsWithoutMovingReaderOutOfBounds() {
        var buffer = ByteBufferAllocator().buffer(capacity: 24)
        buffer.writeBytes([
            0x00, 0xFF, 0xFF, 0x00, 0x06, // VERSION points beyond message
            0x01, 0x00, 0x11, 0x00, 0x01, // ENCRYPTION
            0xFF,
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01
        ])

        XCTAssertThrowsError(try TDSMessages.PreloginResponse.parse(from: &buffer)) { error in
            XCTAssertEqual(error as? TDSError, .needMoreData)
        }
    }

    func testUnknownOptionDoesNotHideRequiredFields() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 26)
        buffer.writeBytes([
            0x00, 0x00, 0x10, 0x00, 0x06, // VERSION
            0xEE, 0x00, 0x16, 0x00, 0x01, // future option
            0x01, 0x00, 0x17, 0x00, 0x01, // ENCRYPTION
            0xFF,
            0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x42, 0x01
        ])

        let response = try TDSMessages.PreloginResponse.parse(from: &buffer)
        XCTAssertEqual(response.version, "9.0.0")
        XCTAssertEqual(response.encryption, .encryptOn)
    }

    func testOptionalModeRejectsServerWithoutFullEncryption() throws {
        for serverEncryption: UInt8 in [0x00, 0x02] {
            let request = PreloginRequest(encryptionMode: .optional, hasTLSConfiguration: true)
            var response = ByteBufferAllocator().buffer(capacity: 18)
            response.writeBytes([
                0x00, 0x00, 0x0B, 0x00, 0x06,
                0x01, 0x00, 0x11, 0x00, 0x01,
                0xFF,
                0x09, 0x00, 0x00, 0x00, 0x00, 0x00,
                serverEncryption
            ])
            XCTAssertThrowsError(try request.handle(dataStream: response, allocator: ByteBufferAllocator()))
        }
    }
}
