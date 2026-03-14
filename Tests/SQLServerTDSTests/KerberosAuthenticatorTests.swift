import XCTest
import Foundation
@testable import SQLServerTDS
import NIO

final class KerberosAuthenticatorTests: XCTestCase, @unchecked Sendable {

    // MARK: - KerberosError tests

    func testKerberosErrorUnsupportedDescription() {
        let error = KerberosError.unsupported
        XCTAssertTrue(error.description.contains("not supported"), "Expected description to contain 'not supported', got: \(error.description)")
    }

    #if canImport(GSS)
    func testKerberosErrorGssErrorDescription() {
        let error = KerberosError.gssError(major: 1, minor: 2, message: "test error")
        XCTAssertTrue(error.description.contains("major=1"))
        XCTAssertTrue(error.description.contains("minor=2"))
        XCTAssertTrue(error.description.contains("test error"))
    }

    func testKerberosErrorNameImportFailedDescription() {
        let error = KerberosError.nameImportFailed("bad name")
        XCTAssertTrue(error.description.contains("bad name"))
    }

    func testKerberosErrorCredentialAcquisitionFailedDescription() {
        let error = KerberosError.credentialAcquisitionFailed("no ticket")
        XCTAssertTrue(error.description.contains("no ticket"))
    }

    func testKerberosErrorContextInitFailedDescription() {
        let error = KerberosError.contextInitFailed("timeout")
        XCTAssertTrue(error.description.contains("timeout"))
    }

    func testKerberosErrorNoTokenProducedDescription() {
        let error = KerberosError.noTokenProduced
        XCTAssertTrue(error.description.contains("no output token"))
    }

    func testKerberosAuthenticatorCreatesWithEmptyCredentials() throws {
        // When username and password are empty, credentials are not acquired
        // (relies on cached Kerberos ticket). This should not throw.
        let authenticator = try KerberosAuthenticator(
            username: "",
            password: "",
            domain: nil,
            server: "localhost",
            port: 1433,
            logger: .init(label: "test")
        )
        // initialToken() will fail because there's no cached ticket in CI,
        // but the authenticator itself should be created successfully.
        XCTAssertNotNil(authenticator)
    }
    #endif

    // MARK: - SSPIRequest tests

    func testSSPIRequestSerializesTokenData() throws {
        let tokenData = Data([0x60, 0x82, 0x01, 0x00, 0xAA, 0xBB])
        let request = SSPIRequest(tokenData: tokenData)

        XCTAssertEqual(request.packetType, .sspi)
        XCTAssertFalse(request.stream)
        XCTAssertNil(request.onRow)
        XCTAssertNil(request.onMetadata)
        XCTAssertNil(request.onDone)
        XCTAssertNil(request.onMessage)
        XCTAssertNil(request.onReturnValue)

        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        try request.serialize(into: &buffer)

        XCTAssertEqual(buffer.readableBytes, 6)
        let bytes = buffer.readBytes(length: 6)
        XCTAssertEqual(bytes, [0x60, 0x82, 0x01, 0x00, 0xAA, 0xBB])
    }

    func testSSPIRequestProducesPackets() throws {
        let tokenData = Data([0x01, 0x02, 0x03, 0x04])
        let request = SSPIRequest(tokenData: tokenData)
        let packets = try request.start(allocator: ByteBufferAllocator())

        XCTAssertFalse(packets.isEmpty)
        XCTAssertEqual(packets.first?.type, .sspi)
    }

    // MARK: - SSPI Token parsing tests

    func testParseSSPIToken() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        // Write SSPI token type (0xED)
        buffer.writeInteger(TDSTokens.TokenType.sspi.rawValue)
        // Write length (2 bytes, little-endian)
        let payload: [UInt8] = [0x60, 0x82, 0x00, 0x03, 0xAA, 0xBB, 0xCC]
        buffer.writeInteger(UInt16(payload.count), endianness: .little)
        buffer.writeBytes(payload)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        let tokens = try parser.parse()

        XCTAssertEqual(tokens.count, 1)
        let sspiToken = try XCTUnwrap(tokens[0] as? TDSTokens.SSPIToken)
        XCTAssertEqual(sspiToken.type, .sspi)
        XCTAssertEqual(sspiToken.data.count, 7)
        XCTAssertEqual(Array(sspiToken.data), payload)
    }

    func testParseSSPITokenWithEmptyPayload() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 8)
        buffer.writeInteger(TDSTokens.TokenType.sspi.rawValue)
        buffer.writeInteger(UInt16(0), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        let tokens = try parser.parse()

        XCTAssertEqual(tokens.count, 1)
        let sspiToken = try XCTUnwrap(tokens[0] as? TDSTokens.SSPIToken)
        XCTAssertEqual(sspiToken.data.count, 0)
    }

    // MARK: - LoginRequest tests

    func testLoginRequestPublicInitHasNoAuthenticator() {
        let payload = TDSMessages.Login7Message(
            username: "sa",
            password: "password",
            serverName: "localhost",
            database: "master",
            useIntegratedSecurity: false,
            sspiData: nil
        )
        let request = LoginRequest(payload: payload)

        XCTAssertNil(request.authenticator)
        XCTAssertNil(request.connection)
        XCTAssertNil(request.serverErrorMessage)
        XCTAssertEqual(request.packetType, .tds7Login)
        XCTAssertFalse(request.stream)
    }

    func testLoginRequestCapturesServerErrorMessage() {
        let payload = TDSMessages.Login7Message(
            username: "sa",
            password: "password",
            serverName: "localhost",
            database: "master",
            useIntegratedSecurity: false,
            sspiData: nil
        )
        let request = LoginRequest(payload: payload)
        XCTAssertNil(request.serverErrorMessage)

        request.serverErrorMessage = "Login failed for user 'sa'."
        XCTAssertEqual(request.serverErrorMessage, "Login failed for user 'sa'.")
    }

    // MARK: - TDSPacket.HeaderType.sspi

    func testSSPIHeaderTypeValue() {
        XCTAssertEqual(TDSPacket.HeaderType.sspi, 0x11)
    }
}
