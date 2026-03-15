import XCTest
@testable import SQLServerTDS
import NIO

final class FedAuthTests: XCTestCase, @unchecked Sendable {

    // MARK: - TDSAuthentication

    func testAccessTokenAuthCase() {
        let auth = TDSAuthentication.accessToken(token: "eyJ0eXAi...")
        if case .accessToken(let token) = auth {
            XCTAssertEqual(token, "eyJ0eXAi...")
        } else {
            XCTFail("Expected accessToken case")
        }
    }

    // MARK: - Login7Message with FEDAUTH FeatureExt

    func testLogin7WithFedAuthContainsFeatureExt() throws {
        let message = TDSMessages.Login7Message(
            username: "",
            password: "",
            serverName: "myserver.database.windows.net",
            database: "mydb",
            useIntegratedSecurity: false,
            sspiData: nil,
            fedAuthAccessToken: "test-access-token"
        )

        var buffer = ByteBufferAllocator().buffer(capacity: 1024)
        try message.serialize(into: &buffer)

        // Read the total length
        let totalLength = buffer.getInteger(at: 0, endianness: .little, as: UInt32.self)!
        XCTAssertEqual(Int(totalLength), buffer.readableBytes)

        // OptionFlags3 is at offset 27 from start
        let optionFlags3 = buffer.getInteger(at: 27, as: UInt8.self)!
        XCTAssertEqual(optionFlags3 & 0x10, 0x10, "fExtension bit should be set in OptionFlags3")

        // Verify the buffer contains the FEDAUTH feature ID (0x02) and token
        let bytes = Array(buffer.readableBytesView)

        // Find the FeatureExt terminator (0xFF) near the end
        let lastByte = bytes.last!
        XCTAssertEqual(lastByte, 0xFF, "FeatureExt should end with 0xFF terminator")

        // Find FEDAUTH feature ID (0x02) in the buffer
        // It should appear in the FeatureExt block near the end
        var foundFedAuth = false
        for i in (bytes.count - 50)..<bytes.count {
            if bytes[i] == 0x02 {
                // Check if this looks like a FeatureId followed by a DWORD length
                let featureDataLen = UInt32(bytes[i+1]) |
                    (UInt32(bytes[i+2]) << 8) |
                    (UInt32(bytes[i+3]) << 16) |
                    (UInt32(bytes[i+4]) << 24)

                if featureDataLen > 0 && featureDataLen < 1000 {
                    // bOptions should be 0x04 (SECURITY_TOKEN << 1)
                    let bOptions = bytes[i + 5]
                    if bOptions == 0x04 {
                        foundFedAuth = true
                        break
                    }
                }
            }
        }
        XCTAssertTrue(foundFedAuth, "Should find FEDAUTH feature with bOptions=0x04 in FeatureExt")

        // Verify the token is present as UTF-8 bytes by searching for the subsequence
        let tokenBytes = Array("test-access-token".utf8)
        var foundToken = false
        for j in 0..<(bytes.count - tokenBytes.count) {
            if Array(bytes[j..<(j + tokenBytes.count)]) == tokenBytes {
                foundToken = true
                break
            }
        }
        XCTAssertTrue(foundToken, "Buffer should contain the access token as UTF-8 bytes")
    }

    func testLogin7WithoutFedAuthHasNoFeatureExt() throws {
        let message = TDSMessages.Login7Message(
            username: "sa",
            password: "password",
            serverName: "localhost",
            database: "master",
            useIntegratedSecurity: false,
            sspiData: nil,
            fedAuthAccessToken: nil
        )

        var buffer = ByteBufferAllocator().buffer(capacity: 1024)
        try message.serialize(into: &buffer)

        // OptionFlags3 should NOT have fExtension bit
        let optionFlags3 = buffer.getInteger(at: 27, as: UInt8.self)!
        XCTAssertEqual(optionFlags3 & 0x10, 0x00, "fExtension bit should NOT be set")
    }

    // MARK: - PreloginMessage with FEDAUTHREQUIRED

    func testPreloginWithFedAuthRequired() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        try TDSMessages.PreloginMessage(
            version: "9.0.0",
            encryption: .encryptOn,
            fedAuthRequired: true
        ).serialize(into: &buffer)

        let bytes = Array(buffer.readableBytesView)

        // Find FEDAUTHREQUIRED token (0x06) in the option table
        var foundFedAuth = false
        var i = 0
        while i < bytes.count {
            if bytes[i] == 0xFF { break } // terminator
            if bytes[i] == 0x06 {
                foundFedAuth = true
                // Read the offset and length
                let offset = Int(bytes[i+1]) << 8 | Int(bytes[i+2])
                let length = Int(bytes[i+3]) << 8 | Int(bytes[i+4])
                XCTAssertEqual(length, 1, "FEDAUTHREQUIRED data should be 1 byte")
                // Check the data value at the offset
                XCTAssertEqual(bytes[offset], 0x01, "FEDAUTHREQUIRED value should be 0x01")
                break
            }
            i += 5 // each option entry is 5 bytes
        }
        XCTAssertTrue(foundFedAuth, "FEDAUTHREQUIRED option should be present")
    }

    func testPreloginWithoutFedAuth() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        try TDSMessages.PreloginMessage(
            version: "9.0.0",
            encryption: .encryptOn,
            fedAuthRequired: false
        ).serialize(into: &buffer)

        let bytes = Array(buffer.readableBytesView)

        // Should NOT contain FEDAUTHREQUIRED token (0x06)
        var i = 0
        while i < bytes.count {
            if bytes[i] == 0xFF { break }
            XCTAssertNotEqual(bytes[i], 0x06, "FEDAUTHREQUIRED token should not be present")
            i += 5
        }
    }

    // MARK: - TDSLoginConfiguration with accessToken

    func testLoginConfigurationWithAccessToken() {
        let config = TDSLoginConfiguration(
            serverName: "myserver.database.windows.net",
            port: 1433,
            database: "mydb",
            authentication: .accessToken(token: "eyJ0eXAi...")
        )

        if case .accessToken(let token) = config.authentication {
            XCTAssertEqual(token, "eyJ0eXAi...")
        } else {
            XCTFail("Expected accessToken authentication")
        }
        XCTAssertEqual(config.serverName, "myserver.database.windows.net")
        XCTAssertEqual(config.port, 1433)
    }
}
