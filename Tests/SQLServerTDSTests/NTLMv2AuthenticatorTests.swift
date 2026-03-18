import Testing
import Foundation
import Logging
@testable import SQLServerTDS

/// Integration tests for the NTLMv2 authentication flow.
struct NTLMv2AuthenticatorTests {

    private let logger = Logger(label: "test.ntlm")

    // MARK: - Initialization

    @Test("NTLMv2Authenticator requires non-empty credentials")
    func emptyCredentialsFails() {
        #expect(throws: NTLMv2Authenticator.NTLMError.self) {
            try NTLMv2Authenticator(
                username: "",
                password: "",
                domain: "DOMAIN",
                server: "server",
                port: 1433,
                logger: logger
            )
        }
    }

    @Test("NTLMv2Authenticator creates successfully with valid credentials")
    func validCredentials() throws {
        let auth = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "Domain",
            server: "server.example.com",
            port: 1433,
            logger: logger
        )
        // Should not throw
        _ = auth
    }

    // MARK: - Initial Token (Type 1)

    @Test("initialToken produces valid NTLM NEGOTIATE message")
    func initialTokenValid() throws {
        let auth = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "Domain",
            server: "server",
            port: 1433,
            logger: logger
        )

        let token = try auth.initialToken()

        // Verify NTLMSSP signature
        #expect(token[0..<8] == Data([0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00]))
        // Verify message type = 1
        #expect(token.readUInt32LE(at: 8) == 1)
        // Should be non-trivial size
        #expect(token.count >= 40)
    }

    // MARK: - Full Authentication Flow

    @Test("Full NTLM exchange produces valid Type 3 from mock Type 2")
    func fullAuthenticationFlow() throws {
        let auth = try NTLMv2Authenticator(
            username: "TestUser",
            password: "TestPassword",
            domain: "TESTDOMAIN",
            server: "sqlserver.test.local",
            port: 1433,
            logger: logger
        )

        // Step 1: Get initial token (Type 1)
        let type1 = try auth.initialToken()
        #expect(type1.readUInt32LE(at: 8) == 1)

        // Step 2: Build a mock Type 2 challenge
        let serverChallenge = Data([0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF])
        let type2 = buildMockType2(serverChallenge: serverChallenge, includeTimestamp: true)

        // Step 3: Process challenge → should produce Type 3
        let (responseData, _) = try auth.continueAuthentication(serverToken: type2)

        guard let type3 = responseData else {
            Issue.record("Expected Type 3 response data")
            return
        }

        // Verify Type 3 structure
        #expect(type3[0..<8] == Data([0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00]))
        #expect(type3.readUInt32LE(at: 8) == 3)

        // Verify LM response field is 24 bytes
        let lmLen = type3.readUInt16LE(at: 12)
        #expect(lmLen == 24)

        // Verify NT response field is present and non-trivial
        let ntLen = type3.readUInt16LE(at: 20)
        #expect(ntLen > 16) // At least ntProofStr (16) + client blob

        // Verify MIC is non-zero (since we included timestamp)
        let mic = type3[72..<88]
        #expect(mic != Data(count: 16))
    }

    @Test("Authentication without timestamp skips MIC")
    func authenticationWithoutTimestamp() throws {
        let auth = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "DOMAIN",
            server: "server",
            port: 1433,
            logger: logger
        )

        _ = try auth.initialToken()

        let type2 = buildMockType2(
            serverChallenge: Data(repeating: 0xCC, count: 8),
            includeTimestamp: false
        )

        let (responseData, _) = try auth.continueAuthentication(serverToken: type2)
        guard let type3 = responseData else {
            Issue.record("Expected response data")
            return
        }

        // MIC should be all zeros (no timestamp = no MIC computation)
        let mic = type3[72..<88]
        #expect(mic == Data(count: 16))
    }

    @Test("Invalid Type 2 challenge throws error")
    func invalidChallenge() throws {
        let auth = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "DOMAIN",
            server: "server",
            port: 1433,
            logger: logger
        )

        _ = try auth.initialToken()

        // Send garbage as Type 2
        let invalidType2 = Data([0xFF, 0xFF, 0xFF, 0xFF])
        #expect(throws: NTLMv2Authenticator.NTLMError.self) {
            try auth.continueAuthentication(serverToken: invalidType2)
        }
    }

    @Test("Type 3 domain matches provided domain")
    func type3DomainField() throws {
        let auth = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "MYDOMAIN",
            server: "server",
            port: 1433,
            logger: logger
        )

        _ = try auth.initialToken()
        let type2 = buildMockType2(serverChallenge: Data(count: 8), includeTimestamp: false)
        let (responseData, _) = try auth.continueAuthentication(serverToken: type2)
        guard let type3 = responseData else {
            Issue.record("Expected response data")
            return
        }

        // Read domain field from Type 3
        let domainLen = Int(type3.readUInt16LE(at: 28))
        let domainOffset = Int(type3.readUInt32LE(at: 32))
        let domainData = type3[domainOffset ..< domainOffset + domainLen]

        // "MYDOMAIN" in UTF-16LE
        let expected = "MYDOMAIN".utf16.flatMap { [UInt8($0 & 0xFF), UInt8($0 >> 8)] }
        #expect(domainData == Data(expected))
    }

    // MARK: - TDSAuthenticator Protocol

    @Test("Empty domain does not crash during authentication")
    func emptyDomainAuth() throws {
        let auth = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "",
            server: "server",
            port: 1433,
            logger: logger
        )

        _ = try auth.initialToken()
        let type2 = buildMockType2(serverChallenge: Data(count: 8), includeTimestamp: false)
        let (responseData, _) = try auth.continueAuthentication(serverToken: type2)

        // Should produce valid Type 3 even with empty domain
        guard let type3 = responseData else {
            Issue.record("Expected response data")
            return
        }
        #expect(type3.readUInt32LE(at: 8) == 3)

        // Domain length should be 0
        let domainLen = type3.readUInt16LE(at: 28)
        #expect(domainLen == 0)
    }

    @Test("NTLMv2Authenticator conforms to TDSAuthenticator")
    func protocolConformance() throws {
        let auth: any TDSAuthenticator = try NTLMv2Authenticator(
            username: "User",
            password: "Password",
            domain: "DOMAIN",
            server: "server",
            port: 1433,
            logger: logger
        )

        let token = try auth.initialToken()
        #expect(token.count > 0)
    }

    // MARK: - Helpers

    /// Builds a minimal NTLM Type 2 (CHALLENGE) message for testing.
    private func buildMockType2(serverChallenge: Data, includeTimestamp: Bool) -> Data {
        var data = Data()

        // Signature
        data.append(contentsOf: [0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00])
        // MessageType = 2
        data.appendUInt32LE(2)
        // TargetNameFields (empty)
        data.appendUInt16LE(0)
        data.appendUInt16LE(0)
        data.appendUInt32LE(0)
        // NegotiateFlags
        data.appendUInt32LE(0x00028233)
        // ServerChallenge
        data.append(serverChallenge)
        // Reserved (8 bytes)
        data.append(Data(count: 8))

        // Build TargetInfo
        var targetInfo = Data()
        // NbDomainName
        targetInfo.appendUInt16LE(0x0002)
        let domainUTF16 = Data([0x44, 0x00, 0x4F, 0x00, 0x4D, 0x00]) // "DOM"
        targetInfo.appendUInt16LE(UInt16(domainUTF16.count))
        targetInfo.append(domainUTF16)

        if includeTimestamp {
            targetInfo.appendUInt16LE(0x0007) // MsvAvTimestamp
            targetInfo.appendUInt16LE(8)
            targetInfo.appendUInt64LE(132_500_000_000_000_000)
        }

        // EOL
        targetInfo.appendUInt16LE(0x0000)
        targetInfo.appendUInt16LE(0)

        // TargetInfoFields
        let targetInfoOffset = UInt32(data.count + 8)
        data.appendUInt16LE(UInt16(targetInfo.count))
        data.appendUInt16LE(UInt16(targetInfo.count))
        data.appendUInt32LE(targetInfoOffset)
        data.append(targetInfo)

        return data
    }
}
