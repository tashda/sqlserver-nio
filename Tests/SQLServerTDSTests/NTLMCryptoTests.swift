import Testing
import Foundation
@testable import SQLServerTDS

/// Tests for NTLMv2 cryptographic operations using known test vectors
/// from the MS-NLMP specification (Section 4.2).
struct NTLMCryptoTests {

    // MARK: - MD4

    @Test("MD4 of empty input")
    func md4Empty() {
        let hash = NTLMCrypto.md4(Data())
        #expect(hash == Data([
            0x31, 0xD6, 0xCF, 0xE0, 0xD1, 0x6A, 0xE9, 0x31,
            0xB7, 0x3C, 0x59, 0xD7, 0xE0, 0xC0, 0x89, 0xC0
        ]))
    }

    @Test("MD4 of 'a'")
    func md4SingleChar() {
        let hash = NTLMCrypto.md4(Data("a".utf8))
        #expect(hash == Data([
            0xBD, 0xE5, 0x2C, 0xB3, 0x1D, 0xE3, 0x3E, 0x46,
            0x24, 0x5E, 0x05, 0xFB, 0xDB, 0xD6, 0xFB, 0x24
        ]))
    }

    @Test("MD4 of 'abc'")
    func md4Abc() {
        let hash = NTLMCrypto.md4(Data("abc".utf8))
        #expect(hash == Data([
            0xA4, 0x48, 0x01, 0x7A, 0xAF, 0x21, 0xD8, 0x52,
            0x5F, 0xC1, 0x0A, 0xE8, 0x7A, 0xA6, 0x72, 0x9D
        ]))
    }

    @Test("MD4 of 'message digest'")
    func md4MessageDigest() {
        let hash = NTLMCrypto.md4(Data("message digest".utf8))
        #expect(hash == Data([
            0xD9, 0x13, 0x0A, 0x81, 0x64, 0x54, 0x9F, 0xE8,
            0x18, 0x87, 0x48, 0x06, 0xE1, 0xC7, 0x01, 0x4B
        ]))
    }

    // MARK: - NT Hash

    @Test("ntHash of 'Password' matches MS-NLMP test vector")
    func ntHashPassword() {
        let hash = NTLMCrypto.ntHash(password: "Password")
        #expect(hash == Data([
            0xA4, 0xF4, 0x9C, 0x40, 0x65, 0x10, 0xBD, 0xCA,
            0xB6, 0x82, 0x4E, 0xE7, 0xC3, 0x0F, 0xD8, 0x52
        ]))
    }

    // MARK: - NTv2 Hash

    @Test("ntv2Hash for User/Domain matches MS-NLMP test vector")
    func ntv2HashUserDomain() {
        let ntHash = Data([
            0xA4, 0xF4, 0x9C, 0x40, 0x65, 0x10, 0xBD, 0xCA,
            0xB6, 0x82, 0x4E, 0xE7, 0xC3, 0x0F, 0xD8, 0x52
        ])
        let hash = NTLMCrypto.ntv2Hash(ntHash: ntHash, username: "User", domain: "Domain")
        #expect(hash == Data([
            0x0C, 0x86, 0x8A, 0x40, 0x3B, 0xFD, 0x7A, 0x93,
            0xA3, 0x00, 0x1E, 0xF2, 0x2E, 0xF0, 0x2E, 0x3F
        ]))
    }

    // MARK: - HMAC-MD5

    @Test("HMAC-MD5 basic operation")
    func hmacMD5Basic() {
        let key = Data("key".utf8)
        let data = Data("The quick brown fox jumps over the lazy dog".utf8)
        let mac = NTLMCrypto.hmacMD5(key: key, data: data)
        #expect(mac == Data([
            0x80, 0x07, 0x07, 0x13, 0x46, 0x3E, 0x77, 0x49,
            0xB9, 0x0C, 0x2D, 0xC2, 0x49, 0x11, 0xE2, 0x75
        ]))
    }

    // MARK: - Session Base Key

    @Test("sessionBaseKey computation")
    func sessionBaseKey() {
        let ntv2Hash = Data(repeating: 0xAA, count: 16)
        let ntProof = Data(repeating: 0xBB, count: 16)
        let key = NTLMCrypto.sessionBaseKey(ntv2Hash: ntv2Hash, ntProofStr: ntProof)
        // Verify it produces a 16-byte result (specific value depends on HMAC)
        #expect(key.count == 16)
        // Verify determinism
        let key2 = NTLMCrypto.sessionBaseKey(ntv2Hash: ntv2Hash, ntProofStr: ntProof)
        #expect(key == key2)
    }

    // MARK: - Windows Timestamp

    @Test("windowsTimestamp for Unix epoch")
    func windowsTimestampUnixEpoch() {
        let unixEpoch = Date(timeIntervalSince1970: 0)
        let filetime = NTLMCrypto.windowsTimestamp(from: unixEpoch)
        // 11644473600 seconds * 10_000_000 = 116444736000000000
        #expect(filetime == 116_444_736_000_000_000)
    }

    @Test("windowsTimestamp for known date")
    func windowsTimestampKnownDate() {
        // 2024-01-01 00:00:00 UTC
        let date = Date(timeIntervalSince1970: 1704067200)
        let filetime = NTLMCrypto.windowsTimestamp(from: date)
        // Should be > Unix epoch filetime
        #expect(filetime > 116_444_736_000_000_000)
    }

    // MARK: - MIC Computation

    @Test("computeMIC produces 16-byte result")
    func computeMIC() {
        let sessionKey = Data(repeating: 0x01, count: 16)
        let negotiate = Data([0x4E, 0x54, 0x4C, 0x4D]) // partial
        let challenge = Data([0x01, 0x02, 0x03, 0x04])
        let authenticate = Data([0x05, 0x06, 0x07, 0x08])

        let mic = NTLMCrypto.computeMIC(
            sessionKey: sessionKey,
            negotiate: negotiate,
            challenge: challenge,
            authenticate: authenticate
        )
        #expect(mic.count == 16)
    }
}
