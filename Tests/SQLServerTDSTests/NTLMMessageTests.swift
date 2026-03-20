import Testing
import Foundation
@testable import SQLServerTDS

/// Tests for NTLM message serialization and parsing.
struct NTLMMessageTests {

    // MARK: - Type 1: NEGOTIATE

    @Test("Type 1 starts with NTLMSSP signature and type 1")
    func negotiateSignature() {
        let msg = NTLMNegotiateMessage(domain: "DOMAIN", workstation: "WORKSTATION")
        let data = msg.serialize()

        // Signature: "NTLMSSP\0"
        #expect(data[0..<8] == Data([0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00]))
        // Message type: 1
        #expect(data.readUInt32LE(at: 8) == 1)
    }

    @Test("Type 1 includes negotiate flags")
    func negotiateFlags() {
        let msg = NTLMNegotiateMessage(domain: "DOMAIN", workstation: "WORKSTATION")
        let data = msg.serialize()

        let flags = NTLMFlags(rawValue: data.readUInt32LE(at: 12))
        #expect(flags.contains(.negotiateUnicode))
        #expect(flags.contains(.negotiateNTLM))
        #expect(flags.contains(.requestTarget))
        #expect(flags.contains(.negotiateExtendedSecurity))
        #expect(flags.contains(.negotiate128))
        #expect(flags.contains(.negotiateOEMDomainSupplied))
        #expect(flags.contains(.negotiateOEMWorkstationSupplied))
    }

    @Test("Type 1 domain and workstation fields point to payload")
    func negotiateDomainWorkstation() {
        let msg = NTLMNegotiateMessage(domain: "TEST", workstation: "HOST")
        let data = msg.serialize()

        // Domain fields at offset 16
        let domainLen = data.readUInt16LE(at: 16)
        let domainOffset = data.readUInt32LE(at: 20)
        #expect(domainLen == 4) // "TEST" in OEM
        #expect(domainOffset == 40) // after 32-byte header + 8-byte version

        // Workstation fields at offset 24
        let wsLen = data.readUInt16LE(at: 24)
        let wsOffset = data.readUInt32LE(at: 28)
        #expect(wsLen == 4) // "HOST" in OEM
        #expect(wsOffset == 44) // after domain payload

        // Verify payload content
        let domainStr = String(data: data[40..<44], encoding: .ascii)
        #expect(domainStr == "TEST")
        let wsStr = String(data: data[44..<48], encoding: .ascii)
        #expect(wsStr == "HOST")
    }

    @Test("Type 1 with empty domain and workstation")
    func negotiateEmptyFields() {
        let msg = NTLMNegotiateMessage(domain: "", workstation: "")
        let data = msg.serialize()

        let flags = NTLMFlags(rawValue: data.readUInt32LE(at: 12))
        #expect(!flags.contains(.negotiateOEMDomainSupplied))
        #expect(!flags.contains(.negotiateOEMWorkstationSupplied))

        // Domain length should be 0
        #expect(data.readUInt16LE(at: 16) == 0)
        // Workstation length should be 0
        #expect(data.readUInt16LE(at: 24) == 0)
    }

    // MARK: - Type 2: CHALLENGE (Parse)

    @Test("Parse valid Type 2 message")
    func parseChallengeBasic() throws {
        let type2 = buildMockChallenge(
            serverChallenge: Data([0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]),
            flags: 0x00028233
        )

        let parsed = try NTLMChallengeMessage.parse(type2)
        #expect(parsed.serverChallenge == Data([0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]))
        #expect(parsed.negotiateFlags == 0x00028233)
    }

    @Test("Parse Type 2 extracts timestamp from TargetInfo")
    func parseChallengeWithTimestamp() throws {
        let timestamp: UInt64 = 132_500_000_000_000_000
        let targetInfo = buildTargetInfo(timestamp: timestamp)
        let type2 = buildMockChallenge(
            serverChallenge: Data(repeating: 0xAA, count: 8),
            flags: 0x00028233,
            targetInfo: targetInfo
        )

        let parsed = try NTLMChallengeMessage.parse(type2)
        #expect(parsed.timestamp == timestamp)
        #expect(parsed.targetInfo.count == targetInfo.count)
    }

    @Test("Parse Type 2 without timestamp returns nil")
    func parseChallengeNoTimestamp() throws {
        let targetInfo = buildTargetInfo(timestamp: nil)
        let type2 = buildMockChallenge(
            serverChallenge: Data(repeating: 0xBB, count: 8),
            flags: 0x00028233,
            targetInfo: targetInfo
        )

        let parsed = try NTLMChallengeMessage.parse(type2)
        #expect(parsed.timestamp == nil)
    }

    @Test("Parse Type 2 rejects too-short data")
    func parseChallengeShort() {
        #expect(throws: NTLMChallengeMessage.ParseError.self) {
            try NTLMChallengeMessage.parse(Data(count: 20))
        }
    }

    @Test("Parse Type 2 rejects wrong signature")
    func parseChallengeWrongSignature() {
        var data = Data(count: 48)
        data[0] = 0xFF  // Wrong signature
        data[8] = 2     // Type 2
        #expect(throws: NTLMChallengeMessage.ParseError.self) {
            try NTLMChallengeMessage.parse(data)
        }
    }

    @Test("Parse Type 2 rejects wrong message type")
    func parseChallengeWrongType() {
        var data = Data(count: 48)
        // Correct signature
        let sig: [UInt8] = [0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00]
        for (i, b) in sig.enumerated() { data[i] = b }
        // Wrong type (1 instead of 2)
        data[8] = 1
        #expect(throws: NTLMChallengeMessage.ParseError.self) {
            try NTLMChallengeMessage.parse(data)
        }
    }

    // MARK: - Type 3: AUTHENTICATE

    @Test("Type 3 starts with NTLMSSP signature and type 3")
    func authenticateSignature() {
        let msg = NTLMAuthenticateMessage(
            domain: "DOMAIN",
            username: "User",
            workstation: "HOST",
            lmResponse: Data(count: 24),
            ntResponse: Data(count: 84),
            flags: 0x00028233,
            hasMIC: false
        )
        let data = msg.serialize()

        #expect(data[0..<8] == Data([0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00]))
        #expect(data.readUInt32LE(at: 8) == 3)
    }

    @Test("Type 3 header is 88 bytes")
    func authenticateHeaderSize() {
        let msg = NTLMAuthenticateMessage(
            domain: "D",
            username: "U",
            workstation: "W",
            lmResponse: Data(count: 24),
            ntResponse: Data(count: 16),
            flags: 0,
            hasMIC: true
        )
        let data = msg.serialize()

        // Domain field should point to offset 88 (start of payload)
        let domainOffset = data.readUInt32LE(at: 32)
        #expect(domainOffset == 88)
    }

    @Test("Type 3 MIC field at offset 72 is zeroed")
    func authenticateMICZeroed() {
        let msg = NTLMAuthenticateMessage(
            domain: "DOMAIN",
            username: "User",
            workstation: "HOST",
            lmResponse: Data(count: 24),
            ntResponse: Data(count: 84),
            flags: 0x00028233,
            hasMIC: true
        )
        let data = msg.serialize()

        // MIC at offset 72-87 should be all zeros (caller patches later)
        let mic = data[72..<88]
        #expect(mic == Data(count: 16))
    }

    @Test("Type 3 domain is UTF-16LE encoded")
    func authenticateDomainEncoding() {
        let msg = NTLMAuthenticateMessage(
            domain: "Test",
            username: "U",
            workstation: "W",
            lmResponse: Data(),
            ntResponse: Data(),
            flags: 0,
            hasMIC: false
        )
        let data = msg.serialize()

        let domainLen = Int(data.readUInt16LE(at: 28))
        let domainOffset = Int(data.readUInt32LE(at: 32))
        let domainData = data[domainOffset ..< domainOffset + domainLen]

        // "TEST" (uppercased) in UTF-16LE = 54 00 45 00 53 00 54 00
        #expect(domainData == Data([0x54, 0x00, 0x45, 0x00, 0x53, 0x00, 0x54, 0x00]))
    }

    @Test("Type 3 LM response is 24 zero bytes")
    func authenticateLMResponse() {
        let lmResponse = Data(count: 24)
        let msg = NTLMAuthenticateMessage(
            domain: "D",
            username: "U",
            workstation: "W",
            lmResponse: lmResponse,
            ntResponse: Data(count: 16),
            flags: 0,
            hasMIC: false
        )
        let data = msg.serialize()

        let lmLen = Int(data.readUInt16LE(at: 12))
        let lmOffset = Int(data.readUInt32LE(at: 16))
        #expect(lmLen == 24)
        #expect(data[lmOffset ..< lmOffset + lmLen] == Data(count: 24))
    }

    // MARK: - Client Blob

    @Test("Client blob has correct structure")
    func clientBlobStructure() {
        let clientChallenge = Data([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
        let targetInfo = buildTargetInfo(timestamp: nil)
        let timestamp: UInt64 = 132_500_000_000_000_000

        let blob = NTLMAuthenticateMessage.buildClientBlob(
            timestamp: timestamp,
            clientChallenge: clientChallenge,
            targetInfo: targetInfo,
            includeMICFlag: false
        )

        #expect(blob[0] == 0x01) // RespType
        #expect(blob[1] == 0x01) // HiRespType
        #expect(blob[2] == 0x00) // Reserved1
        #expect(blob[3] == 0x00)
        // Reserved2 at offset 4-7
        #expect(blob[4..<8] == Data(count: 4))
        // Timestamp at offset 8-15
        let ts = blob.readUInt64LE(at: 8)
        #expect(ts == timestamp)
        // Client challenge at offset 16-23
        #expect(blob[16..<24] == clientChallenge)
    }

    @Test("Client blob inserts MIC flag in TargetInfo")
    func clientBlobMICFlag() {
        let targetInfo = buildTargetInfo(timestamp: nil)
        let blob = NTLMAuthenticateMessage.buildClientBlob(
            timestamp: 0,
            clientChallenge: Data(count: 8),
            targetInfo: targetInfo,
            includeMICFlag: true
        )

        // The blob should contain MsvAvFlags (0x0006) with value 0x0002 somewhere
        // after the fixed header (28 bytes) and before the trailing 4-byte reserved
        let avPairsStart = 28 // After header
        let avPairsData = Data(blob[avPairsStart ..< blob.count - 4])  // Re-base indices
        #expect(containsAVPair(avPairsData, id: 0x0006, value: 0x0002))
    }

    // MARK: - TargetInfo AV_PAIR Parsing

    @Test("TargetInfo with multiple AV pairs")
    func targetInfoMultiplePairs() throws {
        var targetInfo = Data()
        // NbDomainName = "D"
        targetInfo.appendUInt16LE(0x0002) // MsvAvNbDomainName
        targetInfo.appendUInt16LE(2)       // Length (UTF-16LE "D")
        targetInfo.append(contentsOf: [0x44, 0x00])
        // Timestamp
        targetInfo.appendUInt16LE(0x0007)
        targetInfo.appendUInt16LE(8)
        let ts: UInt64 = 132_500_000_000_000_000
        targetInfo.appendUInt64LE(ts)
        // EOL
        targetInfo.appendUInt16LE(0x0000)
        targetInfo.appendUInt16LE(0)

        // Build a challenge with this targetInfo and verify parsing
        let challenge = buildMockChallenge(
            serverChallenge: Data(count: 8),
            flags: 0,
            targetInfo: targetInfo
        )
        let parsed = try NTLMChallengeMessage.parse(challenge)
        #expect(parsed.timestamp == ts)
    }

    // MARK: - Helpers

    /// Builds a minimal NTLM Type 2 (CHALLENGE) message for testing.
    private func buildMockChallenge(
        serverChallenge: Data,
        flags: UInt32,
        targetInfo: Data = Data()
    ) -> Data {
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
        data.appendUInt32LE(flags)
        // ServerChallenge
        data.append(serverChallenge)
        // Reserved (8 bytes)
        data.append(Data(count: 8))

        if !targetInfo.isEmpty {
            // TargetInfoFields
            let offset = UInt32(data.count + 8) // after these 8 bytes of fields
            data.appendUInt16LE(UInt16(targetInfo.count))
            data.appendUInt16LE(UInt16(targetInfo.count))
            data.appendUInt32LE(offset)
            data.append(targetInfo)
        } else {
            // TargetInfoFields (empty)
            data.appendUInt16LE(0)
            data.appendUInt16LE(0)
            data.appendUInt32LE(0)
        }

        return data
    }

    /// Builds a simple TargetInfo AV_PAIR list for testing.
    private func buildTargetInfo(timestamp: UInt64?) -> Data {
        var info = Data()

        // NbDomainName = "DOMAIN"
        info.appendUInt16LE(0x0002)
        let domainUTF16 = Data([0x44, 0x00, 0x4F, 0x00, 0x4D, 0x00, 0x41, 0x00, 0x49, 0x00, 0x4E, 0x00])
        info.appendUInt16LE(UInt16(domainUTF16.count))
        info.append(domainUTF16)

        if let timestamp {
            info.appendUInt16LE(0x0007) // MsvAvTimestamp
            info.appendUInt16LE(8)
            info.appendUInt64LE(timestamp)
        }

        // EOL
        info.appendUInt16LE(0x0000)
        info.appendUInt16LE(0)

        return info
    }

    /// Checks if an AV_PAIR list contains a specific ID with a UInt32 value.
    private func containsAVPair(_ data: Data, id: UInt16, value: UInt32) -> Bool {
        var offset = 0
        while offset + 4 <= data.count {
            let avId = data.readUInt16LE(at: offset)
            let avLen = Int(data.readUInt16LE(at: offset + 2))
            offset += 4
            if avId == id, avLen == 4, offset + 4 <= data.count {
                let val = data.readUInt32LE(at: offset)
                if val == value { return true }
            }
            if avId == 0 { break }
            offset += avLen
        }
        return false
    }
}
