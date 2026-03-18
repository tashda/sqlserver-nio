import Foundation

// MARK: - Constants

/// NTLM signature: "NTLMSSP\0"
private let ntlmSignature = Data([0x4E, 0x54, 0x4C, 0x4D, 0x53, 0x53, 0x50, 0x00])

/// NTLM negotiate flags used in the protocol exchange.
struct NTLMFlags: OptionSet, Sendable {
    let rawValue: UInt32

    static let negotiateUnicode                = NTLMFlags(rawValue: 0x0000_0001)
    static let negotiateOEM                    = NTLMFlags(rawValue: 0x0000_0002)
    static let requestTarget                   = NTLMFlags(rawValue: 0x0000_0004)
    static let negotiateNTLM                   = NTLMFlags(rawValue: 0x0000_0200)
    static let negotiateOEMDomainSupplied      = NTLMFlags(rawValue: 0x0000_1000)
    static let negotiateOEMWorkstationSupplied = NTLMFlags(rawValue: 0x0000_2000)
    static let negotiateAlwaysSign             = NTLMFlags(rawValue: 0x0000_8000)
    static let negotiateExtendedSecurity       = NTLMFlags(rawValue: 0x0008_0000)
    static let negotiateTargetInfo             = NTLMFlags(rawValue: 0x0080_0000)
    static let negotiateVersion                = NTLMFlags(rawValue: 0x0200_0000)
    static let negotiate128                    = NTLMFlags(rawValue: 0x2000_0000)
    static let negotiateKeyExchange            = NTLMFlags(rawValue: 0x4000_0000)
    static let negotiate56                     = NTLMFlags(rawValue: 0x8000_0000)
}

/// AV_PAIR identifier values from the TargetInfo field.
enum AVPairID: UInt16 {
    case eol              = 0x0000
    case nbComputerName   = 0x0001
    case nbDomainName     = 0x0002
    case dnsComputerName  = 0x0003
    case dnsDomainName    = 0x0004
    case dnsTreeName      = 0x0005
    case flags            = 0x0006
    case timestamp        = 0x0007
    case singleHost       = 0x0008
    case targetName       = 0x0009
    case channelBindings  = 0x000A
}

// MARK: - NTLM Version

/// NTLM Version structure (8 bytes). Used in Type 1 and Type 3 messages.
private let ntlmVersion: Data = {
    var data = Data(count: 8)
    data[0] = 10  // ProductMajorVersion (Windows 10)
    data[1] = 0   // ProductMinorVersion
    data[2] = 0   // ProductBuild (low byte)
    data[3] = 0   // ProductBuild (high byte)
    data[4] = 0   // Reserved
    data[5] = 0   // Reserved
    data[6] = 0   // Reserved
    data[7] = 15  // NTLMRevisionCurrent (NTLMSSP_REVISION_W2K3)
    return data
}()

// MARK: - Type 1: NEGOTIATE

/// Builds the NTLM Type 1 (NEGOTIATE) message.
struct NTLMNegotiateMessage {
    let domain: String
    let workstation: String

    func serialize() -> Data {
        let domainBytes = Data(domain.uppercased().utf8)
        let workstationBytes = Data(workstation.uppercased().utf8)

        var flags: NTLMFlags = [
            .negotiateUnicode,
            .negotiateOEM,
            .requestTarget,
            .negotiateNTLM,
            .negotiateAlwaysSign,
            .negotiateExtendedSecurity,
            .negotiate128,
            .negotiateVersion,
        ]
        if !domain.isEmpty {
            flags.insert(.negotiateOEMDomainSupplied)
        }
        if !workstation.isEmpty {
            flags.insert(.negotiateOEMWorkstationSupplied)
        }

        // Header: 32 bytes + 8 bytes version = 40 bytes before payload
        let headerSize = 40
        let domainOffset = headerSize
        let workstationOffset = domainOffset + domainBytes.count

        var data = Data(capacity: workstationOffset + workstationBytes.count)

        // Signature + MessageType
        data.append(ntlmSignature)
        data.appendUInt32LE(1)

        // NegotiateFlags
        data.appendUInt32LE(flags.rawValue)

        // DomainNameFields: Len, MaxLen, Offset
        data.appendUInt16LE(UInt16(domainBytes.count))
        data.appendUInt16LE(UInt16(domainBytes.count))
        data.appendUInt32LE(UInt32(domainOffset))

        // WorkstationFields: Len, MaxLen, Offset
        data.appendUInt16LE(UInt16(workstationBytes.count))
        data.appendUInt16LE(UInt16(workstationBytes.count))
        data.appendUInt32LE(UInt32(workstationOffset))

        // Version
        data.append(ntlmVersion)

        // Payload
        data.append(domainBytes)
        data.append(workstationBytes)

        return data
    }
}

// MARK: - Type 2: CHALLENGE (Parse)

/// Parsed NTLM Type 2 (CHALLENGE) message from the server.
struct NTLMChallengeMessage {
    /// The 8-byte server nonce.
    let serverChallenge: Data
    /// Negotiated flags from the server.
    let negotiateFlags: UInt32
    /// Raw TargetInfo AV_PAIR bytes.
    let targetInfo: Data
    /// Server's timestamp from TargetInfo (MsvAvTimestamp), if present.
    let timestamp: UInt64?

    enum ParseError: Error, CustomStringConvertible {
        case tooShort
        case invalidSignature
        case invalidMessageType
        case targetInfoOutOfBounds

        var description: String {
            switch self {
            case .tooShort: "NTLM challenge message too short"
            case .invalidSignature: "Invalid NTLM signature"
            case .invalidMessageType: "Expected NTLM Type 2 (CHALLENGE) message"
            case .targetInfoOutOfBounds: "TargetInfo offset/length exceeds message bounds"
            }
        }
    }

    /// Parses a raw Type 2 message from the server.
    static func parse(_ data: Data) throws -> NTLMChallengeMessage {
        guard data.count >= 32 else { throw ParseError.tooShort }
        guard data[0..<8] == ntlmSignature else { throw ParseError.invalidSignature }
        guard data.readUInt32LE(at: 8) == 2 else { throw ParseError.invalidMessageType }

        let flags = data.readUInt32LE(at: 20)
        let challenge = Data(data[24..<32])

        // Parse TargetInfo if present (fields at offset 40)
        var targetInfoData = Data()
        var serverTimestamp: UInt64?

        if data.count >= 48 {
            let targetInfoLen = Int(data.readUInt16LE(at: 40))
            let targetInfoOffset = Int(data.readUInt32LE(at: 44))

            if targetInfoLen > 0 {
                guard targetInfoOffset + targetInfoLen <= data.count else {
                    throw ParseError.targetInfoOutOfBounds
                }
                // Re-base indices to 0 so readUInt16LE/readUInt32LE work correctly
                targetInfoData = Data(data[targetInfoOffset ..< targetInfoOffset + targetInfoLen])
                serverTimestamp = Self.extractTimestamp(from: targetInfoData)
            }
        }

        return NTLMChallengeMessage(
            serverChallenge: challenge,
            negotiateFlags: flags,
            targetInfo: targetInfoData,
            timestamp: serverTimestamp
        )
    }

    /// Walks the AV_PAIR list to find MsvAvTimestamp (0x0007).
    private static func extractTimestamp(from targetInfo: Data) -> UInt64? {
        var offset = 0
        while offset + 4 <= targetInfo.count {
            let avId = targetInfo.readUInt16LE(at: offset)
            let avLen = Int(targetInfo.readUInt16LE(at: offset + 2))
            offset += 4

            guard offset + avLen <= targetInfo.count else { break }

            if avId == AVPairID.timestamp.rawValue, avLen == 8 {
                return targetInfo.readUInt64LE(at: offset)
            }

            if avId == AVPairID.eol.rawValue { break }
            offset += avLen
        }
        return nil
    }
}

// MARK: - Type 3: AUTHENTICATE (Build)

/// Builds the NTLM Type 3 (AUTHENTICATE) message.
struct NTLMAuthenticateMessage {
    let domain: String
    let username: String
    let workstation: String
    let lmResponse: Data
    let ntResponse: Data
    let flags: UInt32
    let hasMIC: Bool

    /// Serializes the Type 3 message.
    ///
    /// When `hasMIC` is true, the MIC field at offset 72 is zeroed out.
    /// The caller must compute the MIC and patch bytes 72-87 afterward.
    func serialize() -> Data {
        let domainBytes = utf16LE(domain.uppercased())
        let userBytes = utf16LE(username)
        let workstationBytes = utf16LE(workstation.uppercased())
        let sessionKeyBytes = Data()  // Empty — not using key exchange

        // Header is always 88 bytes (includes Version + MIC fields)
        let headerSize = 88
        var payloadOffset = headerSize

        let domainOffset = payloadOffset
        payloadOffset += domainBytes.count

        let userOffset = payloadOffset
        payloadOffset += userBytes.count

        let workstationOffset = payloadOffset
        payloadOffset += workstationBytes.count

        let lmOffset = payloadOffset
        payloadOffset += lmResponse.count

        let ntOffset = payloadOffset
        payloadOffset += ntResponse.count

        let sessionKeyOffset = payloadOffset

        var data = Data(capacity: payloadOffset + sessionKeyBytes.count)

        // Signature + MessageType
        data.append(ntlmSignature)
        data.appendUInt32LE(3)

        // LmChallengeResponseFields
        data.appendSecurityBuffer(length: lmResponse.count, offset: lmOffset)

        // NtChallengeResponseFields
        data.appendSecurityBuffer(length: ntResponse.count, offset: ntOffset)

        // DomainNameFields
        data.appendSecurityBuffer(length: domainBytes.count, offset: domainOffset)

        // UserNameFields
        data.appendSecurityBuffer(length: userBytes.count, offset: userOffset)

        // WorkstationFields
        data.appendSecurityBuffer(length: workstationBytes.count, offset: workstationOffset)

        // EncryptedRandomSessionKeyFields
        data.appendSecurityBuffer(length: sessionKeyBytes.count, offset: sessionKeyOffset)

        // NegotiateFlags
        data.appendUInt32LE(flags)

        // Version (8 bytes)
        data.append(ntlmVersion)

        // MIC (16 bytes) — zeroed; caller patches after computing
        data.append(Data(count: 16))

        // Payload
        data.append(domainBytes)
        data.append(userBytes)
        data.append(workstationBytes)
        data.append(lmResponse)
        data.append(ntResponse)
        data.append(sessionKeyBytes)

        return data
    }

    /// Builds the NTLMv2 client blob (temp structure) that forms part of the NtChallengeResponse.
    ///
    /// Structure:
    /// - RespType (1 byte): 0x01
    /// - HiRespType (1 byte): 0x01
    /// - Reserved1 (2 bytes): 0
    /// - Reserved2 (4 bytes): 0
    /// - TimeStamp (8 bytes): FILETIME
    /// - ChallengeFromClient (8 bytes): random
    /// - Reserved3 (4 bytes): 0
    /// - AvPairs: modified copy of server's TargetInfo
    /// - Reserved4 (4 bytes): 0
    static func buildClientBlob(
        timestamp: UInt64,
        clientChallenge: Data,
        targetInfo: Data,
        includeMICFlag: Bool = true
    ) -> Data {
        var blob = Data()

        blob.append(0x01)  // RespType
        blob.append(0x01)  // HiRespType
        blob.appendUInt16LE(0)  // Reserved1
        blob.appendUInt32LE(0)  // Reserved2
        blob.appendUInt64LE(timestamp)
        blob.append(clientChallenge)
        blob.appendUInt32LE(0)  // Reserved3

        // Append modified TargetInfo with MIC flag
        if includeMICFlag {
            blob.append(insertMICFlag(in: targetInfo))
        } else {
            blob.append(targetInfo)
        }

        blob.appendUInt32LE(0)  // Reserved4

        return blob
    }

    /// Inserts MsvAvFlags = 0x0002 (MIC present) into the TargetInfo AV_PAIR list,
    /// just before the MsvAvEOL terminator.
    private static func insertMICFlag(in targetInfo: Data) -> Data {
        var result = Data()
        var offset = 0
        var flagsInserted = false

        while offset + 4 <= targetInfo.count {
            let avId = targetInfo.readUInt16LE(at: offset)
            let avLen = Int(targetInfo.readUInt16LE(at: offset + 2))

            if avId == AVPairID.eol.rawValue {
                // Insert MIC flags pair before EOL if not already present
                if !flagsInserted {
                    result.appendUInt16LE(AVPairID.flags.rawValue)
                    result.appendUInt16LE(4)
                    result.appendUInt32LE(0x0000_0002)  // MIC present
                }
                // Append the EOL
                result.appendUInt16LE(0)
                result.appendUInt16LE(0)
                return result
            }

            if avId == AVPairID.flags.rawValue, avLen == 4 {
                // Existing flags — set the MIC bit
                let existingFlags = targetInfo.readUInt32LE(at: offset + 4)
                result.appendUInt16LE(AVPairID.flags.rawValue)
                result.appendUInt16LE(4)
                result.appendUInt32LE(existingFlags | 0x0000_0002)
                flagsInserted = true
                offset += 4 + avLen
                continue
            }

            // Copy the pair as-is
            guard offset + 4 + avLen <= targetInfo.count else { break }
            result.append(targetInfo[offset ..< offset + 4 + avLen])
            offset += 4 + avLen
        }

        // If we got here without finding EOL, append what we have
        return result
    }

    private func utf16LE(_ string: String) -> Data {
        let codeUnits = Array(string.utf16)
        guard !codeUnits.isEmpty else { return Data() }
        return codeUnits.withUnsafeBufferPointer { buf in
            Data(bytes: buf.baseAddress!, count: buf.count * 2)
        }
    }
}

// MARK: - Data Helpers

extension Data {
    mutating func appendUInt16LE(_ value: UInt16) {
        let le = value.littleEndian
        append(UInt8(truncatingIfNeeded: le))
        append(UInt8(truncatingIfNeeded: le >> 8))
    }

    mutating func appendUInt32LE(_ value: UInt32) {
        let le = value.littleEndian
        append(UInt8(truncatingIfNeeded: le))
        append(UInt8(truncatingIfNeeded: le >> 8))
        append(UInt8(truncatingIfNeeded: le >> 16))
        append(UInt8(truncatingIfNeeded: le >> 24))
    }

    mutating func appendUInt64LE(_ value: UInt64) {
        let le = value.littleEndian
        for shift in stride(from: 0, to: 64, by: 8) {
            append(UInt8(truncatingIfNeeded: le >> shift))
        }
    }

    mutating func appendSecurityBuffer(length: Int, offset: Int) {
        appendUInt16LE(UInt16(length))
        appendUInt16LE(UInt16(length))
        appendUInt32LE(UInt32(offset))
    }

    func readUInt16LE(at offset: Int) -> UInt16 {
        UInt16(self[offset]) | (UInt16(self[offset + 1]) << 8)
    }

    func readUInt32LE(at offset: Int) -> UInt32 {
        UInt32(self[offset])
            | (UInt32(self[offset + 1]) << 8)
            | (UInt32(self[offset + 2]) << 16)
            | (UInt32(self[offset + 3]) << 24)
    }

    func readUInt64LE(at offset: Int) -> UInt64 {
        var result: UInt64 = 0
        for i in 0..<8 {
            result |= UInt64(self[offset + i]) << (i * 8)
        }
        return result
    }
}
