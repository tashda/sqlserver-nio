import Foundation
import Crypto

/// Low-level cryptographic operations for NTLMv2 authentication.
///
/// All functions are pure and stateless, making them easy to test against
/// known vectors from the MS-NLMP specification.
enum NTLMCrypto {

    // MARK: - NTLM Key Derivation

    /// Computes the NT hash: `MD4(UTF-16LE(password))`.
    static func ntHash(password: String) -> Data {
        let utf16le = Array(password.utf16).withUnsafeBufferPointer { buf in
            Data(bytes: buf.baseAddress!, count: buf.count * 2)
        }
        return md4(utf16le)
    }

    /// Computes the NTv2 hash: `HMAC-MD5(ntHash, UTF-16LE(UPPER(user) + domain))`.
    ///
    /// Per MS-NLMP section 3.3.2, only the username is uppercased; the domain
    /// is concatenated as-is.
    static func ntv2Hash(ntHash: Data, username: String, domain: String) -> Data {
        let identity = username.uppercased() + domain
        let utf16le = Array(identity.utf16).withUnsafeBufferPointer { buf in
            Data(bytes: buf.baseAddress!, count: buf.count * 2)
        }
        return hmacMD5(key: ntHash, data: utf16le)
    }

    /// Computes the NT proof string: `HMAC-MD5(ntv2Hash, serverChallenge || clientBlob)`.
    static func ntProofStr(ntv2Hash: Data, serverChallenge: Data, clientBlob: Data) -> Data {
        var combined = serverChallenge
        combined.append(clientBlob)
        return hmacMD5(key: ntv2Hash, data: combined)
    }

    /// Computes the session base key: `HMAC-MD5(ntv2Hash, ntProofStr)`.
    static func sessionBaseKey(ntv2Hash: Data, ntProofStr: Data) -> Data {
        hmacMD5(key: ntv2Hash, data: ntProofStr)
    }

    /// Computes the Message Integrity Code: `HMAC-MD5(sessionKey, type1 || type2 || type3)`.
    static func computeMIC(sessionKey: Data, negotiate: Data, challenge: Data, authenticate: Data) -> Data {
        var combined = negotiate
        combined.append(challenge)
        combined.append(authenticate)
        return hmacMD5(key: sessionKey, data: combined)
    }

    // MARK: - Timestamp

    /// Converts a `Date` to Windows FILETIME (100-nanosecond intervals since January 1, 1601).
    static func windowsTimestamp(from date: Date) -> UInt64 {
        // Seconds between Windows epoch (1601-01-01) and Unix epoch (1970-01-01)
        let epochDelta: TimeInterval = 11_644_473_600
        let totalSeconds = date.timeIntervalSince1970 + epochDelta
        return UInt64(totalSeconds * 10_000_000)
    }

    // MARK: - Primitives

    /// HMAC-MD5 using swift-crypto's `Insecure.MD5`.
    static func hmacMD5(key: Data, data: Data) -> Data {
        let symmetricKey = SymmetricKey(data: key)
        let mac = HMAC<Insecure.MD5>.authenticationCode(for: data, using: symmetricKey)
        return Data(mac)
    }

    // MARK: - MD4 (Pure Swift)

    /// Pure Swift MD4 implementation (RFC 1320).
    ///
    /// MD4 is cryptographically broken but required by the NTLM protocol for
    /// computing the NT password hash. This avoids depending on CommonCrypto's
    /// deprecated `CC_MD4`.
    static func md4(_ input: Data) -> Data {
        var msg = Array(input)
        let bitLen = UInt64(msg.count) &* 8

        // Padding: append 1-bit, then zeros until length == 56 mod 64
        msg.append(0x80)
        while msg.count % 64 != 56 {
            msg.append(0)
        }
        // Append original bit length as 64-bit little-endian
        for i in 0..<8 {
            msg.append(UInt8(truncatingIfNeeded: bitLen >> (i * 8)))
        }

        // Initial hash state
        var h0: UInt32 = 0x6745_2301
        var h1: UInt32 = 0xEFCD_AB89
        var h2: UInt32 = 0x98BA_DCFE
        var h3: UInt32 = 0x1032_5476

        func rotl(_ v: UInt32, _ n: Int) -> UInt32 { (v << n) | (v >> (32 - n)) }

        // Process each 64-byte block
        for blockStart in stride(from: 0, to: msg.count, by: 64) {
            // Decode 16 little-endian 32-bit words
            var x = [UInt32](repeating: 0, count: 16)
            for j in 0..<16 {
                let o = blockStart + j * 4
                x[j] = UInt32(msg[o])
                    | (UInt32(msg[o + 1]) << 8)
                    | (UInt32(msg[o + 2]) << 16)
                    | (UInt32(msg[o + 3]) << 24)
            }

            var a = h0, b = h1, c = h2, d = h3

            // Round 1: F(b,c,d) = (b & c) | (~b & d)
            let s1 = [3, 7, 11, 19]
            for j in 0..<16 {
                let f = (b & c) | (~b & d)
                let t = rotl(a &+ f &+ x[j], s1[j % 4])
                (a, b, c, d) = (d, t, b, c)
            }

            // Round 2: G(b,c,d) = (b & c) | (b & d) | (c & d)
            let r2 = [0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15]
            let s2 = [3, 5, 9, 13]
            for j in 0..<16 {
                let g = (b & c) | (b & d) | (c & d)
                let t = rotl(a &+ g &+ x[r2[j]] &+ 0x5A82_7999, s2[j % 4])
                (a, b, c, d) = (d, t, b, c)
            }

            // Round 3: H(b,c,d) = b ^ c ^ d
            let r3 = [0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15]
            let s3 = [3, 9, 11, 15]
            for j in 0..<16 {
                let h = b ^ c ^ d
                let t = rotl(a &+ h &+ x[r3[j]] &+ 0x6ED9_EBA1, s3[j % 4])
                (a, b, c, d) = (d, t, b, c)
            }

            h0 = h0 &+ a
            h1 = h1 &+ b
            h2 = h2 &+ c
            h3 = h3 &+ d
        }

        // Produce 16-byte digest in little-endian
        var result = Data(count: 16)
        for (i, word) in [h0, h1, h2, h3].enumerated() {
            let le = word.littleEndian
            withUnsafeBytes(of: le) { result.replaceSubrange(i * 4 ..< i * 4 + 4, with: $0) }
        }
        return result
    }
}
