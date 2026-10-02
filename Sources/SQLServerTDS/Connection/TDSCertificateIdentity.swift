import Foundation
import NIOCore
import NIOSSL
import Crypto

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

/// Checks that a server certificate names the expected host (RFC 6125).
///
/// NIOSSL's full verification also accepts a certificate whose IP address
/// matches the address the socket connected to, whatever host name was
/// expected. With "Host Name In Certificate" (SSMS, JDBC `hostNameInCertificate`)
/// the certificate must name that host, so the driver repeats the check
/// without the socket-address fallback:
///
/// - An expected DNS name matches a DNS subject alternative name, with a
///   wildcard allowed only as the whole left-most label (`*.example.com`).
/// - An expected IP address matches an IP subject alternative name.
/// - The subject common name is used only when the certificate has no DNS or
///   IP subject alternative names.
public enum TDSCertificateIdentity {
    public static func matches(_ certificate: NIOSSLCertificate, expectedHost: String) -> Bool {
        let host = normalize(expectedHost)
        guard !host.isEmpty else { return false }
        let expectedIP = ipBytes(host)

        var sawIdentity = false
        for name in certificate._subjectAlternativeNames() {
            switch name.nameType {
            case .dnsName:
                sawIdentity = true
                let dnsName = name.contents.withUnsafeBufferPointer { String(decoding: $0, as: UTF8.self) }
                if expectedIP == nil, matchesDNS(pattern: dnsName, host: host) { return true }
            case .ipAddress:
                sawIdentity = true
                let bytes = name.contents.withUnsafeBufferPointer { Array($0) }
                if let expectedIP, bytes == expectedIP { return true }
            default:
                continue
            }
        }
        guard !sawIdentity else { return false }

        for entry in certificate.subjectName where entry.type == .commonName {
            if let expectedIP {
                if ipBytes(normalize(entry.value)) == expectedIP { return true }
            } else if matchesDNS(pattern: entry.value, host: host) {
                return true
            }
        }
        return false
    }

    private static func normalize(_ name: String) -> String {
        var value = name.lowercased()
        if value.hasPrefix("["), value.hasSuffix("]") {
            value = String(value.dropFirst().dropLast())
        }
        if value.hasSuffix(".") { value.removeLast() }
        return value
    }

    static func matchesDNS(pattern: String, host: String) -> Bool {
        let pattern = normalize(pattern)
        guard !pattern.isEmpty else { return false }
        if !pattern.contains("*") { return pattern == host }
        // Only "*.rest", with at least two labels after the wildcard, and the
        // wildcard never matches an empty label or across dots.
        guard pattern.hasPrefix("*."), pattern.dropFirst(2).contains("."),
              !pattern.dropFirst(2).contains("*"),
              let dot = host.firstIndex(of: ".") else { return false }
        let firstLabel = host[..<dot]
        guard !firstLabel.isEmpty, !firstLabel.hasPrefix("xn--") else { return false }
        return host[dot...] == pattern.dropFirst(1)
    }

    static func ipBytes(_ value: String) -> [UInt8]? {
        var v4 = in_addr()
        if inet_pton(AF_INET, value, &v4) == 1 {
            return withUnsafeBytes(of: &v4) { Array($0) }
        }
        var v6 = in6_addr()
        if inet_pton(AF_INET6, value, &v6) == 1 {
            return withUnsafeBytes(of: &v6) { Array($0) }
        }
        return nil
    }

    /// Returns an error when full verification is configured and the peer
    /// certificate does not name `expectedHost`.
    static func verify(
        handler: NIOSSLClientHandler?,
        configuration: TLSConfiguration?,
        expectedHost: String?
    ) -> Error? {
        guard configuration?.certificateVerification == .fullVerification,
              let expectedHost, !expectedHost.isEmpty else { return nil }
        guard let certificate = handler?.peerCertificate else {
            return TDSError.tlsHandshake(.certificateVerification, "The server did not present a certificate")
        }
        guard matches(certificate, expectedHost: expectedHost) else {
            return TDSError.tlsHandshake(.certificateName, "The server certificate does not name '\(expectedHost)'. Check the server name or Host Name In Certificate.")
        }
        return nil
    }

    /// Plain facts about a certificate, for explaining a failed check.
    public struct Description: Sendable {
        public let subject: String
        public let issuer: String
        public let names: [String]
        public let notValidBefore: Date
        public let notValidAfter: Date
        public let isSelfSigned: Bool
        public let sha256Fingerprint: String
    }

    public static func describe(_ certificate: NIOSSLCertificate) -> Description {
        func commonName(_ entries: [SSLCertificateName]) -> String {
            entries.first { $0.type == .commonName }?.value
                ?? entries.map(\.value).joined(separator: ", ")
        }
        var names: [String] = []
        for name in certificate._subjectAlternativeNames() {
            switch name.nameType {
            case .dnsName:
                names.append(name.contents.withUnsafeBufferPointer { String(decoding: $0, as: UTF8.self) })
            case .ipAddress:
                let bytes = name.contents.withUnsafeBufferPointer { Array($0) }
                names.append(bytes.count == 4
                    ? bytes.map(String.init).joined(separator: ".")
                    : stride(from: 0, to: bytes.count, by: 2).map { String(format: "%02x%02x", bytes[$0], bytes[$0 + 1]) }.joined(separator: ":"))
            default:
                continue
            }
        }
        let subject = commonName(certificate.subjectName)
        let issuer = commonName(certificate.issuerName)
        if names.isEmpty, !subject.isEmpty { names = [subject] }
        let der = (try? certificate.toDERBytes()) ?? []
        let fingerprint = SHA256.hash(data: der).map { String(format: "%02X", $0) }.joined(separator: ":")
        return Description(
            subject: subject,
            issuer: issuer,
            names: names,
            notValidBefore: Date(timeIntervalSince1970: TimeInterval(certificate.notValidBefore)),
            notValidAfter: Date(timeIntervalSince1970: TimeInterval(certificate.notValidAfter)),
            isSelfSigned: certificate.subjectName == certificate.issuerName,
            sha256Fingerprint: fingerprint
        )
    }
}
