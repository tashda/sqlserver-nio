import Foundation
import Logging

/// NTLMv2 authenticator for SQL Server Windows Integrated authentication.
///
/// Implements the 3-message NTLMv2 exchange (NEGOTIATE → CHALLENGE → AUTHENTICATE)
/// as a pure challenge-response protocol. Unlike Kerberos, this does not require a
/// reachable KDC — it works with just username, password, and domain.
///
/// This matches the authentication behavior of jTDS and tedious when explicit
/// credentials are provided for Windows Integrated authentication.
final class NTLMv2Authenticator: TDSAuthenticator, @unchecked Sendable {
    private let username: String
    private let domain: String
    private let workstation: String
    private let logger: Logger

    // Pre-computed key material
    private let ntHashValue: Data
    private let ntv2HashValue: Data

    // Saved messages for MIC computation (set during the exchange)
    private var negotiateMessage: Data?

    enum NTLMError: Error, CustomStringConvertible {
        case emptyCredentials
        case challengeParseFailed(String)

        var description: String {
            switch self {
            case .emptyCredentials:
                return "NTLMv2 requires non-empty username and password"
            case .challengeParseFailed(let detail):
                return "Failed to parse NTLM challenge: \(detail)"
            }
        }
    }

    init(username: String, password: String, domain: String, server: String, port: Int, logger: Logger) throws {
        guard !username.isEmpty, !password.isEmpty else {
            throw NTLMError.emptyCredentials
        }
        self.username = username
        self.domain = domain
        self.workstation = Self.localWorkstation()
        self.logger = logger
        self.ntHashValue = NTLMCrypto.ntHash(password: password)
        self.ntv2HashValue = NTLMCrypto.ntv2Hash(ntHash: ntHashValue, username: username, domain: domain)
        logger.debug("NTLMv2 authenticator created for \(username)@\(domain)")
    }

    // MARK: - TDSAuthenticator

    func initialToken() throws -> Data {
        let negotiate = NTLMNegotiateMessage(domain: domain, workstation: workstation)
        let data = negotiate.serialize()
        self.negotiateMessage = data
        logger.debug("NTLM NEGOTIATE produced (\(data.count) bytes)")
        return data
    }

    func continueAuthentication(serverToken: Data) throws -> (Data?, Bool) {
        let challenge: NTLMChallengeMessage
        do {
            challenge = try NTLMChallengeMessage.parse(serverToken)
        } catch {
            throw NTLMError.challengeParseFailed("\(error)")
        }

        logger.debug("NTLM CHALLENGE received: \(serverToken.count) bytes, flags=0x\(String(challenge.negotiateFlags, radix: 16))")

        // Generate 8 random bytes for client challenge
        var clientChallenge = Data(count: 8)
        for i in 0..<8 {
            clientChallenge[i] = UInt8.random(in: 0...255)
        }

        // Use server's timestamp if available, otherwise current time
        let timestamp = challenge.timestamp ?? NTLMCrypto.windowsTimestamp(from: Date())
        let hasMIC = challenge.timestamp != nil

        // Build the client blob
        let clientBlob = NTLMAuthenticateMessage.buildClientBlob(
            timestamp: timestamp,
            clientChallenge: clientChallenge,
            targetInfo: challenge.targetInfo,
            includeMICFlag: hasMIC
        )

        // Compute NTv2 response
        let ntProof = NTLMCrypto.ntProofStr(
            ntv2Hash: ntv2HashValue,
            serverChallenge: challenge.serverChallenge,
            clientBlob: clientBlob
        )
        let sessionKey = NTLMCrypto.sessionBaseKey(ntv2Hash: ntv2HashValue, ntProofStr: ntProof)

        // NtChallengeResponse = ntProofStr || clientBlob
        var ntResponse = ntProof
        ntResponse.append(clientBlob)

        // LM response: 24 zero bytes (NTLMv2 mode — server ignores this)
        let lmResponse = Data(count: 24)

        // Build Type 3 AUTHENTICATE message
        let authMessage = NTLMAuthenticateMessage(
            domain: domain,
            username: username,
            workstation: workstation,
            lmResponse: lmResponse,
            ntResponse: ntResponse,
            flags: challenge.negotiateFlags,
            hasMIC: hasMIC
        )
        var authData = authMessage.serialize()

        // Compute and insert MIC if server provided a timestamp
        if hasMIC, let negotiate = negotiateMessage {
            let mic = NTLMCrypto.computeMIC(
                sessionKey: sessionKey,
                negotiate: negotiate,
                challenge: serverToken,
                authenticate: authData
            )
            authData.replaceSubrange(72..<88, with: mic)
        }

        logger.debug("NTLM AUTHENTICATE produced (\(authData.count) bytes, MIC=\(hasMIC))")
        return (authData, false)
    }

    // MARK: - Private

    private static func localWorkstation() -> String {
        ProcessInfo.processInfo.hostName
            .components(separatedBy: ".")
            .first?
            .uppercased() ?? "WORKSTATION"
    }
}
