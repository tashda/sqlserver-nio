import Foundation
import NIO
import NIOPosix
import SQLServerTDS

public enum SQLServerError: Swift.Error, CustomStringConvertible, LocalizedError {
    case clientShutdown
    case connectionClosed
    case timeout(description: String?, underlying: Swift.Error?)
    case authenticationFailed(message: String? = nil)
    case protocolError(TDSError)
    case unsupportedPlatform
    /// SQL Server returned an error. `details` carries the error number,
    /// severity, state, line and procedure, plus every message of the batch.
    case sqlExecutionError(message: String, details: SQLServerErrorDetails? = nil)
    /// The session was chosen as a deadlock victim (error 1205). SQL Server
    /// rolled back the whole transaction, so only a retry of the entire
    /// transaction from its start is safe.
    case deadlockDetected(message: String, details: SQLServerErrorDetails? = nil)
    /// The commit acknowledgement was lost; the transaction may have committed.
    case commitOutcomeUnknown(Swift.Error)
    /// The TLS handshake failed. `SQLServerTLSFailure` says why (for a
    /// certificate: untrusted, self-signed, expired, not yet valid or for
    /// another host) and describes the certificate the server presented.
    case tlsFailed(SQLServerTLSFailure)
    case invalidArgument(String)
    case databaseDoesNotExist(String)
    case notImplemented(String)
    case transient(Swift.Error)
    case unknown(Swift.Error)

    public var description: String {
        switch self {
        case .clientShutdown:
            return "The client has been shut down."
        case .connectionClosed:
            return "The connection was closed."
        case .timeout(let description, _):
            if let description {
                return "Connection timed out: \(description)"
            } else {
                return "Connection timed out. The server may be unreachable."
            }
        case .authenticationFailed(let message):
            if let message {
                return message
            } else {
                return "Authentication failed."
            }
        case .protocolError(let error):
            return error.description
        case .tlsFailed(let failure):
            return failure.message
        case .unsupportedPlatform:
            return "This platform is not supported."
        case .sqlExecutionError(let message, _):
            return message
        case .deadlockDetected(let message, _):
            return "Deadlock detected: \(message)"
        case .commitOutcomeUnknown(let error):
            return "Commit outcome is unknown; check the database before retrying: \(error)"
        case .invalidArgument(let message):
            return message
        case .databaseDoesNotExist(let name):
            return "Database '\(name)' does not exist."
        case .notImplemented(let message):
            return message
        case .transient(let error):
            return Self.describeNIOError(error)
        case .unknown(let error):
            return Self.describeNIOError(error)
        }
    }

    public var errorDescription: String? {
        return description
    }

    /// Translate common NIO errors into user-friendly messages.
    private static func describeNIOError(_ error: Swift.Error) -> String {
        // IOError has errnoCode — use it directly for reliable matching
        if let ioError = error as? IOError {
            return describeIOError(ioError)
        }

        // Use String(describing:) which includes the real description, not the
        // generic NSError bridge that just shows "NIOCore.IOError error N"
        let desc = String(describing: error).lowercased()

        // ChannelError
        if desc.contains("channelerror") || desc.contains("connecttimeout") {
            return "Connection timed out. The server may be unreachable."
        }

        // NIOConnectionError
        if desc.contains("nioconnectionerror") || desc.contains("connecterror") {
            if desc.contains("connection refused") {
                return "Connection refused. The server may not be running or the port may be wrong."
            }
            return "Could not connect to the server."
        }

        // DNS resolution failures
        if desc.contains("name or service not known")
            || desc.contains("nodename nor servname provided")
            || desc.contains("getaddrinfo")
            || desc.contains("no such host") {
            return "Could not resolve hostname. Check the server address."
        }

        // Fallback: use String(describing:) which is more informative than localizedDescription
        return String(describing: error)
    }

    /// Translate IOError errno codes into user-friendly messages.
    private static func describeIOError(_ error: IOError) -> String {
        switch error.errnoCode {
        case 1:  // EPERM
            return "Connection failed. The server may not be running or the address is unreachable."
        case 13: // EACCES
            return "Permission denied when connecting to the server."
        case 51: // ENETUNREACH
            return "Network is unreachable."
        case 60: // ETIMEDOUT
            return "Connection timed out. The server may be unreachable."
        case 61: // ECONNREFUSED
            return "Connection refused. The server may not be running or the port may be wrong."
        case 64: // EHOSTDOWN
            return "The server appears to be down."
        case 65: // EHOSTUNREACH
            return "No route to host. The server may be unreachable."
        default:
            // Use String(describing:) which includes the actual reason string
            return "Connection failed: \(String(describing: error))"
        }
    }
}

/// The server-reported fields of a SQL Server error (MS-TDS ERROR token).
public struct SQLServerErrorDetails: Sendable {
    /// The first error the server reported for the request.
    public let primary: SQLServerStreamMessage
    /// Every error and informational message the request produced, in order.
    public let messages: [SQLServerStreamMessage]

    public init(primary: SQLServerStreamMessage, messages: [SQLServerStreamMessage]) {
        self.primary = primary
        self.messages = messages
    }

    public var number: Int32 { primary.number }
    public var severity: UInt8 { primary.severity }
    public var state: UInt8 { primary.state }
    public var lineNumber: Int32 { primary.lineNumber }
    public var procedureName: String { primary.procedureName }
    public var serverName: String { primary.serverName }

    /// All error messages, excluding informational ones.
    public var errors: [SQLServerStreamMessage] { messages.filter { $0.kind == .error } }
}

extension SQLServerError {
    /// Builds the error for a request whose messages include at least one
    /// error. Returns nil when there is none.
    public static func fromServerMessages(_ messages: [SQLServerStreamMessage]) -> SQLServerError? {
        guard let first = messages.first(where: { $0.kind == .error }) else { return nil }
        let details = SQLServerErrorDetails(primary: first, messages: messages)
        if messages.contains(where: { $0.kind == .error && $0.number == 1205 }) {
            let deadlock = messages.first(where: { $0.kind == .error && $0.number == 1205 })!
            return .deadlockDetected(message: deadlock.message, details: details)
        }
        return .sqlExecutionError(message: first.message, details: details)
    }

    /// Server error details when this error came from SQL Server.
    public var serverDetails: SQLServerErrorDetails? {
        switch self {
        case .sqlExecutionError(_, let details), .deadlockDetected(_, let details):
            return details
        default:
            return nil
        }
    }

    /// The SQL Server error number, when this error came from SQL Server.
    public var serverErrorNumber: Int32? { serverDetails?.number }

    /// True when the physical connection is gone or unusable and must not be
    /// used again. The outcome of a request that was executing is unknown.
    public var isConnectionLost: Bool {
        switch self {
        case .connectionClosed, .transient:
            return true
        case .protocolError(let tds):
            return tds != .cancelled
        case .sqlExecutionError(_, let details):
            // Severity 20 and above terminates the session (fatal errors).
            return (details?.severity ?? 0) >= 20
        default:
            return false
        }
    }

    /// True for failures that SQL Server documents as transient: deadlock
    /// victims, lock or resource timeouts, Azure SQL throttling and failover
    /// errors. Retrying is only safe when the whole unit of work is repeated
    /// and is known to be idempotent or was rolled back.
    public var isTransient: Bool {
        switch self {
        case .deadlockDetected, .transient:
            return true
        case .sqlExecutionError(_, let details):
            guard let number = details?.number else { return false }
            return Self.transientErrorNumbers.contains(number)
        default:
            return false
        }
    }

    /// Error numbers the Microsoft drivers treat as transient (see
    /// Microsoft.Data.SqlClient SqlConfigurableRetryFactory defaults and
    /// Azure SQL transient fault guidance).
    public static let transientErrorNumbers: Set<Int32> = [
        1205,   // deadlock victim
        1222,   // lock request timeout
        233, 64, 10053, 10054, 10060, 10928, 10929,
        40143, 40197, 40501, 40540, 40613, 42108, 42109,
        49918, 49919, 49920, 4060, 4221, 615, 926,
    ]
}

/// Why a TLS handshake with SQL Server failed.
public struct SQLServerTLSFailure: Sendable, CustomStringConvertible {
    public enum Kind: String, Sendable {
        /// The certificate chain does not lead to a trusted root.
        case certificateUntrusted
        /// The certificate signs itself and is not trusted.
        case certificateSelfSigned
        case certificateExpired
        case certificateNotYetValid
        /// The certificate is trusted but names another host.
        case certificateNameMismatch
        /// The server offers no TLS version the client accepts (for example
        /// SQL Server 2008 R2 without its TLS 1.2 update).
        case protocolVersionTooOld
        case handshakeFailed
    }

    public let kind: Kind
    public let message: String
    /// The name the certificate had to match.
    public let expectedHost: String?
    /// The certificate the server presented, when it could be read.
    public let certificate: SQLServerCertificateSummary?

    public init(kind: Kind, message: String, expectedHost: String? = nil, certificate: SQLServerCertificateSummary? = nil) {
        self.kind = kind
        self.message = message
        self.expectedHost = expectedHost
        self.certificate = certificate
    }

    public var description: String { message }

    /// True for the failures a user can resolve by trusting the certificate
    /// or naming the host it was issued for.
    public var isCertificateProblem: Bool {
        switch kind {
        case .certificateUntrusted, .certificateSelfSigned, .certificateExpired,
             .certificateNotYetValid, .certificateNameMismatch:
            return true
        case .protocolVersionTooOld, .handshakeFailed:
            return false
        }
    }
}

/// What a server certificate says about itself.
public struct SQLServerCertificateSummary: Sendable, Equatable {
    public let subject: String
    public let issuer: String
    /// DNS and IP subject alternative names; the common name when there are none.
    public let names: [String]
    public let notValidBefore: Date
    public let notValidAfter: Date
    public let isSelfSigned: Bool
    /// Hex SHA-256 of the DER encoding, for comparing with what the server's
    /// administrator reports.
    public let sha256Fingerprint: String

    public init(subject: String, issuer: String, names: [String], notValidBefore: Date, notValidAfter: Date,
                isSelfSigned: Bool, sha256Fingerprint: String) {
        self.subject = subject
        self.issuer = issuer
        self.names = names
        self.notValidBefore = notValidBefore
        self.notValidAfter = notValidAfter
        self.isSelfSigned = isSelfSigned
        self.sha256Fingerprint = sha256Fingerprint
    }
}
