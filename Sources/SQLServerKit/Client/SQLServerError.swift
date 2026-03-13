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
    case sqlExecutionError(message: String)
    // Specific transient error that should generally be retried: SQL Server deadlock (error 1205)
    case deadlockDetected(message: String)
    case invalidArgument(String)
    case databaseDoesNotExist(String)
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
            return "TDS error: \(error)"
        case .unsupportedPlatform:
            return "This platform is not supported."
        case .sqlExecutionError(let message):
            return message
        case .deadlockDetected(let message):
            return "Deadlock detected: \(message)"
        case .invalidArgument(let message):
            return message
        case .databaseDoesNotExist(let name):
            return "Database '\(name)' does not exist."
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
