import Foundation
import NIO
import NIOPosix
import SQLServerTDS

public enum SQLServerError: Swift.Error, CustomStringConvertible, LocalizedError {
    case clientShutdown
    case connectionClosed
    case timeout(description: String?, underlying: Swift.Error?)
    case authenticationFailed
    case protocolError(TDSError)
    case unsupportedPlatform
    case sqlExecutionError(message: String)
    case invalidArgument(String)
    case transient(Swift.Error)
    case unknown(Swift.Error)

    public var description: String {
        switch self {
        case .clientShutdown:
            return "SQLServerError.clientShutdown"
        case .connectionClosed:
            return "SQLServerError.connectionClosed"
        case .timeout(let description, _):
            if let description {
                return "SQLServerError.timeout(\(description))"
            } else {
                return "SQLServerError.timeout"
            }
        case .authenticationFailed:
            return "SQLServerError.authenticationFailed"
        case .protocolError(let error):
            return "SQLServerError.protocolError(\(error))"
        case .unsupportedPlatform:
            return "SQLServerError.unsupportedPlatform"
        case .sqlExecutionError(let message):
            return "SQLServerError.sqlExecutionError(\(message))"
        case .invalidArgument(let message):
            return "SQLServerError.invalidArgument(\(message))"
        case .transient(let error):
            return "SQLServerError.transient(\(error))"
        case .unknown(let error):
            return "SQLServerError.unknown(\(error))"
        }
    }
    
    public var errorDescription: String? {
        return description
    }
}
