import NIO
import NIOPosix
import SQLServerTDS

public enum SQLServerError: Swift.Error, CustomStringConvertible {
    case clientShutdown
    case connectionClosed
    case timeout(description: String?, underlying: Swift.Error?)
    case authenticationFailed
    case protocolError(TDSError)
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
        case .transient(let error):
            return "SQLServerError.transient(\(error))"
        case .unknown(let error):
            return "SQLServerError.unknown(\(error))"
        }
    }
}
