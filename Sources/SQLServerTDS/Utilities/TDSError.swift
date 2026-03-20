import Foundation

public enum TDSError: Error, LocalizedError, CustomStringConvertible, Equatable {
    case protocolError(String)
    case connectionClosed
    case invalidCredentials(String)
    case needMoreData
    case sslError(String)

    /// See `LocalizedError`.
    public var errorDescription: String? {
        return self.description
    }

    /// See `CustomStringConvertible`.
    public var description: String {
        let description: String
        switch self {
        case .protocolError(let message):
            description = "protocol error: \(message)"
        case .connectionClosed:
            description = "connection closed"
        case .invalidCredentials(let message):
            description = message
        case .needMoreData:
            description = "need more data"
        case .sslError(let message):
            description = "SSL error: \(message)"
        }
        return "TDS error: \(description)"
    }
}
