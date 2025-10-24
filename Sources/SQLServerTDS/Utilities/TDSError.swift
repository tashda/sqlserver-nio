import Foundation

public enum TDSError: Error, LocalizedError, CustomStringConvertible {
    case protocolError(String)
    case connectionClosed
    case invalidCredentials
    case needMoreData
    
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
        case .invalidCredentials:
            description = "Invalid login credentials"
        case .needMoreData:
            description = "need more data"
        }
        return "TDS error: \(description)"
    }
}
