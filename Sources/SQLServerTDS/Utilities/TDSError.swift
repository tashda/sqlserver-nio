import Foundation

public enum TDSError: Error, LocalizedError, CustomStringConvertible, Equatable {
    case protocolError(String)
    case connectionClosed
    case invalidCredentials(String)
    case needMoreData
    case sslError(String)
    /// The request was cancelled. The server acknowledged the cancellation,
    /// so the connection can run further requests. Work the server finished
    /// before the cancellation arrived is not rolled back by the cancellation.
    case cancelled
    /// The request exceeded its deadline and was cancelled on the server.
    case requestTimeout(String)

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
        case .cancelled:
            description = "request cancelled"
        case .requestTimeout(let message):
            description = message
        }
        return "TDS error: \(description)"
    }
}
