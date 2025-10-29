import NIO
import NIOPosix
import SQLServerTDS

extension SQLServerError {
    static func normalize(_ error: Swift.Error) -> SQLServerError {
        if let sqlError = error as? SQLServerError {
            return sqlError
        }
        if let tds = error as? TDSError {
            switch tds {
            case .connectionClosed:
                return .connectionClosed
            case .invalidCredentials:
                return .authenticationFailed
            case .protocolError(let message):
                // Map protocol errors that explicitly signal a timeout to SQLServerError.timeout
                if message.localizedCaseInsensitiveContains("timeout") {
                    return .timeout(description: message, underlying: tds)
                }
                return .protocolError(tds)
            default:
                return .protocolError(tds)
            }
        }
        if let channelError = error as? ChannelError {
            switch channelError {
            case .ioOnClosedChannel, .outputClosed, .eof, .alreadyClosed:
                return .connectionClosed
            default:
                return .unknown(channelError)
            }
        }
        if let nioError = error as? NIOConnectionError {
            return .transient(nioError)
        }
        return .unknown(error)
    }
}
