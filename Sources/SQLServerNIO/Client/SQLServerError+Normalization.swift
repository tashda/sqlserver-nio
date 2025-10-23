import NIO
import NIOPosix

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
            default:
                return .protocolError(tds)
            }
        }
        if let channelError = error as? ChannelError {
            switch channelError {
            case .ioOnClosedChannel, .outputClosed, .eof:
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
