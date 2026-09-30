import NIO
import NIOPosix
import SQLServerTDS
import NIOSSL

extension SQLServerError {
    static func normalize(_ error: Swift.Error) -> SQLServerError {
        if let sqlError = error as? SQLServerError {
            return sqlError
        }
        if let tds = error as? TDSError {
            switch tds {
            case .connectionClosed:
                return .connectionClosed
            case .invalidCredentials(let message):
                return .authenticationFailed(message: message)
            case .requestTimeout(let message):
                return .timeout(description: message, underlying: tds)
            case .tlsHandshake(let kind, let message):
                let mapped: SQLServerTLSFailure.Kind
                switch kind {
                case .certificateVerification: mapped = .certificateUntrusted
                case .certificateName: mapped = .certificateNameMismatch
                case .protocolVersion: mapped = .protocolVersionTooOld
                case .other: mapped = .handshakeFailed
                }
                return .tlsFailed(SQLServerTLSFailure(kind: mapped, message: message))
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
        if let sslError = error as? NIOSSLError {
            // After the handshake, a TLS failure means the transport broke
            // (for example a TCP reset surfacing as an unclean shutdown).
            if case .uncleanShutdown = sslError {
                return .connectionClosed
            }
            return .protocolError(.sslError(String(describing: sslError)))
        }
        if let channelError = error as? ChannelError {
            switch channelError {
            case .ioOnClosedChannel, .outputClosed, .eof, .alreadyClosed:
                return .connectionClosed
            case .connectTimeout:
                return .transient(channelError)
            default:
                return .unknown(channelError)
            }
        }
        if let nioError = error as? NIOConnectionError {
            return .transient(nioError)
        }
        // IOError from NIO (POSIX errno-based errors like connection refused, no route to host)
        if let ioError = error as? IOError {
            return .transient(ioError)
        }
        return .unknown(error)
    }
}
