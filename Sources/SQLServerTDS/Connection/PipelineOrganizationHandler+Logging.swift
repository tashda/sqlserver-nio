import NIO
import Logging

extension PipelineOrganizationHandler {
    var stateDescription: String {
        switch self.state {
        case .start: return "start"
        case .sslHandshake: return "sslHandshake"
        case .allDone: return "allDone"
        }
    }
}

extension PipelineOrganizationHandler {
    public func channelInactive(context: ChannelHandlerContext) {
        self.logger.debug("PipelineOrganizationHandler.channelInactive state=\(stateDescription)")
        switch self.state {
        case .sslHandshake(let hs):
            hs.outputPromise.fail(TDSError.connectionClosed)
        default:
            break
        }
        context.fireChannelInactive()
    }
}
