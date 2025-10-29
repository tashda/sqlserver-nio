import NIO

extension PipelineOrganizationHandler {
    func failHandshakeIfPending() {
        switch self.state {
        case .sslHandshake(let hs):
            hs.outputPromise.fail(TDSError.connectionClosed)
        default:
            break
        }
    }
}

