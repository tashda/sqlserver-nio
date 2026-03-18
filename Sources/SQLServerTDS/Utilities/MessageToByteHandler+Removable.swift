import NIOCore

/// NIO's `MessageToByteHandler` does not conform to `RemovableChannelHandler` out of the box.
/// We need this conformance so the TDS pipeline can remove the packet encoder during
/// SSL handshake completion without causing "Unremovable handler" errors.
extension MessageToByteHandler: @retroactive RemovableChannelHandler {}
