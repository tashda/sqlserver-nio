import Foundation
import NIOCore
import Logging

public class RpcRequest: TDSRequest, @unchecked Sendable {
    private let rpcMessage: TDSMessages.RpcRequestMessage

    public let onRow: (@Sendable (TDSRow) -> Void)?
    public let onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)?
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)?
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)?
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)?
    public let resultPromise: EventLoopPromise<[TDSData]>?
    public let stream: Bool = false // RPC requests are always non-streaming
    public let onData: (@Sendable (TDSData) -> Void)? = nil // RPC requests don't use onData

    public var packetType: TDSPacket.HeaderType { .rpc }

    public init(
        rpcMessage: TDSMessages.RpcRequestMessage,
        onRow: (@Sendable (TDSRow) -> Void)? = nil,
        onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)? = nil,
        onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil,
        onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil,
        resultPromise: EventLoopPromise<[TDSData]>? = nil
    ) {
        self.rpcMessage = rpcMessage
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onDone = onDone
        self.onMessage = onMessage
        self.onReturnValue = onReturnValue
        self.resultPromise = resultPromise
    }

    public func log(to logger: Logger) {
        logger.debug("Sending RPC request for: \(rpcMessage.procedureName)")
    }

    public func serialize(into buffer: inout ByteBuffer) throws {
        try rpcMessage.serialize(into: &buffer)
    }
}
