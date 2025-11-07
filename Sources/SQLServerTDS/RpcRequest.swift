import NIOCore
import Logging

public class RpcRequest: TDSRequest {
    private let rpcMessage: TDSMessages.RpcRequestMessage

    public let onRow: ((TDSRow) -> Void)?
    public let onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)?
    public let onDone: ((TDSTokens.DoneToken) -> Void)?
    public let onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)?
    public let onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)?
    public let resultPromise: EventLoopPromise<[TDSData]>?
    public let stream: Bool = false // RPC requests are always non-streaming
    public let onData: ((TDSData) -> Void)? = nil // RPC requests don't use onData

    public init(
        rpcMessage: TDSMessages.RpcRequestMessage,
        onRow: ((TDSRow) -> Void)? = nil,
        onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? = nil,
        onDone: ((TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil,
        onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)? = nil,
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

    public func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(payload: rpcMessage, allocator: allocator)
        return message.packets
    }

    public func log(to logger: Logger) {
        logger.debug("Sending RPC request: \(rpcMessage.procedureName)")
    }
}
