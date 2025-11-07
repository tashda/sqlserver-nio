
import NIOCore
import Logging

public class LoginRequest: TDSRequest {
    private let payload: TDSMessages.Login7Message

    public let onRow: ((TDSRow) -> Void)? = nil
    public let onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? = nil
    public let onDone: ((TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    public let onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)? = nil
    public let stream: Bool = false // Login requests are always non-streaming
    public let onData: ((TDSData) -> Void)? = nil // Login requests don't use onData

    public init(payload: TDSMessages.Login7Message) {
        self.payload = payload
    }

    public func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(payload: payload, allocator: allocator)
        return message.packets
    }

    public func log(to logger: Logger) {
        logger.debug("Logging in as user: \(payload.username) to database: \(payload.database) and server: \(payload.serverName)")
    }
}
