
import NIOCore
import Logging

public class RawSqlRequest: TDSRequest {
    public let sql: String
    public let onRow: ((TDSRow) -> Void)?
    public let onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)?
    public let onDone: ((TDSTokens.DoneToken) -> Void)?
    public let onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)?
    public let onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)?
    public let resultPromise: EventLoopPromise<[TDSData]>?
    public let stream: Bool
    public let onData: ((TDSData) -> Void)?

    public init(
        sql: String,
        stream: Bool = false,
        onRow: ((TDSRow) -> Void)? = nil,
        onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? = nil,
        onData: ((TDSData) -> Void)? = nil,
        onDone: ((TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil,
        onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)? = nil,
        resultPromise: EventLoopPromise<[TDSData]>? = nil
    ) {
        self.sql = sql
        self.stream = stream
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onDone = onDone
        self.onMessage = onMessage
        self.onReturnValue = onReturnValue
        self.onData = onData
        self.resultPromise = resultPromise
    }

    public func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = TDSMessages.RawSqlBatchMessage(sqlText: sql)
        let tdsMessage = try TDSMessage(payload: message, allocator: allocator)
        return tdsMessage.packets
    }

    public func log(to logger: Logger) {
        logger.debug("Sending raw SQL request: \(sql)")
    }
}
