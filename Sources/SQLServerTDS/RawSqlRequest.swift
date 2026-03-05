
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

    /// Optional overrides for the MARS Transaction Descriptor header.
    /// When nil, the request will be sent with an AutoCommit descriptor (all zeros, requestCount = 1).
    /// When set by the connection pipeline, these values are propagated into the ALL_HEADERS block.
    public var transactionDescriptorOverride: [UInt8]?
    public var outstandingRequestCountOverride: UInt32?

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
        // Resolve the transaction descriptor for this batch. If the pipeline has injected
        // an explicit descriptor (for an active transaction), use it; otherwise default
        // to AutoCommit semantics (descriptor = 0, requestCount = 1) per MS‑TDS.
        let descriptor = transactionDescriptorOverride ?? [UInt8](repeating: 0, count: 8)
        let requestCount = outstandingRequestCountOverride ?? 1
        let message = TDSMessages.RawSqlBatchMessage(
            sqlText: sql,
            transactionDescriptor: descriptor,
            outstandingRequestCount: requestCount
        )
        let tdsMessage = try TDSMessage(payload: message, allocator: allocator)
        return tdsMessage.packets
    }

    public func log(to logger: Logger) {
        logger.debug("Sending raw SQL request: \(sql)")
    }
}
