import Foundation
import NIOCore
import Logging

public typealias TDSColumnMetadata = TDSTokens.ColMetadataToken.ColumnData

public class RawSqlRequest: TDSRequest, @unchecked Sendable {
    public let sql: String
    public let onRow: (@Sendable (TDSRow) -> Void)?
    public let onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)?
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)?
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)?
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)?
    public let resultPromise: EventLoopPromise<[TDSData]>?
    public let stream: Bool
    public let onData: (@Sendable (TDSData) -> Void)?
    public var storesRowsInContext: Bool { resultPromise != nil }

    /// Optional overrides for the MARS Transaction Descriptor header.
    /// When nil, the request will be sent with an AutoCommit descriptor (all zeros, requestCount = 1).
    /// When set by the connection pipeline, these values are propagated into the ALL_HEADERS block.
    public var transactionDescriptorOverride: [UInt8]?
    public var outstandingRequestCountOverride: UInt32?

    public var packetType: TDSPacket.HeaderType { .sqlBatch }

    public init(
        sql: String,
        onRow: (@Sendable (TDSRow) -> Void)? = nil,
        onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)? = nil,
        onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil,
        onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil,
        resultPromise: EventLoopPromise<[TDSData]>? = nil,
        stream: Bool = false,
        onData: (@Sendable (TDSData) -> Void)? = nil
    ) {
        self.sql = sql
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onDone = onDone
        self.onMessage = onMessage
        self.onReturnValue = onReturnValue
        self.resultPromise = resultPromise
        self.stream = stream
        self.onData = onData
    }

    public func log(to logger: Logger) {
        logger.debug("Sending SQL Batch request: \(Self.summarize(sql))")
    }

    public func serialize(into buffer: inout ByteBuffer) throws {
        let payload = TDSMessages.RawSqlBatchMessage(
            sqlText: sql,
            transactionDescriptor: transactionDescriptorOverride ?? [0, 0, 0, 0, 0, 0, 0, 0],
            outstandingRequestCount: outstandingRequestCountOverride ?? 1
        )
        try payload.serialize(into: &buffer)
    }

    private static func summarize(_ sql: String, maxLength: Int = 240) -> String {
        let singleLine = sql.replacingOccurrences(of: "\n", with: " ").replacingOccurrences(of: "\r", with: " ")
        guard singleLine.count > maxLength else { return singleLine }
        let prefix = singleLine.prefix(maxLength)
        return "\(prefix)... [truncated \(singleLine.count - maxLength) chars]"
    }
}
