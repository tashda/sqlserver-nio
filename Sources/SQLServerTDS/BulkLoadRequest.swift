import Foundation
import NIOCore
import Logging

/// The data half of a bulk load (MS-TDS 2.2.1.6): after an `INSERT BULK` batch, a BulkLoadBCP message
/// (packet type 0x07) carrying COLMETADATA, one ROW token per row and a DONE token. The server answers
/// with DONE (rows copied) or errors. Build the payload with ``TDSBulkLoadWriter``.
public final class BulkLoadRequest: TDSRequest, @unchecked Sendable {
    private let payload: ByteBuffer
    private let rowCount: Int

    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)?
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)?
    public var onRow: (@Sendable (TDSRow) -> Void)? { nil }
    public var onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)? { nil }
    public var onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? { nil }

    public var packetType: TDSPacket.HeaderType { .bulkLoadData }

    public init(
        payload: ByteBuffer,
        rowCount: Int,
        onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    ) {
        self.payload = payload
        self.rowCount = rowCount
        self.onDone = onDone
        self.onMessage = onMessage
    }

    public func log(to logger: Logger) {
        logger.debug("Sending bulk load data: \(rowCount) rows, \(payload.readableBytes) bytes")
    }

    public func serialize(into buffer: inout ByteBuffer) throws {
        buffer.writeImmutableBuffer(payload)
    }
}
