import Foundation
import NIOCore
import Logging

public final class TransactionManagerRequest: TDSRequest, @unchecked Sendable {
    public enum Command: Sendable {
        case begin(isolationLevel: UInt8 = 0x00)
        case commit
        case rollback
    }

    private let message: TDSMessages.TransactionManagerRequestMessage
    private let command: Command

    public let onRow: (@Sendable (TDSRow) -> Void)? = nil
    public let onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)? = nil
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil

    public var packetType: TDSPacket.HeaderType { .transactionManagerRequest }

    public init(
        command: Command,
        transactionDescriptor: [UInt8],
        outstandingRequestCount: UInt32 = 1
    ) {
        self.command = command

        switch command {
        case .begin(let isolationLevel):
            self.message = .init(
                requestType: .begin,
                payload: [isolationLevel, 0],
                transactionDescriptor: transactionDescriptor,
                outstandingRequestCount: outstandingRequestCount
            )
        case .commit:
            self.message = .init(
                requestType: .commit,
                payload: [0, 0],
                transactionDescriptor: transactionDescriptor,
                outstandingRequestCount: outstandingRequestCount
            )
        case .rollback:
            self.message = .init(
                requestType: .rollback,
                payload: [0, 0],
                transactionDescriptor: transactionDescriptor,
                outstandingRequestCount: outstandingRequestCount
            )
        }
    }

    public func log(to logger: Logger) {
        switch command {
        case .begin:
            logger.debug("Sending TM_BEGIN_XACT request")
        case .commit:
            logger.debug("Sending TM_COMMIT_XACT request")
        case .rollback:
            logger.debug("Sending TM_ROLLBACK_XACT request")
        }
    }

    public func serialize(into buffer: inout ByteBuffer) throws {
        try message.serialize(into: &buffer)
    }
}
