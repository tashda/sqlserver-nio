import Foundation
import NIO

extension TDSMessages {
    public struct TransactionManagerRequestMessage: TDSMessagePayload {
        public static let packetType: TDSPacket.HeaderType = .transactionManagerRequest

        public enum RequestType: UInt16, Sendable {
            case begin = 5
            case commit = 7
            case rollback = 8
        }

        public let requestType: RequestType
        public let payload: [UInt8]
        public let transactionDescriptor: [UInt8]
        public let outstandingRequestCount: UInt32

        public init(
            requestType: RequestType,
            payload: [UInt8],
            transactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0],
            outstandingRequestCount: UInt32 = 1
        ) {
            self.requestType = requestType
            self.payload = payload
            self.transactionDescriptor = transactionDescriptor
            self.outstandingRequestCount = outstandingRequestCount
        }

        public func serialize(into buffer: inout ByteBuffer) throws {
            SQLServerTDS.TDSMessage.serializeAllHeaders(
                &buffer,
                transactionDescriptor: transactionDescriptor,
                outstandingRequestCount: outstandingRequestCount
            )
            buffer.writeInteger(requestType.rawValue, endianness: .little)
            buffer.writeBytes(payload)
        }
    }
}
