import Logging
import NIO
import Foundation

extension TDSMessages {
    /// `SQLBatch`
    /// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/f2026cd3-9a46-4a3f-9a08-f63140bcbbe3
    public struct RawSqlBatchMessage: TDSMessagePayload {
        public static let packetType: TDSPacket.HeaderType = .sqlBatch

        public var sqlText: String
        public var transactionDescriptor: [UInt8]
        public var outstandingRequestCount: UInt32
        
        public init(sqlText: String, transactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0], outstandingRequestCount: UInt32 = 1) { 
            self.sqlText = sqlText
            self.transactionDescriptor = transactionDescriptor
            self.outstandingRequestCount = outstandingRequestCount
        }

        public func serialize(into buffer: inout ByteBuffer) throws {
            TDSMessage.serializeAllHeaders(&buffer, transactionDescriptor: transactionDescriptor, outstandingRequestCount: outstandingRequestCount)
            buffer.writeUTF16String(sqlText)
            return
        }
    }
}
