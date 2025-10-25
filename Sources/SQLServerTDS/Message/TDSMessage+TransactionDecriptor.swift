import NIO

extension TDSMessage {

    static public func serializeAllHeaders(_ buffer: inout ByteBuffer, transactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0], outstandingRequestCount: UInt32 = 1) {
        let startWriterIndex = buffer.writerIndex
        // skip TotalLength for now
        buffer.moveWriterIndex(forwardBy: 4)

        // MARS/Transaction Header (exactly like Microsoft JDBC driver)
        buffer.writeInteger(18 as DWord, endianness: .little) // MARS header length (HeaderLength + HeaderType + TransactionDescriptor + OutstandingRequestCount)
        buffer.writeInteger(0x02 as UShort, endianness: .little) // HeaderType (MARS header)
        buffer.writeBytes(transactionDescriptor) // TransactionDescriptor (8 bytes, raw bytes like Microsoft)
        buffer.writeInteger(outstandingRequestCount as DWord, endianness: .little) // OutstandingRequestCount

        // Set the total length of all headers
        buffer.setInteger(DWord(buffer.writerIndex - startWriterIndex), at: startWriterIndex, endianness: .little)
        return
    }
}
