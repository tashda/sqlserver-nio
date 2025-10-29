import NIO

extension TDSMessage {

    static public func serializeAllHeaders(_ buffer: inout ByteBuffer, transactionDescriptor: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0], outstandingRequestCount: UInt32 = 1) {
        // Match Microsoft JDBC framing observed on wire:
        // TotalLength (DWORD) = 4 (MarsHeaderLen field) + 18 (MARS header bytes)
        // MarsHeaderLen (DWORD) = 18
        // HeaderType (USHORT) = 0x0002
        // TransactionDescriptor (8 bytes)
        // OutstandingRequestCount (DWORD)
        let marsHeaderLen: DWord = 18
        let totalLen: DWord = 4 + marsHeaderLen
        buffer.writeInteger(totalLen, endianness: .little)
        buffer.writeInteger(marsHeaderLen, endianness: .little)
        buffer.writeInteger(0x0002 as UShort, endianness: .little)
        buffer.writeBytes(transactionDescriptor)
        buffer.writeInteger(outstandingRequestCount as DWord, endianness: .little)
        return
    }
}
