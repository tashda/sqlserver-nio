import NIOCore

extension TDSTokenOperations {
    internal static func parseLoginAckToken(from buffer: inout ByteBuffer) throws -> TDSTokens.LoginAckToken {
        guard let length: UInt16 = buffer.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }
        let start = buffer.readerIndex

        guard let interface: UInt8 = buffer.readInteger(),
              let tdsVersion: UInt32 = buffer.readInteger(endianness: .big),
              let progNameLen: UInt8 = buffer.readInteger(),
              let progName = buffer.readUTF16String(length: Int(progNameLen) * 2),
              let majorVer: UInt8 = buffer.readInteger(),
              let minorVer: UInt8 = buffer.readInteger(),
              let buildNumHi: UInt8 = buffer.readInteger(),
              let buildNumLow: UInt8 = buffer.readInteger() else {
            throw TDSError.needMoreData
        }

        buffer.moveReaderIndex(to: start + Int(length))

        // Reconstruct version as a single UInt32
        let version = (UInt32(majorVer) << 24) | (UInt32(minorVer) << 16) | (UInt32(buildNumHi) << 8) | UInt32(buildNumLow)

        return TDSTokens.LoginAckToken(
            interface: interface,
            tdsVersion: tdsVersion,
            progName: progName,
            version: version
        )
    }
}
