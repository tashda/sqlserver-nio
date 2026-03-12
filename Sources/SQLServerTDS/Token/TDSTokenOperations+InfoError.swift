import NIOCore

extension TDSTokenOperations {
    internal static func parseErrorInfoToken(type: TDSTokens.TokenType, from buffer: inout ByteBuffer) throws -> TDSTokens.ErrorInfoToken {
        guard let length: UInt16 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
        let start = buffer.readerIndex
        
        guard let number: Int32 = buffer.readInteger(endianness: .little),
              let state: UInt8 = buffer.readInteger(),
              let classValue: UInt8 = buffer.readInteger(),
              let msgLen: UInt16 = buffer.readInteger(endianness: .little),
              let messageText = buffer.readUTF16String(length: Int(msgLen) * 2),
              let serverNameLen: UInt8 = buffer.readInteger(),
              let serverName = buffer.readUTF16String(length: Int(serverNameLen) * 2),
              let procNameLen: UInt8 = buffer.readInteger(),
              let procName = buffer.readUTF16String(length: Int(procNameLen) * 2),
              let lineNumber: Int32 = buffer.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }

        buffer.moveReaderIndex(to: start + Int(length))

        return TDSTokens.ErrorInfoToken(
            type: type,
            number: number,
            state: state,
            classValue: classValue,
            messageText: messageText,
            serverName: serverName,
            procName: procName,
            lineNumber: lineNumber
        )
    }
}
