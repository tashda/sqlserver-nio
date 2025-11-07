extension TDSTokenParser {
    public static func parseErrorInfoToken(type: TDSTokens.TokenType, from buffer: inout ByteBuffer) throws -> TDSTokens.ErrorInfoToken {
        guard buffer.readUShort() != nil else {
            throw TDSError.needMoreData
        }
        guard
            let number = buffer.readLong(),
            let state = buffer.readByte(),
            let classValue = buffer.readByte(),
            let msgText = buffer.readUSVarchar(),
            let serverName = buffer.readBVarchar(),
            let procName = buffer.readBVarchar(),
            let lineNumber = buffer.readLong()
        else {
            throw TDSError.needMoreData
        }

        let token = TDSTokens.ErrorInfoToken(
            type: type,
            number: Int(number),
            state: state,
            classValue: classValue,
            messageText: msgText,
            serverName: serverName,
            procedureName: procName,
            lineNumber: Int(lineNumber)
        )

        return token
    }
}
