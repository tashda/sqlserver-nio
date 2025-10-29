extension TDSTokenParser {
    public static func parseDoneToken(from buffer: inout ByteBuffer) throws -> TDSTokens.DoneToken {
        // DONE token layout: STATUS (2) + CURCMD (2) + ROWCOUNT (8)
        let requiredBytes = 2 + 2 + 8
        guard buffer.readableBytes >= requiredBytes else {
            // Not enough bytes to parse the DONE token; ask for more data.
            throw TDSError.needMoreData
        }
        guard let status = buffer.readUShort() else {
            throw TDSError.protocolError("Invalid DONE token: missing status")
        }
        guard let curCmd = buffer.readUShort() else {
            throw TDSError.protocolError("Invalid DONE token: missing curCmd")
        }
        guard let doneRowCount = buffer.readULongLong() else {
            throw TDSError.protocolError("Invalid DONE token: missing rowcount")
        }

        let token = TDSTokens.DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
        return token
    }
}
