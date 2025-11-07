import NIOCore

extension TDSTokenParser {
    public func parseDoneToken() throws -> TDSTokens.DoneToken? {
        let start = streamParser.position

        guard let tokenByte = streamParser.readUInt8() else {
            return nil
        }

        guard let tokenType = TDSTokens.TokenType(rawValue: tokenByte),
              tokenType == .done || tokenType == .doneInProc || tokenType == .doneProc
        else {
            streamParser.position = start
            return nil
        }

        guard let status = streamParser.readUInt16LE(),
              let curCmd = streamParser.readUInt16LE(),
              let doneRowCount = streamParser.readUInt64LE()
        else {
            streamParser.position = start
            throw TDSError.needMoreData
        }

        var token = TDSTokens.DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
        token.type = tokenType
        return token
    }
}
