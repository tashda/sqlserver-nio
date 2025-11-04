import NIOCore

extension TDSTokenParser {
    public func parseDoneToken() throws -> TDSTokens.DoneToken? {
        guard let tokenType = streamParser.readUInt8(), tokenType == TDSTokens.TokenType.done.rawValue else {
            // This is not a DONE token, so we should reset the position and return nil
            streamParser.position -= 1
            return nil
        }

        guard let status = streamParser.readUInt16LE() else {
            throw TDSError.needMoreData
        }

        guard let curCmd = streamParser.readUInt16LE() else {
            throw TDSError.needMoreData
        }

        guard let doneRowCount = streamParser.readUInt64LE() else {
            throw TDSError.needMoreData
        }

        return TDSTokens.DoneToken(status: status, curCmd: curCmd, doneRowCount: doneRowCount)
    }
}