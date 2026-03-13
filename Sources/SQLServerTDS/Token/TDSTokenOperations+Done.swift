import NIOCore

extension TDSTokenOperations {
    internal func parseDoneToken() throws -> TDSTokens.DoneToken? {
        let start = streamParser.position
        var buffer = streamParser.buffer
        buffer.moveReaderIndex(to: streamParser.position)

        do {
            guard let rawType: UInt8 = buffer.readInteger(),
                  let type = TDSTokens.TokenType(rawValue: rawType),
                  type == .done || type == .doneInProc || type == .doneProc else {
                return nil
            }
            var token = try TDSTokens.DoneToken.parse(from: &buffer)
            token.type = type
            streamParser.position = buffer.readerIndex
            return token
        } catch TDSError.needMoreData {
            streamParser.position = start
            throw TDSError.needMoreData
        }
    }

    internal func parseOrderToken() throws -> TDSTokens.OrderToken? {
        var buffer = streamParser.buffer
        buffer.moveReaderIndex(to: streamParser.position)
        
        do {
            guard let rawType: UInt8 = buffer.readInteger(),
                  TDSTokens.TokenType(rawValue: rawType) == .order else {
                return nil
            }
            let token = try TDSTokens.OrderToken.parse(from: &buffer)
            streamParser.position = buffer.readerIndex
            return token
        } catch TDSError.needMoreData {
            return nil
        }
    }
}
