public class TDSTokenParser {
    private var buffer: ByteBuffer
    private var colMetadata: TDSTokens.ColMetadataToken?
    private var logger: Logger
    
    init(logger: Logger) {
        self.logger = logger
        self.buffer = ByteBufferAllocator().buffer(capacity: 0)
    }
    
    func writeAndParseTokens(_ inputBuffer: ByteBuffer) -> [TDSToken] {
        var packetMessageBuffer = inputBuffer
        buffer.writeBuffer(&packetMessageBuffer)
        return parseTokens()
    }
    
    func parseTokens() -> [TDSToken] {
        var bufferCopy = buffer
        var parsedTokens: [TDSToken] = []
        while buffer.readableBytes > 0 {
            do {
                var token: TDSToken
                guard
                    let tokenByte = buffer.readByte(),
                    let tokenType = TDSTokens.TokenType(rawValue: tokenByte)
                else {
                    throw TDSError.protocolError("Parsed unknown token type.")
                }
                
                switch tokenType {
                case .error, .info:
                    token = try TDSTokenParser.parseErrorInfoToken(type: tokenType, from: &buffer)
                case .loginAck:
                    token = try TDSTokenParser.parseLoginAckToken(from: &buffer)
                case .envchange:
                    token = try TDSTokenParser.parseEnvChangeToken(from: &buffer)
                case .done, .doneInProc, .doneProc :
                    token = try TDSTokenParser.parseDoneToken(from: &buffer)
                case .colMetadata:
                    let colMetadataToken = try TDSTokenParser.parseColMetadataToken(from: &buffer)
                    colMetadata = colMetadataToken
                    token = colMetadataToken
                case .row:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseRowToken(from: &buffer, with: colMetadata)
                case .sspi:
                    token = try TDSTokenParser.parseSSPIToken(from: &buffer)
                default:
                    throw TDSError.protocolError("Parsing implementation incomplete")
                }

                parsedTokens.append(token)
                
            } catch {
                buffer = bufferCopy
                return parsedTokens
            }
            
            bufferCopy = buffer
        }
        
        return parsedTokens
    }
}

extension TDSTokenParser {
    static func parseSSPIToken(from buffer: inout ByteBuffer) throws -> TDSTokens.SSPIToken {
        guard let length = buffer.readInteger(as: UInt16.self) else {
            throw TDSError.protocolError("Invalid SSPI token length")
        }
        guard let slice = buffer.readSlice(length: Int(length)) else {
            throw TDSError.protocolError("Incomplete SSPI token")
        }
        return TDSTokens.SSPIToken(payload: slice)
    }
}
