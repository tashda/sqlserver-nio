import Foundation

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
                guard let tokenByte = buffer.readByte() else {
                    throw TDSError.needMoreData
                }

                guard let tokenType = TDSTokens.TokenType(rawValue: tokenByte) else {
                    logger.warning("Encountered unsupported token byte: 0x\(String(tokenByte, radix: 16, uppercase: false))")
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
                case .colInfo:
                    token = try TDSTokenParser.parseColInfoToken(from: &buffer)
                case .tabName:
                    token = try TDSTokenParser.parseTabNameToken(from: &buffer)
                case .returnStatus:
                    token = try TDSTokenParser.parseReturnStatusToken(from: &buffer)
                case .row:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseRowToken(from: &buffer, with: colMetadata)
                case .nbcRow:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing NBC row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseNbcRowToken(from: &buffer, with: colMetadata)
                case .order:
                    token = try TDSTokenParser.parseOrderToken(from: &buffer)
                case .sspi:
                    token = try TDSTokenParser.parseSSPIToken(from: &buffer)
                default:
                    logger.warning("Unhandled token type: \(tokenType)")
                    throw TDSError.protocolError("Parsing implementation incomplete")
                }

                parsedTokens.append(token)
                
            } catch TDSError.needMoreData {
                buffer = bufferCopy
                return parsedTokens
            } catch {
                if let nextByte = bufferCopy.getInteger(at: bufferCopy.readerIndex, as: UInt8.self) {
                    logger.error("Token parser error: \(error) next byte: 0x\(String(nextByte, radix: 16)) remaining: \(bufferCopy.readableBytes)")
                } else {
                    logger.error("Token parser error: \(error) (no additional bytes)")
                }
                buffer = bufferCopy
                return parsedTokens
            }
            
            bufferCopy = buffer
        }
        
        return parsedTokens
    }
}

extension TDSTokenParser {
    static func parseReturnStatusToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ReturnStatusToken {
        guard buffer.readableBytes >= MemoryLayout<Int32>.size else {
            throw TDSError.needMoreData
        }
        guard let value = buffer.readInteger(endianness: .little, as: Int32.self) else {
            throw TDSError.protocolError("RETURNSTATUS token missing value.")
        }
        return TDSTokens.ReturnStatusToken(value: value)
    }

    static func parseTabNameToken(from buffer: inout ByteBuffer) throws -> TDSTokens.TabNameToken {
        guard buffer.readableBytes >= MemoryLayout<UInt16>.size else {
            throw TDSError.needMoreData
        }
        guard let length = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("TABNAME token missing length field.")
        }
        guard buffer.readableBytes >= Int(length) else {
            throw TDSError.needMoreData
        }
        guard let bytes = buffer.readBytes(length: Int(length)) else {
            throw TDSError.protocolError("TABNAME token truncated.")
        }
        return TDSTokens.TabNameToken(data: bytes)
    }

    static func parseColInfoToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ColInfoToken {
        guard buffer.readableBytes >= MemoryLayout<UInt16>.size else {
            throw TDSError.needMoreData
        }
        guard let length = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("COLINFO token missing length field.")
        }
        guard buffer.readableBytes >= Int(length) else {
            throw TDSError.needMoreData
        }
        guard let bytes = buffer.readBytes(length: Int(length)) else {
            throw TDSError.protocolError("COLINFO token truncated.")
        }
        return TDSTokens.ColInfoToken(data: bytes)
    }

    static func parseSSPIToken(from buffer: inout ByteBuffer) throws -> TDSTokens.SSPIToken {
        guard let length = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("Invalid SSPI token length")
        }
        guard let slice = buffer.readSlice(length: Int(length)) else {
            throw TDSError.protocolError("Incomplete SSPI token")
        }
        return TDSTokens.SSPIToken(payload: slice)
    }

    static func parseOrderToken(from buffer: inout ByteBuffer) throws -> TDSTokens.OrderToken {
        let lengthFieldSize = MemoryLayout<UInt16>.size
        guard buffer.readableBytes >= lengthFieldSize else {
            throw TDSError.needMoreData
        }
        guard let length = buffer.getInteger(at: buffer.readerIndex, endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("ORDER token missing length field.")
        }
        let byteCount = Int(length)
        let totalRequired = lengthFieldSize + byteCount
        guard buffer.readableBytes >= totalRequired else {
            throw TDSError.needMoreData
        }
        buffer.moveReaderIndex(forwardBy: lengthFieldSize)
        guard let data = buffer.readSlice(length: byteCount) else {
            throw TDSError.protocolError("ORDER token truncated.")
        }
        var ordinals: [UInt16] = []
        ordinals.reserveCapacity(byteCount / 2)
        var copy = data
        while copy.readableBytes >= 2, let ordinal = copy.readInteger(endianness: .little, as: UInt16.self) {
            ordinals.append(ordinal)
        }
        return TDSTokens.OrderToken(columnOrdinals: ordinals)
    }
}
