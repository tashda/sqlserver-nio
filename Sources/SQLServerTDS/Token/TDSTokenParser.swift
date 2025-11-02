import Foundation
import Logging

public class TDSTokenParser {
    private var buffer: ByteBuffer
    private var colMetadata: TDSTokens.ColMetadataToken?
    private var logger: Logger
    // Internal for diagnostics in row parser
    weak var ring: TDSTokenRing?
    
    init(logger: Logger, ring: TDSTokenRing? = nil) {
        self.logger = logger
        self.buffer = ByteBufferAllocator().buffer(capacity: 0)
        self.ring = ring
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

                if tokenType == .done || tokenType == .doneInProc || tokenType == .doneProc {
                    let required = 2 + 2 + 8
                    if buffer.readableBytes >= required,
                       let status: UInt16 = buffer.getInteger(at: buffer.readerIndex, endianness: .little) {
                        ring?.record("token=\(tokenType) status=0x\(String(status, radix: 16)) remaining=\(buffer.readableBytes)")
                    } else {
                        ring?.record("token=\(tokenType) remaining=\(buffer.readableBytes)")
                    }
                } else {
                    ring?.record("token=\(tokenType) remaining=\(buffer.readableBytes)")
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
                case .columnStatus:
                    token = try TDSTokenParser.parseColumnStatusToken(from: &buffer)
                case .unknown0x74:
                    token = try TDSTokenParser.parseUnknown0x74Token(from: &buffer)
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
                    token = try TDSTokenParser.parseRowToken(from: &buffer, with: colMetadata, ring: ring)
                case .nbcRow:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing NBC row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseNbcRowToken(from: &buffer, with: colMetadata, ring: ring)
                case .order:
                    token = try TDSTokenParser.parseOrderToken(from: &buffer)
                case .sspi:
                    token = try TDSTokenParser.parseSSPIToken(from: &buffer)
                case .offset:
                    token = try TDSTokenParser.parseOffsetToken(from: &buffer)
                case .tvpRow:
                    guard let colMetadata = colMetadata else {
                        throw TDSError.protocolError("Error while parsing TVP row data: no COLMETADATA recieved")
                    }
                    token = try TDSTokenParser.parseTVPRowToken(from: &buffer, with: colMetadata, ring: ring)
                case .featureExtAck:
                    token = try TDSTokenParser.parseFeatureExtAckToken(from: &buffer)
                case .fedAuthInfo:
                    token = try TDSTokenParser.parseFedAuthInfoToken(from: &buffer, ring: ring)
                case .sessionState:
                    token = try TDSTokenParser.parseSessionStateToken(from: &buffer)
                case .dataClassification:
                    token = try TDSTokenParser.parseDataClassificationToken(from: &buffer)
                case .returnValue:
                    token = try TDSTokenParser.parseReturnValueToken(from: &buffer)
                default:
                    logger.warning("Unhandled token type: \(tokenType)")
                    throw TDSError.protocolError("Parsing implementation incomplete")
                }

                parsedTokens.append(token)
                
            } catch TDSError.needMoreData {
                // Record that we yielded due to insufficient bytes; this helps
                // catch boundary issues where we may be waiting for bytes that
                // won't actually arrive (diagnostics only).
                ring?.record("need_more_data remaining=\(buffer.readableBytes)")
                buffer = bufferCopy
                return parsedTokens
            } catch {
                if let nextByte = bufferCopy.getInteger(at: bufferCopy.readerIndex, as: UInt8.self) {
                    logger.error("Token parser error: \(error) next byte: 0x\(String(nextByte, radix: 16)) remaining: \(bufferCopy.readableBytes)")
                    ring?.record("parser_error next=0x\(String(nextByte, radix: 16)) remaining=\(bufferCopy.readableBytes)")
                } else {
                    logger.error("Token parser error: \(error) (no additional bytes)")
                    ring?.record("parser_error (no additional bytes)")
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

    static func parseOffsetToken(from buffer: inout ByteBuffer) throws -> TDSTokens.OffsetToken {
        let lengthFieldSize = MemoryLayout<UInt16>.size
        guard buffer.readableBytes >= lengthFieldSize else { throw TDSError.needMoreData }
        guard let length = buffer.getInteger(at: buffer.readerIndex, endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("OFFSET token missing length field.")
        }
        let totalRequired = lengthFieldSize + Int(length)
        guard buffer.readableBytes >= totalRequired else { throw TDSError.needMoreData }
        buffer.moveReaderIndex(forwardBy: lengthFieldSize)
        guard let data = buffer.readBytes(length: Int(length)) else { throw TDSError.protocolError("OFFSET token truncated.") }
        return TDSTokens.OffsetToken(data: data)
    }

    static func parseColumnStatusToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ColumnStatusToken {
        // ColumnStatus token structure: 2-byte length + 2-byte status + variable length data
        let lengthFieldSize = MemoryLayout<UInt16>.size
        guard buffer.readableBytes >= lengthFieldSize else {
            throw TDSError.needMoreData
        }

        guard let length = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("COLUMNSTATUS token missing length field.")
        }

        let statusSize = MemoryLayout<UInt16>.size
        let totalRequired = Int(length) + lengthFieldSize
        guard buffer.readableBytes >= totalRequired else {
            throw TDSError.needMoreData
        }

        guard let status = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("COLUMNSTATUS token missing status field.")
        }

        let dataLength = Int(length) - statusSize
        guard dataLength >= 0 else {
            throw TDSError.protocolError("COLUMNSTATUS token invalid length.")
        }

        var data: [Byte] = []
        if dataLength > 0 {
            guard let dataBytes = buffer.readBytes(length: dataLength) else {
                throw TDSError.protocolError("COLUMNSTATUS token truncated data.")
            }
            data = dataBytes
        }

        return TDSTokens.ColumnStatusToken(status: status, data: data)
    }

    static func parseUnknown0x74Token(from buffer: inout ByteBuffer) throws -> TDSTokens.Unknown0x74Token {
        // This token appears to be an undocumented Microsoft TDS token
        // Handle it as a length-prefixed payload
        guard let length = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("UNKNOWN_0x74 token missing length field")
        }

        guard buffer.readableBytes >= Int(length) else {
            throw TDSError.needMoreData
        }

        guard let payload = buffer.readSlice(length: Int(length)) else {
            throw TDSError.protocolError("UNKNOWN_0x74 token truncated")
        }

        // Simple diagnostic - just log when we encounter this token
        print("🎯 Encountered TDS Token 0x74: length=\(length), payload=\(payload.readableBytes) bytes")

        return TDSTokens.Unknown0x74Token(payload: payload)
    }
}

// MARK: - Additional token parsers and helpers
extension TDSTokenParser {
    static func parseLengthPrefixedPayload(from buffer: inout ByteBuffer) throws -> ByteBuffer {
        guard let length = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.protocolError("Invalid token length header")
        }
        guard let slice = buffer.readSlice(length: Int(length)) else {
            throw TDSError.needMoreData
        }
        return slice
    }

    static func parseFeatureExtAckToken(from buffer: inout ByteBuffer) throws -> TDSTokens.FeatureExtAckToken {
        let payload = try parseLengthPrefixedPayload(from: &buffer)
        return TDSTokens.FeatureExtAckToken(payload: payload)
    }

    static func parseFedAuthInfoToken(from buffer: inout ByteBuffer, ring: TDSTokenRing? = nil) throws -> TDSTokens.FedAuthInfoToken {
        // FedAuthInfo length field ambiguity: some servers report the payload length (excluding the 2‑byte
        // field), others report a total length including it. Be defensive and accept either. As a last
        // resort, consume whatever bytes are available to prevent stalls since the payload is opaque.
        let avail0 = buffer.readableBytes
        guard avail0 >= 2 else { throw TDSError.needMoreData }
        guard let lenVal: UInt16 = buffer.getInteger(at: buffer.readerIndex, endianness: .little) else {
            throw TDSError.protocolError("FEDAUTHINFO missing length field")
        }
        let len = Int(lenVal)
        let requiredExcluding = 2 + len
        let requiredIncluding = len
        ring?.record("fedAuth: avail=\(avail0) len=\(len) reqExcl=\(requiredExcluding) reqIncl=\(requiredIncluding)")

        if avail0 >= requiredExcluding {
            // Standard case: length excludes the 2‑byte field
            buffer.moveReaderIndex(forwardBy: 2)
            guard let payload = buffer.readSlice(length: len) else {
                throw TDSError.protocolError("FEDAUTHINFO truncated payload (excl mode)")
            }
            return TDSTokens.FedAuthInfoToken(payload: payload)
        }
        if len >= 2 && avail0 >= requiredIncluding {
            // Inclusive case: length includes the 2‑byte field
            buffer.moveReaderIndex(forwardBy: 2)
            let payloadLen = len - 2
            guard let payload = buffer.readSlice(length: payloadLen) else {
                throw TDSError.protocolError("FEDAUTHINFO truncated payload (incl mode)")
            }
            return TDSTokens.FedAuthInfoToken(payload: payload)
        }

        // Fallback: consume what we have (opaque payload) to avoid stalling parser
        // This is safe because FedAuthInfo appears at the end of a message and isn't used by SQL password auth.
        if avail0 >= 2 {
            buffer.moveReaderIndex(forwardBy: 2)
            let partial = min(avail0 - 2, max(0, len))
            if partial > 0, let payload = buffer.readSlice(length: partial) {
                ring?.record("fedAuth: fallback consume partial=\(partial)")
                return TDSTokens.FedAuthInfoToken(payload: payload)
            } else {
                ring?.record("fedAuth: fallback empty payload")
                return TDSTokens.FedAuthInfoToken(payload: ByteBuffer())
            }
        }
        throw TDSError.needMoreData
    }

    static func parseSessionStateToken(from buffer: inout ByteBuffer) throws -> TDSTokens.SessionStateToken {
        let payload = try parseLengthPrefixedPayload(from: &buffer)
        return TDSTokens.SessionStateToken(payload: payload)
    }

    static func parseDataClassificationToken(from buffer: inout ByteBuffer) throws -> TDSTokens.DataClassificationToken {
        let payload = try parseLengthPrefixedPayload(from: &buffer)
        return TDSTokens.DataClassificationToken(payload: payload)
    }

    static func parseTypeInfo(from buffer: inout ByteBuffer) throws -> TypeMetadata {
        guard let dataTypeVal = buffer.readByte(), let dataType = TDSDataType(rawValue: dataTypeVal) else {
            throw TDSError.protocolError("Invalid TYPE_INFO")
        }

        // consume type-specific length information to keep stream aligned
        var collation: [Byte] = []
        var precision: Int?
        var scale: Int?

        switch dataType {
        case .sqlVariant, .nText, .text, .image:
            // sql_variant and LOB-like: 4-byte LONGLEN in TYPE_INFO
            guard buffer.readLongLen() != nil else { throw TDSError.protocolError("TYPE_INFO length") }
        case .vector:
            // USHORTLEN + 1 byte dimension type
            guard buffer.readUShortLen() != nil else { throw TDSError.protocolError("TYPE_INFO length") }
            _ = buffer.readByte()
        case .json:
            // No extra TYPE_INFO payload
            break
        case .xml:
            // XML header: schemaPresent + optional DB/Schema (B_USVAR) + collection (US_VARCHAR)
            if let present = buffer.readByte(), present != 0 {
                if let dbChars = buffer.readByte() { _ = buffer.readBytes(length: Int(dbChars) * 2) }
                if let schemaChars = buffer.readByte() { _ = buffer.readBytes(length: Int(schemaChars) * 2) }
                if let collChars = buffer.readUShort() { _ = buffer.readBytes(length: Int(collChars) * 2) }
            }
        case .clrUdt:
            // UDT header: USHORT maxLen + DB/Schema/Type (B_USVAR) + assembly (US_VARCHAR)
            _ = buffer.readUShort()
            if let dbChars = buffer.readByte() { _ = buffer.readBytes(length: Int(dbChars) * 2) }
            if let schemaChars = buffer.readByte() { _ = buffer.readBytes(length: Int(schemaChars) * 2) }
            if let typeChars = buffer.readByte() { _ = buffer.readBytes(length: Int(typeChars) * 2) }
            if let asmChars = buffer.readUShort() { _ = buffer.readBytes(length: Int(asmChars) * 2) }
        case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
            guard buffer.readUShortLen() != nil else { throw TDSError.protocolError("TYPE_INFO length") }
        case .date:
            _ = 0 // fixed 3 bytes when reading value
        case .tinyInt, .bit:
            _ = 0
        case .smallInt:
            _ = 0
        case .int, .smallDateTime, .real, .smallMoney:
            _ = 0
        case .money, .datetime, .float, .bigInt:
            _ = 0
        case .null:
            _ = 0
        default:
            guard buffer.readByteLen() != nil else { throw TDSError.protocolError("TYPE_INFO length") }
        }

        if dataType.isCollationType() {
            guard let coll = buffer.readBytes(length: 5) else { throw TDSError.protocolError("TYPE_INFO collation") }
            collation = coll
        }

        if dataType.isPrecisionType() {
            guard let p = buffer.readByte(), p <= 38 else { throw TDSError.protocolError("TYPE_INFO precision") }
            precision = Int(p)
        }
        if dataType.isScaleType() {
            guard let s = buffer.readByte() else { throw TDSError.protocolError("TYPE_INFO scale") }
            if let p = precision, s > p { throw TDSError.protocolError("TYPE_INFO scale > precision") }
            scale = Int(s)
        }

        return TypeMetadata(userType: 0, flags: 0, dataType: dataType, collation: collation, precision: precision, scale: scale)
    }

    static func parseReturnValueToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ReturnValueToken {
        guard let name = buffer.readBVarchar(),
              let status = buffer.readByte(),
              let userType = buffer.readULong(),
              let flags = buffer.readUShort() else {
            throw TDSError.protocolError("Invalid RETURNVALUE token header")
        }
        var meta = try parseTypeInfo(from: &buffer)
        let colMeta = TDSTokens.ColMetadataToken.ColumnData(
            userType: meta.userType,
            flags: meta.flags,
            dataType: meta.dataType,
            length: 0xFFFF,
            collation: meta.collation,
            tableName: nil,
            colName: name,
            precision: meta.precision,
            scale: meta.scale
        )
        let value = try TDSTokenParser.readColumnValue(for: colMeta, from: &buffer)
        meta.userType = userType
        meta.flags = flags
        return TDSTokens.ReturnValueToken(name: name, status: status, userType: userType, flags: flags, metadata: meta, value: value)
    }
}
