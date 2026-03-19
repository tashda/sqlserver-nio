import Foundation
import NIOCore
import Logging

public class TDSTokenOperations: @unchecked Sendable {
    internal let streamParser: TDSStreamParser
    private let logger: Logger
    internal var state: State = .expectingColMetadata
    internal var colMetadata: TDSTokens.ColMetadataToken?
    internal let allocator = ByteBufferAllocator()
    internal static let generalTokenTypes: Set<TDSTokens.TokenType> = [
        .envchange,
        .info,
        .error,
        .loginAck,
        .featureExtAck,
        .fedAuthInfo,
        .sessionState,
        .sspi,
        .tabName,
        .colInfo,
        .offset,
        .dataClassification,
        .returnStatus,
        .returnValue
    ]

    internal enum State {
        case expectingColMetadata
        case expectingRow
        case expectingDone
    }

    public init(streamParser: TDSStreamParser, logger: Logger) {
        self.streamParser = streamParser
        self.logger = logger
    }

    public func parse() throws -> [TDSToken] {
        var tokens: [TDSToken] = []

        parsingLoop: while true {

            guard let nextByte = streamParser.peekUInt8() else {
                break
            }

            if nextByte == 0x00 {
                _ = streamParser.readUInt8()
                continue
            }

            if nextByte == 0x04 {
                logger.debug("Skipping unknown token 0x04 at position \(streamParser.position)")
                _ = streamParser.readUInt8()
                continue
            }

            guard let nextType = TDSTokens.TokenType(rawValue: nextByte) else {
                logger.warning("Skipping unknown token byte 0x\(String(format: "%02X", nextByte)) at position \(streamParser.position)")
                _ = streamParser.readUInt8()
                continue
            }

            if let generalToken = try parseGeneralTokenIfNeeded(for: nextType) {
                tokens.append(generalToken)
                continue
            } else if TDSTokenOperations.generalTokenTypes.contains(nextType) {
                // General token type but not enough data yet; wait for more bytes.
                break parsingLoop
            }

            switch state {
            case .expectingColMetadata:
                guard nextType == .colMetadata else {
                    state = .expectingRow
                    continue
                }

                let start = streamParser.position
                _ = streamParser.readUInt8() // consume token type
                var bufferCopy = streamParser.buffer
                bufferCopy.moveReaderIndex(to: streamParser.position)

                do {
                    let colMetadataToken = try TDSTokenOperations.parseColMetadataToken(from: &bufferCopy)
                    self.colMetadata = colMetadataToken
                    tokens.append(colMetadataToken)
                    streamParser.position = bufferCopy.readerIndex
                    state = .expectingRow
                } catch TDSError.needMoreData {
                    streamParser.position = start
                    break parsingLoop
                }

            case .expectingRow:
                if nextType == .row {
                    guard let rowToken = try parseRowToken() else {
                        break parsingLoop
                    }
                    tokens.append(rowToken)
                } else if nextType == .nbcRow {
                    guard let nbcToken = try parseNbcRowToken() else {
                        break parsingLoop
                    }
                    tokens.append(nbcToken)
                } else if nextType == .tvpRow {
                    guard let tvpToken = try parseTVPRowToken() else {
                        break parsingLoop
                    }
                    tokens.append(tvpToken)
                } else if nextType == .order {
                    guard let orderToken = try parseOrderToken() else {
                        break parsingLoop
                    }
                    tokens.append(orderToken)
                } else {
                    state = .expectingDone
                }

            case .expectingDone:
                do {
                    if let doneToken = try parseDoneToken() {
                        tokens.append(doneToken)
                        state = .expectingColMetadata
                    } else {
                        break parsingLoop
                    }
                } catch TDSError.needMoreData {
                    break parsingLoop
                }
            }
        }

        return tokens
    }

    private func parseGeneralTokenIfNeeded(for tokenType: TDSTokens.TokenType) throws -> TDSToken? {
        guard TDSTokenOperations.generalTokenTypes.contains(tokenType) else {
            return nil
        }

        let start = streamParser.position
        guard streamParser.readUInt8() != nil else {
            return nil
        }

        var payload = streamParser.buffer
        payload.moveReaderIndex(to: streamParser.position)

        do {
            let token: TDSToken
            switch tokenType {
            case .envchange:
                token = try TDSTokenOperations.parseEnvChangeToken(from: &payload)
            case .info, .error:
                token = try TDSTokenOperations.parseErrorInfoToken(type: tokenType, from: &payload)
            case .loginAck:
                token = try TDSTokenOperations.parseLoginAckToken(from: &payload)
            case .featureExtAck:
                let data = try TDSTokenOperations.readFeatureExtAckPayload(from: &payload)
                token = TDSTokens.FeatureExtAckToken(payload: data)
            case .fedAuthInfo:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 4)
                token = TDSTokens.FedAuthInfoToken(payload: data)
            case .sessionState:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 4)
                token = TDSTokens.SessionStateToken(payload: data)
            case .sspi:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                var dataCopy = data
                let bytes = dataCopy.readBytes(length: dataCopy.readableBytes) ?? []
                token = TDSTokens.SSPIToken(data: Data(bytes))
            case .tabName:
                var data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                token = TDSTokens.TabNameToken(data: bytes)
            case .colInfo:
                var data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                token = TDSTokens.ColInfoToken(data: bytes)
            case .offset:
                var data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                token = TDSTokens.OffsetToken(data: bytes)
            case .dataClassification:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                token = TDSTokens.DataClassificationToken(payload: data)
            case .returnStatus:
                guard let value = payload.readInteger(endianness: .little, as: Int32.self) else {
                    throw TDSError.needMoreData
                }
                token = TDSTokens.ReturnStatusToken(value: value)
            case .returnValue:
                token = try parseReturnValueToken(from: &payload, allocator: allocator)
            default:
                // Should never happen due to guard
                streamParser.position = start
                return nil
            }

            streamParser.position = payload.readerIndex
            return token
        } catch TDSError.needMoreData {
            streamParser.position = start
            return nil
        }
    }

    private static func readLengthPrefixedPayload(from buffer: inout ByteBuffer, lengthFieldBytes: Int) throws -> ByteBuffer {
        let length: Int
        switch lengthFieldBytes {
        case 2:
            guard let len = buffer.readInteger(endianness: .little, as: UInt16.self) else {
                throw TDSError.needMoreData
            }
            length = Int(len)
        case 4:
            guard let len = buffer.readInteger(endianness: .little, as: UInt32.self) else {
                throw TDSError.needMoreData
            }
            length = Int(len)
        default:
            throw TDSError.protocolError("Unsupported length-field width \(lengthFieldBytes)")
        }

        guard let slice = buffer.readSlice(length: length) else {
            throw TDSError.needMoreData
        }
        return slice
    }

    private static func readFeatureExtAckPayload(from buffer: inout ByteBuffer) throws -> ByteBuffer {
        let start = buffer.readerIndex

        while true {
            guard let nextByte = buffer.readInteger(as: UInt8.self) else {
                throw TDSError.needMoreData
            }

            if nextByte == 0xFF {
                break
            }

            guard let ackLength = try? buffer.readUShort() else {
                throw TDSError.needMoreData
            }

            guard buffer.readSlice(length: Int(ackLength)) != nil else {
                throw TDSError.needMoreData
            }
        }

        var payloadCopy = buffer
        payloadCopy.moveReaderIndex(to: start)
        let consumed = buffer.readerIndex - start
        guard let slice = payloadCopy.readSlice(length: consumed) else {
            throw TDSError.needMoreData
        }
        return slice
    }
}
