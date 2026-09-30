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
        .sqlResultColumnSources,
        .unknown0x61,
        .unknown0x74,
        .unknown0xc1,
        .returnStatus,
        .returnValue,
        .columnStatus
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

    /// Parses every complete token currently buffered.
    ///
    /// Throws on malformed data. Tokens decoded before the failure are lost;
    /// use `parseAvailable(messageComplete:)` when they matter.
    public func parse(messageComplete: Bool = true) throws -> [TDSToken] {
        let result = parseAvailable(messageComplete: messageComplete)
        if let error = result.error {
            throw error
        }
        return result.tokens
    }

    /// Parses every complete token currently buffered and stops at the first
    /// incomplete one, leaving `streamParser.position` at its first byte so
    /// parsing resumes there when more bytes arrive.
    ///
    /// While `messageComplete` is false a decode failure is treated as a token
    /// that has not fully arrived yet, because the remaining bytes of the
    /// message may still be in flight. Once the end of the message has been
    /// received, the same failure is a protocol error and is returned together
    /// with the tokens decoded before it.
    public func parseAvailable(messageComplete: Bool) -> (tokens: [TDSToken], error: Error?) {
        var tokens: [TDSToken] = []
        while true {
            let start = streamParser.position
            let startState = state
            do {
                guard let step = try parseNextToken() else {
                    return (tokens, nil)
                }
                if let token = step {
                    tokens.append(token)
                }
            } catch TDSError.needMoreData {
                streamParser.position = start
                state = startState
                return (tokens, nil)
            } catch {
                streamParser.position = start
                state = startState
                return (tokens, messageComplete ? error : nil)
            }
        }
    }

    /// Returns nil when no further byte is buffered, `.some(nil)` when bytes
    /// were consumed without producing a token, or the parsed token.
    private func parseNextToken() throws -> TDSToken?? {
        guard let nextByte = streamParser.peekUInt8() else {
            return nil
        }

        if nextByte == 0x00 || nextByte == TDSTokens.TokenType.unknown0x04.rawValue {
            _ = streamParser.readUInt8()
            return .some(nil)
        }

        guard let nextType = TDSTokens.TokenType(rawValue: nextByte) else {
            // Every token has a known framing. An unknown byte means the stream
            // is no longer aligned, and skipping it would decode garbage.
            throw TDSError.protocolError("Unknown TDS token 0x\(String(format: "%02X", nextByte)) at offset \(streamParser.position)")
        }

        if TDSTokenOperations.generalTokenTypes.contains(nextType) {
            guard let generalToken = try parseGeneralTokenIfNeeded(for: nextType) else {
                throw TDSError.needMoreData
            }
            return generalToken
        }

        switch nextType {
        case .colMetadata:
            _ = streamParser.readUInt8()
            var bufferCopy = streamParser.buffer
            bufferCopy.moveReaderIndex(to: streamParser.position)
            let colMetadataToken = try TDSTokenOperations.parseColMetadataToken(from: &bufferCopy)
            self.colMetadata = colMetadataToken
            streamParser.position = bufferCopy.readerIndex
            state = .expectingRow
            return colMetadataToken
        case .row:
            guard let token = try parseRowToken() else { throw TDSError.needMoreData }
            return token
        case .nbcRow:
            guard let token = try parseNbcRowToken() else { throw TDSError.needMoreData }
            return token
        case .tvpRow:
            guard let token = try parseTVPRowToken() else { throw TDSError.needMoreData }
            return token
        case .order:
            guard let token = try parseOrderToken() else { throw TDSError.needMoreData }
            return token
        case .done, .doneInProc, .doneProc:
            guard let token = try parseDoneToken() else { throw TDSError.needMoreData }
            state = .expectingColMetadata
            return token
        default:
            throw TDSError.protocolError("Unexpected TDS token \(nextType) at offset \(streamParser.position)")
        }
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
                guard let identifier = payload.readInteger(endianness: .little, as: UInt16.self),
                      let offset = payload.readInteger(endianness: .little, as: UInt16.self) else {
                    throw TDSError.needMoreData
                }
                token = TDSTokens.OffsetToken(identifier: identifier, offset: offset)
            case .dataClassification:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                token = TDSTokens.DataClassificationToken(payload: data)
            case .sqlResultColumnSources:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 4)
                token = TDSTokens.SQLResultColumnSourcesToken(payload: data)
            case .unknown0x61:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                token = TDSTokens.Unknown0x61Token(payload: data)
            case .unknown0x74:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                token = TDSTokens.Unknown0x74Token(payload: data)
            case .unknown0xc1:
                let data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                token = TDSTokens.Unknown0xC1Token(payload: data)
            case .columnStatus:
                var data = try TDSTokenOperations.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                let status = bytes.count >= 2 ? UInt16(bytes[0]) | UInt16(bytes[1]) << 8 : 0
                let statusBytes = bytes.count >= 4 ? Array(bytes.dropFirst(2)) : []
                token = TDSTokens.ColumnStatusToken(status: status, data: statusBytes)
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
