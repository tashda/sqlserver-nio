
import NIOCore
import Logging

public class TDSTokenParser {
    internal let streamParser: TDSStreamParser
    private let logger: Logger
    private var state: State = .expectingColMetadata
    internal var colMetadata: TDSTokens.ColMetadataToken?
    private let allocator = ByteBufferAllocator()
    private static let generalTokenTypes: Set<TDSTokens.TokenType> = [
        .envchange,
        .info,
        .error,
        .loginAck,
        .featureExtAck,
        .fedAuthInfo,
        .sessionState,
        .tabName,
        .colInfo,
        .offset,
        .dataClassification
    ]

    private enum State {
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
        var iterations = 0
        let maxIterations = 5000

        parsingLoop: while iterations < maxIterations {
            iterations += 1

            guard let nextByte = streamParser.peekUInt8() else {
                break
            }

            if nextByte == 0x00 {
                _ = streamParser.readUInt8()
                continue
            }

            if nextByte == TDSTokens.TokenType.unknown0x04.rawValue {
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
            } else if TDSTokenParser.generalTokenTypes.contains(nextType) {
                // Token consumed (e.g., feature acknowledgement). Nothing further to emit.
                continue
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
                    let colMetadataToken = try TDSTokenParser.parseColMetadataToken(from: &bufferCopy)
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
                } else if nextType == .order {
                    guard let orderToken = try parseOrderToken() else {
                        break parsingLoop
                    }
                    tokens.append(orderToken)
                } else {
                    state = .expectingDone
                }

            case .expectingDone:
                if let doneToken = try parseDoneToken() {
                    tokens.append(doneToken)
                    state = .expectingColMetadata
                } else {
                    break parsingLoop
                }
            }
        }

        if iterations >= maxIterations {
            logger.warning("Token parser reached maximum iteration limit (\(maxIterations)), stopping to prevent infinite loop. State: \(state), Position: \(streamParser.position), Buffer readable: \(streamParser.buffer.readableBytes)")
        }

        return tokens
    }

    private func parseGeneralTokenIfNeeded(for tokenType: TDSTokens.TokenType) throws -> TDSToken? {
        guard TDSTokenParser.generalTokenTypes.contains(tokenType) else {
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
                token = try TDSTokenParser.parseEnvChangeToken(from: &payload)
            case .info, .error:
                token = try TDSTokenParser.parseErrorInfoToken(type: tokenType, from: &payload)
            case .loginAck:
                token = try TDSTokenParser.parseLoginAckToken(from: &payload)
            case .featureExtAck:
                let data = try TDSTokenParser.readFeatureExtAckPayload(from: &payload)
                token = TDSTokens.FeatureExtAckToken(payload: data)
            case .fedAuthInfo:
                let data = try TDSTokenParser.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 4)
                token = TDSTokens.FedAuthInfoToken(payload: data)
            case .sessionState:
                let data = try TDSTokenParser.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 4)
                token = TDSTokens.SessionStateToken(payload: data)
            case .tabName:
                var data = try TDSTokenParser.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                token = TDSTokens.TabNameToken(data: bytes)
            case .colInfo:
                var data = try TDSTokenParser.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                token = TDSTokens.ColInfoToken(data: bytes)
            case .offset:
                var data = try TDSTokenParser.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                let bytes = data.readBytes(length: data.readableBytes) ?? []
                token = TDSTokens.OffsetToken(data: bytes)
            case .dataClassification:
                let data = try TDSTokenParser.readLengthPrefixedPayload(from: &payload, lengthFieldBytes: 2)
                token = TDSTokens.DataClassificationToken(payload: data)
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
            fatalError("Unsupported length-field width \(lengthFieldBytes)")
        }

        guard let slice = buffer.readSlice(length: length) else {
            throw TDSError.needMoreData
        }
        return slice
    }

    private static func readFeatureExtAckPayload(from buffer: inout ByteBuffer) throws -> ByteBuffer {
        let start = buffer.readerIndex

        while true {
            guard let featureId = buffer.readByte() else {
                throw TDSError.needMoreData
            }

            if featureId == 0xFF {
                break
            }

            guard let ackLength = buffer.readUShort() else {
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

    internal func parseNbcRowToken() throws -> TDSTokens.NbcRowToken? {
        let start = streamParser.position
        guard let tokenType = streamParser.readUInt8() else {
            return nil
        }

        guard tokenType == TDSTokens.TokenType.nbcRow.rawValue else {
            streamParser.position = start
            return nil
        }

        guard let colMetadata = self.colMetadata else {
            throw TDSError.protocolError("No COLMETADATA received")
        }

        let columnCount = colMetadata.colData.count
        let bitmapLength = (columnCount + 7) / 8
        guard let bitmap = streamParser.readBytes(count: bitmapLength) else {
            streamParser.position = start
            throw TDSError.needMoreData
        }

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for (index, columnMetadata) in colMetadata.colData.enumerated() {
            let byteIndex = index / 8
            let bitMask = 1 << (index % 8)
            let isNull = (bitmap[byteIndex] & UInt8(bitMask)) != 0
            if isNull {
                columns.append(TDSTokens.RowToken.ColumnData(textPointer: [], timestamp: [], data: nil))
            } else {
                let columnData = try parseColumnValue(for: columnMetadata)
                columns.append(columnData)
            }
        }

        return TDSTokens.NbcRowToken(nullBitmap: bitmap, colData: columns)
    }

    private func parseOrderToken() throws -> TDSTokens.OrderToken? {
        let start = streamParser.position
        guard let tokenType = streamParser.readUInt8() else {
            return nil
        }

        guard tokenType == TDSTokens.TokenType.order.rawValue else {
            streamParser.position = start
            return nil
        }

        guard let length = streamParser.readUInt16LE() else {
            streamParser.position = start
            throw TDSError.needMoreData
        }

        if length % 2 != 0 {
            throw TDSError.protocolError("ORDER token length \(length) is not aligned to 2-byte column ordinals")
        }

        var ordinals: [UInt16] = []
        ordinals.reserveCapacity(Int(length / 2))

        var remaining = Int(length)
        while remaining > 0 {
            guard let ordinal = streamParser.readUInt16LE() else {
                streamParser.position = start
                throw TDSError.needMoreData
            }
            ordinals.append(ordinal)
            remaining -= 2
        }

        return TDSTokens.OrderToken(columnOrdinals: ordinals)
    }

    internal func parseColumnValue(for column: TDSTokens.ColMetadataToken.ColumnData) throws -> TDSTokens.RowToken.ColumnData {
        var buffer = streamParser.buffer
        buffer.moveReaderIndex(to: streamParser.position)

        func finish(_ value: ByteBuffer?, textPointer: [Byte] = [], timestamp: [Byte] = []) -> TDSTokens.RowToken.ColumnData {
            streamParser.position = buffer.readerIndex
            return TDSTokens.RowToken.ColumnData(textPointer: textPointer, timestamp: timestamp, data: value)
        }

        func require<T>(_ value: T?) throws -> T {
            guard let value else {
                throw TDSError.needMoreData
            }
            return value
        }

        func readSlice(length: Int) throws -> ByteBuffer {
            guard var slice = buffer.readSlice(length: length) else {
                throw TDSError.needMoreData
            }
            var copy = allocator.buffer(capacity: slice.readableBytes)
            copy.writeBuffer(&slice)
            return copy
        }

        func readByteLengthPayload(nullMarker: UInt8 = 0xFF) throws -> ByteBuffer? {
            let len: UInt8 = try require(buffer.readInteger(as: UInt8.self))
            if len == nullMarker {
                return nil
            }
            return try readSlice(length: Int(len))
        }

        func readUShortLengthPayload(nullMarker: UInt16 = UInt16.max) throws -> ByteBuffer? {
            let len: UInt16 = try require(buffer.readInteger(endianness: .little, as: UInt16.self))
            if len == nullMarker {
                return nil
            }
            return try readSlice(length: Int(len))
        }

        func readULongLengthPayload(nullMarker: UInt32 = UInt32.max) throws -> ByteBuffer? {
            let len: UInt32 = try require(buffer.readInteger(endianness: .little, as: UInt32.self))
            if len == nullMarker {
                return nil
            }
            return try readSlice(length: Int(len))
        }

        func readPLPPayload() throws -> ByteBuffer? {
            let savedIndex = buffer.readerIndex
            do {
                return try buffer.readPLPBytes()
            } catch TDSError.needMoreData {
                buffer.moveReaderIndex(to: savedIndex)
                throw TDSError.needMoreData
            }
        }

        func timePayloadLength(scale: Int?) -> Int {
            let resolved = max(0, min(scale ?? 7, 7))
            switch resolved {
            case 0...2: return 3
            case 3...4: return 4
            default: return 5
            }
        }

        switch column.dataType {
        case .null:
            return finish(nil)

        case .tinyInt, .bit:
            return finish(try readSlice(length: 1))

        case .smallInt:
            return finish(try readSlice(length: 2))

        case .int, .real, .smallMoney, .smallDateTime:
            return finish(try readSlice(length: 4))

        case .bigInt, .float, .money, .datetime:
            return finish(try readSlice(length: 8))

        case .guid:
            let startIndex = buffer.readerIndex
            guard let declaredLength = buffer.readInteger(as: UInt8.self) else {
                throw TDSError.needMoreData
            }
            if declaredLength == 0 || declaredLength == 0xFF {
                return finish(nil)
            }
            if declaredLength == 0x10 {
                return finish(try readSlice(length: 16))
            }
            buffer.moveReaderIndex(to: startIndex)
            return finish(try readSlice(length: 16))

        case .date:
            return finish(try readSlice(length: 3))

        case .time:
            return finish(try readSlice(length: timePayloadLength(scale: column.scale)))

        case .datetime2:
            return finish(try readSlice(length: timePayloadLength(scale: column.scale) + 3))

        case .datetimeOffset:
            return finish(try readSlice(length: timePayloadLength(scale: column.scale) + 5))

        case .intn, .floatn, .moneyn, .datetimen, .bitn, .decimal, .decimalLegacy, .numeric, .numericLegacy:
            let length: UInt8 = try require(buffer.readInteger(as: UInt8.self))
            if length == 0 {
                return finish(nil)
            }
            return finish(try readSlice(length: Int(length)))

        case .varcharLegacy, .charLegacy, .binaryLegacy, .varbinaryLegacy:
            if let payload = try readByteLengthPayload() {
                return finish(payload)
            } else {
                return finish(nil)
            }

        case .char, .varchar, .binary, .varbinary:
            if column.length >= 0xFFFF {
                return finish(try readPLPPayload())
            }
            if let payload = try readUShortLengthPayload() {
                return finish(payload)
            } else {
                return finish(nil)
            }

        case .nchar, .nvarchar:
            if column.length >= 0xFFFF {
                return finish(try readPLPPayload())
            }
            if let payload = try readUShortLengthPayload() {
                return finish(payload)
            } else {
                return finish(nil)
            }

        case .text, .nText, .image:
            let pointerLength: UInt8 = try require(buffer.readInteger(as: UInt8.self))
            if pointerLength == 0 {
                return finish(nil)
            }
            guard let textPointer = buffer.readBytes(length: Int(pointerLength)) else {
                throw TDSError.needMoreData
            }
            guard let timestampBytes = buffer.readBytes(length: 8) else {
                throw TDSError.needMoreData
            }
            guard let dataLength = buffer.readInteger(endianness: .little, as: UInt32.self) else {
                throw TDSError.needMoreData
            }
            if dataLength == UInt32.max {
                return finish(nil, textPointer: textPointer, timestamp: timestampBytes)
            }
            let payload = try readSlice(length: Int(dataLength))
            return finish(payload, textPointer: textPointer, timestamp: timestampBytes)

        case .xml, .clrUdt:
            return finish(try readPLPPayload())

        case .json:
            if column.length >= 0xFFFF {
                return finish(try readPLPPayload())
            }
            if let payload = try readUShortLengthPayload() {
                return finish(payload)
            } else {
                return finish(nil)
            }

        case .vector:
            if column.length >= 0xFFFF {
                return finish(try readPLPPayload())
            }
            if let payload = try readUShortLengthPayload() {
                return finish(payload)
            } else {
                return finish(nil)
            }

        case .sqlVariant:
            let totalLength: UInt32 = try require(buffer.readInteger(endianness: .little, as: UInt32.self))
            if totalLength == 0 {
                return finish(nil)
            }
            return finish(try readSlice(length: Int(totalLength)))

        @unknown default:
            throw TDSError.protocolError("Unsupported data type \(column.dataType)")
        }
    }
}
