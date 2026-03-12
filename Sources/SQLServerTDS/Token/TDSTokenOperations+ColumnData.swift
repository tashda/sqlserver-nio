import NIOCore

extension TDSTokenOperations {
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

        case .date, .time, .datetime2, .datetimeOffset:
            if let payload = try readByteLengthPayload(nullMarker: 0x00) {
                return finish(payload)
            } else {
                return finish(nil)
            }

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

        case .xml:
            return finish(try readPLPPayload())

        case .clrUdt:
            let savedIndex = buffer.readerIndex
            do {
                return finish(try readPLPPayload())
            } catch TDSError.needMoreData {
                buffer.moveReaderIndex(to: savedIndex)
                if let payload = try readUShortLengthPayload() {
                    return finish(payload)
                } else {
                    return finish(nil)
                }
            }

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
