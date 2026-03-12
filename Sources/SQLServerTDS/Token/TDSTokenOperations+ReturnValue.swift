import NIOCore

extension TDSTokenOperations {
    internal func parseReturnValueToken(from buffer: inout ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSTokens.ReturnValueToken {
        guard let paramOrdinal = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.needMoreData
        }
        guard let nameLength = buffer.readInteger(as: UInt8.self) else {
            throw TDSError.needMoreData
        }
        guard let name = buffer.readUTF16String(length: Int(nameLength) * 2) else {
            throw TDSError.needMoreData
        }
        guard let status = buffer.readInteger(as: UInt8.self) else {
            throw TDSError.needMoreData
        }
        guard let userType = buffer.readInteger(endianness: .little, as: UInt32.self) else {
            throw TDSError.needMoreData
        }
        guard let flags = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.needMoreData
        }
        guard let dataTypeByte = buffer.readInteger(as: UInt8.self) else {
            throw TDSError.needMoreData
        }
        guard let dataType = TDSDataType(rawValue: dataTypeByte) else {
            throw TDSError.protocolError("RETURNVALUE: unknown data type 0x\(String(format: "%02X", dataTypeByte))")
        }

        var length: Int
        switch dataType {
        case .sqlVariant, .nText, .text, .image:
            guard let len = buffer.readInteger(endianness: .little, as: UInt32.self) else { throw TDSError.needMoreData }
            length = Int(len)
        case .xml:
            if let schemaPresent = buffer.readInteger(as: UInt8.self), schemaPresent != 0 {
                if let dbChars = buffer.readInteger(as: UInt8.self) {
                    guard buffer.readBytes(length: Int(dbChars) * 2) != nil else { throw TDSError.needMoreData }
                }
                if let ownerChars = buffer.readInteger(as: UInt8.self) {
                    guard buffer.readBytes(length: Int(ownerChars) * 2) != nil else { throw TDSError.needMoreData }
                }
                if let collectionChars = buffer.readInteger(endianness: .little, as: UInt16.self) {
                    guard buffer.readBytes(length: Int(collectionChars) * 2) != nil else { throw TDSError.needMoreData }
                }
            }
            length = 0xFFFF
        case .clrUdt:
            guard let maxLen = buffer.readInteger(endianness: .little, as: UInt16.self) else { throw TDSError.needMoreData }
            length = Int(maxLen)
            if let dbChars = buffer.readInteger(as: UInt8.self) {
                guard buffer.readBytes(length: Int(dbChars) * 2) != nil else { throw TDSError.needMoreData }
            }
            if let ownerChars = buffer.readInteger(as: UInt8.self) {
                guard buffer.readBytes(length: Int(ownerChars) * 2) != nil else { throw TDSError.needMoreData }
            }
            if let typeChars = buffer.readInteger(as: UInt8.self) {
                guard buffer.readBytes(length: Int(typeChars) * 2) != nil else { throw TDSError.needMoreData }
            }
            if let assemblyChars = buffer.readInteger(endianness: .little, as: UInt16.self) {
                guard buffer.readBytes(length: Int(assemblyChars) * 2) != nil else { throw TDSError.needMoreData }
            }
        case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
            guard let len = buffer.readInteger(endianness: .little, as: UInt16.self) else { throw TDSError.needMoreData }
            length = Int(len)
        case .date:
            length = 3
        case .tinyInt, .bit:
            length = 1
        case .smallInt:
            length = 2
        case .int, .smallDateTime, .real, .smallMoney:
            length = 4
        case .money, .datetime, .float, .bigInt:
            length = 8
        case .null:
            length = 0
        default:
            guard let len = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            length = Int(len)
        }

        var collation: [UInt8] = []
        if dataType.isCollationType() {
            guard let bytes = buffer.readBytes(length: 5) else { throw TDSError.needMoreData }
            collation = bytes
        }

        var precision: UInt8 = 0
        if dataType.isPrecisionType() {
            guard let p = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            precision = p
        }

        var scale: UInt8 = 0
        if dataType.isScaleType() {
            guard let s = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            scale = s
        }

        let metadata = TDSTokens.ColMetadataToken.ColumnData(
            userType: userType,
            flags: flags,
            dataType: dataType,
            length: Int32(length),
            collation: collation,
            tableName: nil,
            colName: name,
            precision: precision,
            scale: scale
        )

        let value = try readTypedValue(from: &buffer, column: metadata, allocator: allocator)
        streamParser.position = buffer.readerIndex

        return TDSTokens.ReturnValueToken(
            ordinal: paramOrdinal,
            name: name,
            status: status,
            userType: userType,
            flags: flags,
            metadata: metadata,
            value: value
        )
    }

    private func readTypedValue(
        from buffer: inout ByteBuffer,
        column: TDSTokens.ColMetadataToken.ColumnData,
        allocator: ByteBufferAllocator
    ) throws -> ByteBuffer? {
        func readSlice(length: Int) throws -> ByteBuffer {
            guard var slice = buffer.readSlice(length: length) else {
                throw TDSError.needMoreData
            }
            var copy = allocator.buffer(capacity: slice.readableBytes)
            copy.writeBuffer(&slice)
            return copy
        }

        func readByteLengthPayload() throws -> ByteBuffer? {
            guard let len = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if len == 0 || len == 0xFF { return nil }
            return try readSlice(length: Int(len))
        }

        func readUShortLengthPayload() throws -> ByteBuffer? {
            guard let len = buffer.readInteger(endianness: .little, as: UInt16.self) else { throw TDSError.needMoreData }
            if len == UInt16.max { return nil }
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

        func timePayloadLength(scale: UInt8) -> Int {
            switch max(0, min(Int(scale), 7)) {
            case 0...2: return 3
            case 3...4: return 4
            default: return 5
            }
        }

        switch column.dataType {
        case .null:
            return nil
        case .tinyInt, .bit:
            return try readSlice(length: 1)
        case .smallInt:
            return try readSlice(length: 2)
        case .int, .real, .smallMoney, .smallDateTime:
            return try readSlice(length: 4)
        case .bigInt, .float, .money, .datetime:
            return try readSlice(length: 8)
        case .guid:
            let savedIndex = buffer.readerIndex
            guard let declaredLength = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if declaredLength == 0 || declaredLength == 0xFF { return nil }
            if declaredLength == 0x10 { return try readSlice(length: 16) }
            buffer.moveReaderIndex(to: savedIndex)
            return try readSlice(length: 16)
        case .date:
            return try readSlice(length: 3)
        case .time:
            return try readSlice(length: timePayloadLength(scale: column.scale))
        case .datetime2:
            return try readSlice(length: timePayloadLength(scale: column.scale) + 3)
        case .datetimeOffset:
            return try readSlice(length: timePayloadLength(scale: column.scale) + 5)
        case .intn, .floatn, .moneyn, .datetimen, .bitn, .decimal, .decimalLegacy, .numeric, .numericLegacy:
            guard let len = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if len == 0 { return nil }
            return try readSlice(length: Int(len))
        case .varcharLegacy, .charLegacy, .binaryLegacy, .varbinaryLegacy:
            return try readByteLengthPayload()
        case .char, .varchar, .binary, .varbinary:
            return column.length >= 0xFFFF ? try readPLPPayload() : try readUShortLengthPayload()
        case .nchar, .nvarchar:
            return column.length >= 0xFFFF ? try readPLPPayload() : try readUShortLengthPayload()
        case .text, .nText, .image:
            guard let pointerLength = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if pointerLength == 0 { return nil }
            guard buffer.readBytes(length: Int(pointerLength)) != nil else { throw TDSError.needMoreData }
            guard buffer.readBytes(length: 8) != nil else { throw TDSError.needMoreData }
            guard let dataLength = buffer.readInteger(endianness: .little, as: UInt32.self) else { throw TDSError.needMoreData }
            if dataLength == UInt32.max { return nil }
            return try readSlice(length: Int(dataLength))
        case .xml:
            return try readPLPPayload()
        case .clrUdt:
            let savedIndex = buffer.readerIndex
            do {
                return try readPLPPayload()
            } catch TDSError.needMoreData {
                buffer.moveReaderIndex(to: savedIndex)
                return try readUShortLengthPayload()
            }
        case .json, .vector:
            return column.length >= 0xFFFF ? try readPLPPayload() : try readUShortLengthPayload()
        case .sqlVariant:
            guard let totalLength = buffer.readInteger(endianness: .little, as: UInt32.self) else { throw TDSError.needMoreData }
            if totalLength == 0 { return nil }
            return try readSlice(length: Int(totalLength))
        @unknown default:
            throw TDSError.protocolError("Unsupported RETURNVALUE data type \(column.dataType)")
        }
    }
}
