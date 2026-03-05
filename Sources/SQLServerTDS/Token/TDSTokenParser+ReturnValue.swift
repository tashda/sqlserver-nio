import NIOCore

extension TDSTokenParser {

    // MARK: - RETURNVALUE token (§2.2.7.19)

    static func parseReturnValueToken(
        from buffer: inout ByteBuffer,
        allocator: ByteBufferAllocator
    ) throws -> TDSTokens.ReturnValueToken {
        // ParamOrdinal: USHORT
        guard let paramOrdinal = buffer.readInteger(endianness: .little, as: UInt16.self) else {
            throw TDSError.needMoreData
        }
        // ParamName: B_VARCHAR (1-byte char count, then UTF-16LE)
        guard let name = buffer.readBVarchar() else {
            throw TDSError.needMoreData
        }
        // Status: BYTE (0x01 = output param, 0x02 = UDF return value)
        guard let status = buffer.readByte() else {
            throw TDSError.needMoreData
        }
        // UserType: ULONG (4 bytes for TDS 7.2+)
        guard let userType = buffer.readULong() else {
            throw TDSError.needMoreData
        }
        // Flags: USHORT
        guard let flags = buffer.readUShort() else {
            throw TDSError.needMoreData
        }

        // TypeInfo — same structure as a COLMETADATA column entry (no colName at end)
        guard let dataTypeVal = buffer.readByte() else {
            throw TDSError.needMoreData
        }
        guard let dataType = TDSDataType(rawValue: dataTypeVal) else {
            throw TDSError.protocolError("RETURNVALUE: unknown data type 0x\(String(format: "%02X", dataTypeVal))")
        }

        var length: Int
        switch dataType {
        case .sqlVariant, .nText, .text, .image:
            guard let len = buffer.readLongLen() else { throw TDSError.needMoreData }
            length = Int(len)
        case .vector:
            guard let len = buffer.readUShortLen() else { throw TDSError.needMoreData }
            length = Int(len)
            guard buffer.readByte() != nil else { throw TDSError.needMoreData }
        case .json:
            length = 0xFFFF
        case .xml:
            if let present = buffer.readByte(), present != 0 {
                if let dbChars = buffer.readByte() {
                    guard buffer.readBytes(length: Int(dbChars) * 2) != nil else { throw TDSError.needMoreData }
                }
                if let schemaChars = buffer.readByte() {
                    guard buffer.readBytes(length: Int(schemaChars) * 2) != nil else { throw TDSError.needMoreData }
                }
                if let collChars = buffer.readUShort() {
                    guard buffer.readBytes(length: Int(collChars) * 2) != nil else { throw TDSError.needMoreData }
                }
            }
            length = 0xFFFF
        case .clrUdt:
            guard let maxLen = buffer.readUShort() else { throw TDSError.needMoreData }
            length = Int(maxLen)
            if let dbChars = buffer.readByte() {
                guard buffer.readBytes(length: Int(dbChars) * 2) != nil else { throw TDSError.needMoreData }
            }
            if let schemaChars = buffer.readByte() {
                guard buffer.readBytes(length: Int(schemaChars) * 2) != nil else { throw TDSError.needMoreData }
            }
            if let typeChars = buffer.readByte() {
                guard buffer.readBytes(length: Int(typeChars) * 2) != nil else { throw TDSError.needMoreData }
            }
            if let asmChars = buffer.readUShort() {
                guard buffer.readBytes(length: Int(asmChars) * 2) != nil else { throw TDSError.needMoreData }
            }
        case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
            guard let len = buffer.readUShortLen() else { throw TDSError.needMoreData }
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
            guard let len = buffer.readByteLen() else { throw TDSError.needMoreData }
            length = Int(len)
        }

        var collation: [UInt8] = []
        if dataType.isCollationType() {
            guard let c = buffer.readBytes(length: 5) else { throw TDSError.needMoreData }
            collation = c
        }

        var precision: Int?
        if dataType.isPrecisionType() {
            guard let p = buffer.readByte() else { throw TDSError.needMoreData }
            guard p <= 38 else { throw TDSError.protocolError("RETURNVALUE: invalid precision \(p)") }
            precision = Int(p)
        }

        var scale: Int?
        if dataType.isScaleType() {
            guard let s = buffer.readByte() else { throw TDSError.needMoreData }
            scale = Int(s)
        }

        // Build a synthetic ColumnData so we can reuse readTypedValue
        let syntheticColumn = TDSTokens.ColMetadataToken.ColumnData(
            userType: userType,
            flags: flags,
            dataType: dataType,
            length: length,
            collation: collation,
            tableName: nil,
            colName: name,
            precision: precision,
            scale: scale
        )

        let value = try TDSTokenParser.readTypedValue(from: &buffer, column: syntheticColumn, allocator: allocator)

        let metadata = TypeMetadata(
            userType: userType,
            flags: flags,
            dataType: dataType,
            collation: collation,
            precision: precision,
            scale: scale
        )

        return TDSTokens.ReturnValueToken(
            paramOrdinal: paramOrdinal,
            name: name,
            status: status,
            userType: userType,
            flags: flags,
            metadata: metadata,
            value: value
        )
    }

    // MARK: - Shared typed-value reader

    /// Reads the wire-encoded value for a column from `buffer`, matching the
    /// encoding used in ROW / NBCROW / RETURNVALUE tokens.  Returns nil for SQL NULL.
    static func readTypedValue(
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
            if len == 0xFF { return nil }
            return try readSlice(length: Int(len))
        }

        func readUShortLengthPayload() throws -> ByteBuffer? {
            guard let len = buffer.readInteger(endianness: .little, as: UInt16.self) else { throw TDSError.needMoreData }
            if len == UInt16.max { return nil }
            return try readSlice(length: Int(len))
        }

        func readPLPPayload() throws -> ByteBuffer? {
            let saved = buffer.readerIndex
            do {
                return try buffer.readPLPBytes()
            } catch TDSError.needMoreData {
                buffer.moveReaderIndex(to: saved)
                throw TDSError.needMoreData
            }
        }

        func timePayloadLength(scale: Int?) -> Int {
            switch max(0, min(scale ?? 7, 7)) {
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
            let startIndex = buffer.readerIndex
            guard let declaredLength = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if declaredLength == 0 || declaredLength == 0xFF { return nil }
            if declaredLength == 0x10 { return try readSlice(length: 16) }
            buffer.moveReaderIndex(to: startIndex)
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
            if column.length >= 0xFFFF { return try readPLPPayload() }
            return try readUShortLengthPayload()

        case .nchar, .nvarchar:
            if column.length >= 0xFFFF { return try readPLPPayload() }
            return try readUShortLengthPayload()

        case .text, .nText, .image:
            guard let ptrLen = buffer.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if ptrLen == 0 { return nil }
            guard buffer.readBytes(length: Int(ptrLen)) != nil else { throw TDSError.needMoreData }
            guard buffer.readBytes(length: 8) != nil else { throw TDSError.needMoreData }
            guard let dataLen = buffer.readInteger(endianness: .little, as: UInt32.self) else { throw TDSError.needMoreData }
            if dataLen == UInt32.max { return nil }
            return try readSlice(length: Int(dataLen))

        case .xml:
            return try readPLPPayload()

        case .clrUdt:
            let saved = buffer.readerIndex
            do {
                return try readPLPPayload()
            } catch TDSError.needMoreData {
                buffer.moveReaderIndex(to: saved)
                return try readUShortLengthPayload()
            }

        case .json, .vector:
            if column.length >= 0xFFFF { return try readPLPPayload() }
            return try readUShortLengthPayload()

        case .sqlVariant:
            guard let totalLen = buffer.readInteger(endianness: .little, as: UInt32.self) else { throw TDSError.needMoreData }
            if totalLen == 0 { return nil }
            return try readSlice(length: Int(totalLen))

        @unknown default:
            throw TDSError.protocolError("readTypedValue: unsupported data type \(column.dataType)")
        }
    }
}
