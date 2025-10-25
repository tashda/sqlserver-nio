extension TDSTokenParser {
    public static func parseRowToken(
        from buffer: inout ByteBuffer,
        with colMetadata: TDSTokens.ColMetadataToken
    ) throws -> TDSTokens.RowToken {
        var colData: [TDSTokens.RowToken.ColumnData] = []
        colData.reserveCapacity(Int(colMetadata.count))

        for column in colMetadata.colData {
            let value = try readColumnValue(for: column, from: &buffer)
            colData.append(
                TDSTokens.RowToken.ColumnData(
                    textPointer: [],
                    timestamp: [],
                    data: value
                )
            )
        }

        return TDSTokens.RowToken(colData: colData)
    }

    public static func parseNbcRowToken(
        from buffer: inout ByteBuffer,
        with colMetadata: TDSTokens.ColMetadataToken
    ) throws -> TDSTokens.NbcRowToken {
        let columnCount = Int(colMetadata.count)
        let bitmapByteCount = (columnCount + 7) / 8

        guard buffer.readableBytes >= bitmapByteCount else {
            throw TDSError.needMoreData
        }

        guard let nullBitmap = buffer.readBytes(length: bitmapByteCount) else {
            throw TDSError.protocolError("Error while reading NBCROW null bitmap.")
        }

        var colData: [TDSTokens.RowToken.ColumnData] = []
        colData.reserveCapacity(columnCount)

        for (index, column) in colMetadata.colData.enumerated() {
            let byteIndex = index / 8
            let bitMask = 1 << (index % 8)
            let isNull = (nullBitmap[byteIndex] & UInt8(bitMask)) != 0

            if isNull {
                colData.append(
                    TDSTokens.RowToken.ColumnData(
                        textPointer: [],
                        timestamp: [],
                        data: nil
                    )
                )
                continue
            }

            let value = try readColumnValue(for: column, from: &buffer)
            colData.append(
                TDSTokens.RowToken.ColumnData(
                    textPointer: [],
                    timestamp: [],
                    data: value
                )
            )
        }

        return TDSTokens.NbcRowToken(nullBitmap: nullBitmap, colData: colData)
    }

    private static func readColumnValue(
        for column: TDSTokens.ColMetadataToken.ColumnData,
        from buffer: inout ByteBuffer
    ) throws -> ByteBuffer? {
        switch column.dataType {
        case .sqlVariant:
            guard buffer.readableBytes >= MemoryLayout<UInt32>.size else {
                throw TDSError.needMoreData
            }
            guard let length = buffer.readInteger(endianness: .little, as: UInt32.self) else {
                throw TDSError.protocolError("Failed to read SQLVARIANT length")
            }
            if length == 0 {
                return nil
            }
            let requiredBytes = Int(length)
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let payload = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Failed to read SQLVARIANT payload")
            }
            return payload
            
        case .xml:
            // XML is a PLP-encoded type
            guard let plpData = try buffer.readPLPBytes() else {
                return nil
            }
            return plpData

        case .nText, .text, .image:
            // TEXT/NTEXT/IMAGE layout in row data:
            //  - textptr length (BYTE). If 0, the value is NULL and no more data follows for this column
            //  - textptr (length bytes), typically 16 bytes
            //  - timestamp (8 bytes)
            //  - LONGLEN (Int32): length of data that follows; 0 for empty
            //  - data bytes
            guard let textPtrLen = buffer.readByte() else {
                throw TDSError.needMoreData
            }
            if textPtrLen == 0 {
                return nil
            }
            let ptrLen = Int(textPtrLen)
            guard buffer.readableBytes >= ptrLen + 8 + MemoryLayout<LongLen>.size else {
                throw TDSError.needMoreData
            }
            guard let _ = buffer.readBytes(length: ptrLen) else {
                throw TDSError.protocolError("Failed to read TEXT/NTEXT/IMAGE text pointer")
            }
            guard let _ = buffer.readBytes(length: 8) else {
                throw TDSError.protocolError("Failed to read TEXT/NTEXT/IMAGE timestamp")
            }
            guard let len = buffer.readLongLen() else {
                throw TDSError.protocolError("Error while reading large-length column.")
            }
            if len == -1 {
                return nil
            }
            let requiredBytes = Int(len)
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading large-length column data.")
            }
            return data

        case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary,
             .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
            let usesPLP = column.length == 0xFFFF
            if usesPLP {
                guard let plpData = try buffer.readPLPBytes() else {
                    return nil
                }
                return plpData
            }
            guard buffer.readableBytes >= MemoryLayout<UShortCharBinLen>.size else {
                throw TDSError.needMoreData
            }
            guard let len = buffer.readUShortCharBinLen() else {
                throw TDSError.protocolError("Error while reading variable-length column.")
            }
            if len == 0xFFFF {
                return nil
            }
            let requiredBytes = Int(len)
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading variable-length column data.")
            }
            return data

        case .intn, .decimal, .decimalLegacy, .numeric, .numericLegacy,
             .floatn, .moneyn, .datetimen, .datetime2, .datetimeOffset,
             .time, .bitn:
            guard buffer.readableBytes >= MemoryLayout<ByteLen>.size else {
                throw TDSError.needMoreData
            }
            guard let len = buffer.readByteLen() else {
                throw TDSError.protocolError("Error while reading numeric column length.")
            }
            if len == 0 {
                return nil
            }
            let requiredBytes = Int(len)
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading numeric column data.")
            }
            return data

        case .guid:
            let requiredBytes = 16
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading GUID column data.")
            }
            return data

        case .date:
            let requiredBytes = 3
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading DATE column data.")
            }
            return data

        case .tinyInt, .bit:
            let requiredBytes = 1
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading TINYINT/BIT column data.")
            }
            return data

        case .smallInt:
            let requiredBytes = 2
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading SMALLINT column data.")
            }
            return data

        case .int, .smallDateTime, .real, .smallMoney:
            let requiredBytes = 4
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading 4-byte column data.")
            }
            return data

        case .money, .datetime, .float, .bigInt:
            let requiredBytes = 8
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading 8-byte column data.")
            }
            return data

        case .null:
            return nil

        case .clrUdt:
            throw TDSError.protocolError("CLR UDT parsing is not implemented.")

        @unknown default:
            throw TDSError.protocolError("Unhandled TDS data type: \(column.dataType)")
        }
    }
}
