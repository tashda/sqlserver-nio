import NIOCore

extension TDSTokenOperations {
    internal static func parseColMetadataToken(from buffer: inout ByteBuffer) throws -> TDSTokens.ColMetadataToken {
        if let first: UInt8 = buffer.getInteger(at: buffer.readerIndex),
           first == TDSTokens.TokenType.colMetadata.rawValue {
            _ = buffer.readInteger(as: UInt8.self)
        }

        guard let countRaw: UInt16 = buffer.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }

        // TDS: COUNT == 0xFFFF means no columns follow for this result set.
        if countRaw == 0xFFFF {
            return TDSTokens.ColMetadataToken(colData: [])
        }

        let count = Int(countRaw)
        var colData: [TDSTokens.ColMetadataToken.ColumnData] = []

        for _ in 0..<count {
            guard let userType: UInt32 = buffer.readInteger(endianness: .little),
                  let flags: UInt16 = buffer.readInteger(endianness: .little),
                  let dataTypeByte: UInt8 = buffer.readInteger() else {
                throw TDSError.needMoreData
            }

            guard let dataType = TDSDataType(rawValue: dataTypeByte) else {
                throw TDSError.protocolError("Invalid data type 0x\(String(format: "%02X", dataTypeByte))")
            }

            var length: Int32 = 0
            switch dataType {
            case .null: length = 0
            case .tinyInt, .bit: length = 1
            case .smallInt: length = 2
            case .int, .real, .smallMoney, .smallDateTime: length = 4
            case .bigInt, .float, .money, .datetime: length = 8
            case .guid, .intn, .floatn, .moneyn, .datetimen, .bitn, .decimal, .decimalLegacy, .numeric, .numericLegacy, .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
                guard let len: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
                length = Int32(len)
            case .date:
                // DATE has a fixed 3-byte payload and no TYPE_VARLEN in TYPE_INFO.
                length = 3
            case .time, .datetime2, .datetimeOffset:
                // TDS 7.3+ encodes TIME/DATETIME2/DATETIMEOFFSET TYPE_INFO as SCALE only.
                // There is no preceding TYPE_VARLEN byte; payload length is derived from SCALE.
                length = 0
            case .char, .varchar, .binary, .varbinary, .nchar, .nvarchar, .clrUdt, .xml, .json, .vector:
                guard let len: UInt16 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
                length = Int32(len)
            case .text, .nText, .image:
                guard let len: Int32 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
                length = len
            case .sqlVariant:
                guard let len: Int32 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
                length = len
            }

            // Skip collation, precision, scale if present for length calculation, but here we just need to advance
            if dataType == .varchar || dataType == .char || dataType == .nvarchar || dataType == .nchar || dataType == .text || dataType == .nText {
                _ = buffer.readBytes(length: 5) // collation
            }
            
            var precision: UInt8 = 0
            var scale: UInt8 = 0
            var udtInfo: TDSTokens.ColMetadataToken.ColumnData.UDTInfo?
            if dataType == .decimal || dataType == .numeric || dataType == .decimalLegacy || dataType == .numericLegacy {
                precision = buffer.readInteger() ?? 0
                scale = buffer.readInteger() ?? 0
            } else if dataType == .time || dataType == .datetime2 || dataType == .datetimeOffset {
                scale = buffer.readInteger() ?? 0
            }

            // Legacy LOB metadata includes an owning table name after TYPE_INFO.
            if dataType == .text || dataType == .nText || dataType == .image {
                guard let numParts: UInt8 = buffer.readInteger() else {
                    throw TDSError.needMoreData
                }
                for _ in 0..<numParts {
                    guard let partLength: UInt16 = buffer.readInteger(endianness: .little) else {
                        throw TDSError.needMoreData
                    }
                    guard buffer.readUTF16String(length: Int(partLength) * 2) != nil else {
                        throw TDSError.needMoreData
                    }
                }
            } else if dataType == .clrUdt {
                udtInfo = try consumeUDTTypeInfo(from: &buffer)
            }

            guard let colNameLen: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
            guard let colName = buffer.readUTF16String(length: Int(colNameLen) * 2) else { throw TDSError.needMoreData }

            colData.append(
                TDSTokens.ColMetadataToken.ColumnData(
                    userType: userType,
                    flags: flags,
                    dataType: dataType,
                    length: length,
                    precision: precision,
                    scale: scale,
                    colName: colName,
                    udtInfo: udtInfo
                )
            )
        }

        return TDSTokens.ColMetadataToken(colData: colData)
    }

    private static func consumeUDTTypeInfo(
        from buffer: inout ByteBuffer
    ) throws -> TDSTokens.ColMetadataToken.ColumnData.UDTInfo {
        guard let databaseNameLength: UInt8 = buffer.readInteger() else {
            throw TDSError.needMoreData
        }
        guard let databaseName = buffer.readUTF16String(length: Int(databaseNameLength) * 2) else {
            throw TDSError.needMoreData
        }

        guard let schemaNameLength: UInt8 = buffer.readInteger() else {
            throw TDSError.needMoreData
        }
        guard let schemaName = buffer.readUTF16String(length: Int(schemaNameLength) * 2) else {
            throw TDSError.needMoreData
        }

        guard let typeNameLength: UInt8 = buffer.readInteger() else {
            throw TDSError.needMoreData
        }
        guard let typeName = buffer.readUTF16String(length: Int(typeNameLength) * 2) else {
            throw TDSError.needMoreData
        }

        guard let assemblyNameLength: UInt16 = buffer.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }
        guard let assemblyName = buffer.readUTF16String(length: Int(assemblyNameLength) * 2) else {
            throw TDSError.needMoreData
        }

        return .init(
            databaseName: databaseName,
            schemaName: schemaName,
            typeName: typeName,
            assemblyName: assemblyName
        )
    }
}
