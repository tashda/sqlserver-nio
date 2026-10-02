import NIOCore

extension TDSTokenOperations {
    internal static func parseColMetadataToken(from buffer: inout ByteBuffer, columnEncryption: Bool = false) throws -> TDSTokens.ColMetadataToken {
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

        // With COLUMNENCRYPTION negotiated every COLMETADATA starts with the CekTable.
        let cekTable = columnEncryption ? try readCekTable(from: &buffer) : []

        let count = Int(countRaw)
        var colData: [TDSTokens.ColMetadataToken.ColumnData] = []

        for _ in 0..<count {
            guard let userType: UInt32 = buffer.readInteger(endianness: .little),
                  let flags: UInt16 = buffer.readInteger(endianness: .little) else {
                throw TDSError.needMoreData
            }
            let (typeInfo, udtInfo) = try readTypeInfo(from: &buffer)
            let dataType = typeInfo.dataType

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
            }

            var encryption: TDSTokens.ColMetadataToken.ColumnData.ColumnEncryption?
            if columnEncryption, flags & TDSTokens.ColMetadataToken.ColumnData.encryptedFlag != 0 {
                encryption = try readCryptoMetadata(from: &buffer, cekTable: cekTable)
            }

            guard let colNameLen: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
            guard let colName = buffer.readUTF16String(length: Int(colNameLen) * 2) else { throw TDSError.needMoreData }

            var column = TDSTokens.ColMetadataToken.ColumnData(
                userType: userType,
                flags: flags,
                dataType: dataType,
                length: typeInfo.length,
                precision: typeInfo.precision,
                scale: typeInfo.scale,
                collation: typeInfo.collation,
                colName: colName,
                udtInfo: udtInfo
            )
            column.encryption = encryption
            colData.append(column)
        }

        return TDSTokens.ColMetadataToken(colData: colData)
    }

    /// TYPE_INFO: the type byte, its length, collation, precision and scale, and a CLR type's names.
    internal static func readTypeInfo(
        from buffer: inout ByteBuffer
    ) throws -> (TDSTokens.ColMetadataToken.ColumnData.TypeInfo, TDSTokens.ColMetadataToken.ColumnData.UDTInfo?) {
        guard let dataTypeByte: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
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
        case .char, .varchar, .binary, .varbinary, .nchar, .nvarchar, .clrUdt, .json, .vector:
            guard let len: UInt16 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
            length = Int32(len)
        case .xml:
            // XML TYPE_INFO: no TYPE_VARLEN prefix — read SchemaPresent and optional schema.
            guard let schemaPresent: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
            if schemaPresent != 0 {
                // Consume DB name (B_VARCHAR: 1-byte char count + UTF16 chars)
                guard let dbLen: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
                if dbLen > 0 {
                    guard buffer.readUTF16String(length: Int(dbLen) * 2) != nil else { throw TDSError.needMoreData }
                }
                // Consume owning schema name
                guard let schLen: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
                if schLen > 0 {
                    guard buffer.readUTF16String(length: Int(schLen) * 2) != nil else { throw TDSError.needMoreData }
                }
                // Consume type/collection name (US_VARCHAR: 2-byte char count + UTF16 chars)
                guard let typeLen: UInt16 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
                if typeLen > 0 {
                    guard buffer.readUTF16String(length: Int(typeLen) * 2) != nil else { throw TDSError.needMoreData }
                }
            }
            length = -1 // PLP — no fixed length
        case .text, .nText, .image:
            guard let len: Int32 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
            length = len
        case .sqlVariant:
            guard let len: Int32 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
            length = len
        }

        // Capture collation bytes for string types (5 bytes: LCID + ColFlags + SortId)
        var collation: [UInt8] = []
        if dataType == .varchar || dataType == .char || dataType == .nvarchar || dataType == .nchar || dataType == .text || dataType == .nText {
            guard let bytes = buffer.readBytes(length: 5) else { throw TDSError.needMoreData }
            collation = bytes
        }
        
        var precision: UInt8 = 0
        var scale: UInt8 = 0
        var udtInfo: TDSTokens.ColMetadataToken.ColumnData.UDTInfo?
        if dataType == .decimal || dataType == .numeric || dataType == .decimalLegacy || dataType == .numericLegacy {
            guard let p: UInt8 = buffer.readInteger(), let sc: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
            precision = p
            scale = sc
        } else if dataType == .time || dataType == .datetime2 || dataType == .datetimeOffset {
            guard let sc: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
            scale = sc
        }

        if dataType == .clrUdt {
            udtInfo = try consumeUDTTypeInfo(from: &buffer)
        }
        return (.init(dataType: dataType, length: length, precision: precision, scale: scale, collation: collation), udtInfo)
    }

    /// One column encryption key's metadata per entry (MS-TDS 2.2.7.4 EK_INFO); a key may have
    /// several encrypted values (one per column master key), the first names the master key.
    internal struct CekEntry {
        var keyStoreName: String?
        var keyPath: String?
    }

    internal static func readCekTable(from buffer: inout ByteBuffer) throws -> [CekEntry] {
        guard let count: UInt16 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
        var entries: [CekEntry] = []
        for _ in 0..<count {
            // DatabaseId, CekId, CekVersion (ULONG each) and CekMDVersion (8 bytes).
            guard buffer.readSlice(length: 4 + 4 + 4 + 8) != nil,
                  let valueCount: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
            var entry = CekEntry()
            for index in 0..<valueCount {
                guard let keyLength: UInt16 = buffer.readInteger(endianness: .little),
                      buffer.readSlice(length: Int(keyLength)) != nil,
                      let storeChars: UInt8 = buffer.readInteger(),
                      let store = buffer.readUTF16String(length: Int(storeChars) * 2),
                      let pathChars: UInt16 = buffer.readInteger(endianness: .little),
                      let path = buffer.readUTF16String(length: Int(pathChars) * 2),
                      let algorithmChars: UInt8 = buffer.readInteger(),
                      buffer.readUTF16String(length: Int(algorithmChars) * 2) != nil else { throw TDSError.needMoreData }
                if index == 0 {
                    entry.keyStoreName = store
                    entry.keyPath = path
                }
            }
            entries.append(entry)
        }
        return entries
    }

    /// CryptoMetaData: the CekTable ordinal, the plaintext type and the encryption algorithm.
    internal static func readCryptoMetadata(
        from buffer: inout ByteBuffer,
        cekTable: [CekEntry]
    ) throws -> TDSTokens.ColMetadataToken.ColumnData.ColumnEncryption {
        guard let ordinal: UInt16 = buffer.readInteger(endianness: .little),
              let baseUserType: UInt32 = buffer.readInteger(endianness: .little) else { throw TDSError.needMoreData }
        let (baseType, _) = try readTypeInfo(from: &buffer)
        guard let algorithmId: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
        var algorithm: String
        switch algorithmId {
        case 0:
            guard let chars: UInt8 = buffer.readInteger(),
                  let name = buffer.readUTF16String(length: Int(chars) * 2) else { throw TDSError.needMoreData }
            algorithm = name
        case 1: algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512"
        case 2: algorithm = "AEAD_AES_256_CBC_HMAC_SHA_256"
        default: algorithm = "algorithm \(algorithmId)"
        }
        guard let kind: UInt8 = buffer.readInteger(),
              let normalization: UInt8 = buffer.readInteger() else { throw TDSError.needMoreData }
        let key = Int(ordinal) < cekTable.count ? cekTable[Int(ordinal)] : nil
        return .init(
            baseType: baseType,
            baseUserType: baseUserType,
            algorithm: algorithm,
            kind: .init(rawValue: kind),
            normalizationVersion: normalization,
            keyStoreName: key?.keyStoreName,
            keyPath: key?.keyPath
        )
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
