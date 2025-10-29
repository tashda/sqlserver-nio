import NIO

private struct TDSSqlVariantComponents {
    var baseMetadata: TypeMetadata
    var value: ByteBuffer?
}

extension TDSData {
    private func decodeSqlVariantComponents() -> TDSSqlVariantComponents? {
        guard self.metadata.dataType == .sqlVariant else {
            return nil
        }
        guard var payload = self.value else {
            return TDSSqlVariantComponents(
                baseMetadata: TypeMetadata(dataType: .null),
                value: nil
            )
        }
        guard payload.readableBytes >= 2 else {
            return nil
        }
        guard
            let baseTypeRaw = payload.readInteger(as: UInt8.self),
            let baseType = TDSDataType(rawValue: baseTypeRaw),
            let propertyLength = payload.readInteger(as: UInt8.self)
        else {
            return nil
        }

        guard payload.readableBytes >= Int(propertyLength) else {
            return nil
        }

        var typeMetadata = TypeMetadata(dataType: baseType)

        if propertyLength > 0 {
            guard var properties = payload.readSlice(length: Int(propertyLength)) else {
                return nil
            }

            switch baseType {
            case .decimal, .decimalLegacy, .numeric, .numericLegacy:
                guard
                    let precisionByte = properties.readInteger(as: UInt8.self),
                    let scaleByte = properties.readInteger(as: UInt8.self)
                else {
                    return nil
                }
                typeMetadata.precision = Int(precisionByte)
                typeMetadata.scale = Int(scaleByte)

            case .datetime2, .datetimeOffset, .time:
                guard let scaleByte = properties.readInteger(as: UInt8.self) else {
                    return nil
                }
                typeMetadata.scale = Int(scaleByte)

            case .char, .varchar, .charLegacy, .varcharLegacy,
                 .nchar, .nvarchar, .text, .nText:
                guard let collation = properties.readBytes(length: 5) else {
                    return nil
                }
                typeMetadata.collation = collation
                // Remaining bytes encode max length; we do not currently surface it.
                if properties.readableBytes >= 2 {
                    _ = properties.readInteger(endianness: .little, as: UInt16.self)
                } else if properties.readableBytes == 1 {
                    _ = properties.readByte()
                }

            case .binary, .varbinary, .binaryLegacy, .varbinaryLegacy:
                if properties.readableBytes >= 2 {
                    _ = properties.readInteger(endianness: .little, as: UInt16.self)
                } else if properties.readableBytes == 1 {
                    _ = properties.readByte()
                }

            default:
                // Consume remaining property bytes if present.
                _ = properties.readBytes(length: properties.readableBytes)
            }
        }

        guard let value = Self.readSqlVariantValue(
            type: typeMetadata.dataType,
            metadata: typeMetadata,
            payload: &payload
        ) else {
            return TDSSqlVariantComponents(baseMetadata: typeMetadata, value: nil)
        }

        return TDSSqlVariantComponents(baseMetadata: typeMetadata, value: value)
    }

    private static func readSqlVariantValue(
        type: TDSDataType,
        metadata: TypeMetadata,
        payload: inout ByteBuffer
    ) -> ByteBuffer? {
        switch type {
        case .tinyInt, .bit:
            return payload.readSlice(length: 1)

        case .smallInt:
            return payload.readSlice(length: 2)

        case .int, .real, .smallMoney, .smallDateTime:
            return payload.readSlice(length: 4)

        case .bigInt, .float, .money, .datetime:
            return payload.readSlice(length: 8)

        case .date:
            return payload.readSlice(length: 3)

        case .guid:
            return payload.readSlice(length: 16)

        case .decimal, .decimalLegacy, .numeric, .numericLegacy,
             .datetime2, .datetimeOffset, .time:
            // Inside sql_variant these are stored without an inner length prefix; the
            // value is the entire remaining payload bytes.
            let bytes = payload.readableBytes
            if bytes == 0 { return nil }
            return payload.readSlice(length: bytes)
        case .intn, .floatn, .moneyn, .datetimen, .bitn:
            // Be defensive: some variant encodings include a 1-byte length; if not present,
            // consume the remaining bytes.
            if let length = payload.readInteger(as: UInt8.self) {
                if length == 0 { return nil }
                return payload.readSlice(length: Int(length))
            } else {
                let bytes = payload.readableBytes
                if bytes == 0 { return nil }
                return payload.readSlice(length: bytes)
            }

        case .char, .varchar, .charLegacy, .varcharLegacy,
             .binary, .varbinary, .binaryLegacy, .varbinaryLegacy:
            // Inside sql_variant, there is no inner length prefix for these types.
            let bytes = payload.readableBytes
            if bytes == 0 { return nil }
            return payload.readSlice(length: bytes)
        
        case .nchar, .nvarchar:
            // Inside sql_variant, NVARCHAR/NCHAR carry no inner length prefix; value is all remaining bytes.
            let bytes = payload.readableBytes
            if bytes == 0 { return nil }
            return payload.readSlice(length: bytes)

        case .xml, .image, .nText, .text, .sqlVariant, .clrUdt, .null, .json, .vector:
            return nil

        @unknown default:
            return nil
        }
    }

    internal func sqlVariantResolved() -> TDSData? {
        guard let components = self.decodeSqlVariantComponents() else {
            return nil
        }
        return TDSData(metadata: components.baseMetadata, value: components.value)
    }
}
