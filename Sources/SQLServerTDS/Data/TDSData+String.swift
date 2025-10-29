extension TDSData {
    public init(string: String) {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf16.count)
        buffer.writeUTF16String(string)
        self.init(metadata: String.tdsMetadata, value: buffer)
    }
    
    public var string: String? {
        if self.metadata.dataType == .sqlVariant {
            if let resolved = self.sqlVariantResolved(), let s = resolved.string {
                return s
            }
            // Fallback: attempt manual parse of sql_variant header and decode remaining payload
            if var payload = self.value, payload.readableBytes >= 2 {
                // Skip base type + property length + property bytes
                _ = payload.readInteger(as: UInt8.self)
                if let propLen = payload.readInteger(as: UInt8.self) {
                    if propLen > 0, payload.readableBytes >= Int(propLen) {
                        _ = payload.readSlice(length: Int(propLen))
                    }
                    let remain = payload.readableBytes
                    if remain > 0 {
                        if remain % 2 == 0, let u16 = payload.readUTF16String(length: remain) {
                            return u16
                        }
                        if let bytes = payload.readBytes(length: remain), let u8 = String(bytes: bytes, encoding: .utf8) {
                            return u8
                        }
                    }
                }
            }
            return nil
        }
        guard var value = self.value else {
            return nil
        }
        
        // TODO
        switch self.metadata.dataType {
        case .charLegacy, .varcharLegacy, .char, .varchar, .text:
            guard let bytes = value.readBytes(length: value.readableBytes) else {
                return nil
            }
            if bytes.isEmpty {
                return ""
            }
            if let utf8 = String(bytes: bytes, encoding: .utf8) {
                return utf8
            }
            if let cp1252 = String(bytes: bytes, encoding: .windowsCP1252) {
                return cp1252
            }
            return String(decoding: bytes, as: UTF8.self)
        case .nvarchar, .nchar, .nText:
            return value.readUTF16String(length: value.readableBytes)
        default:
            // Best-effort fallback: try to decode any remaining bytes as textual.
            if let bytes = value.readBytes(length: value.readableBytes), !bytes.isEmpty {
                // Try UTF-16LE full buffer
                if bytes.count % 2 == 0, let u16 = String(bytes: bytes, encoding: .utf16LittleEndian) {
                    return u16
                }
                // Try UTF-16LE skipping a 2-byte length prefix (common for length-prefixed payloads)
                if bytes.count > 2, (bytes.count - 2) % 2 == 0,
                   let u16 = String(bytes: Array(bytes.dropFirst(2)), encoding: .utf16LittleEndian) {
                    return u16
                }
                // Try UTF-16LE skipping a 1-byte length prefix (some sql_variant short cases)
                if bytes.count > 1, (bytes.count - 1) % 2 == 0,
                   let u16 = String(bytes: Array(bytes.dropFirst(1)), encoding: .utf16LittleEndian) {
                    return u16
                }
                // Try UTF-8
                if let u8 = String(bytes: bytes, encoding: .utf8) {
                    return u8
                }
                // Try Windows-1252
                if let cp1252 = String(bytes: bytes, encoding: .windowsCP1252) {
                    return cp1252
                }
            }
            return nil
        }
    }
}

extension TDSData: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(string: value)
    }
}

extension String: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .varchar)
    }
    
    public init?(tdsData: TDSData) {
        guard let string = tdsData.string else {
            return nil
        }
        self = string
    }

    public var tdsData: TDSData? {
        return .init(string: self)
    }
}
