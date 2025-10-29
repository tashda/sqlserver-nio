import Foundation

extension TDSTokenParser {
    // Hex preview helper for ring diagnostics
    private static func hexPreview(_ buffer: ByteBuffer, count: Int = 16) -> String {
        var copy = buffer
        let n = min(count, copy.readableBytes)
        guard let bytes = copy.readBytes(length: n) else { return "" }
        return bytes.map { String(format: "%02x", $0) }.joined()
    }

    static func parseRowToken(
        from buffer: inout ByteBuffer,
        with colMetadata: TDSTokens.ColMetadataToken,
        ring: TDSTokenRing? = nil
    ) throws -> TDSTokens.RowToken {
        var colData: [TDSTokens.RowToken.ColumnData] = []
        colData.reserveCapacity(Int(colMetadata.count))

        for (idx, column) in colMetadata.colData.enumerated() {
            ring?.record("row/col=\(idx) type=\(column.dataType) len=\(column.length) before=\(buffer.readableBytes) peek=\(Self.hexPreview(buffer))")
            let beforeBytes = buffer.readableBytes
            let value = try readColumnValue(for: column, from: &buffer, ring: ring)
            let afterBytes = buffer.readableBytes
            let consumed = beforeBytes - afterBytes
            ring?.record("row/col=\(idx) consumed=\(consumed) after=\(afterBytes) nil=\(value == nil)")
            colData.append(
                TDSTokens.RowToken.ColumnData(
                    textPointer: [],
                    timestamp: [],
                    data: value
                )
            )
        }
        // Diagnostic: record remaining bytes after a full row decode (via global logger ring)
        // We can't access the parser instance here; TDSTokenParser emits per-token ring records.
        return TDSTokens.RowToken(colData: colData)
    }

    static func parseNbcRowToken(
        from buffer: inout ByteBuffer,
        with colMetadata: TDSTokens.ColMetadataToken,
        ring: TDSTokenRing? = nil
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
            ring?.record("nbcRow/col=\(index) type=\(column.dataType) len=\(column.length) before=\(buffer.readableBytes) peek=\(Self.hexPreview(buffer))")
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

            let beforeBytes = buffer.readableBytes
            let value = try readColumnValue(for: column, from: &buffer, ring: ring)
            let afterBytes = buffer.readableBytes
            let consumed = beforeBytes - afterBytes
            ring?.record("nbcRow/col=\(index) consumed=\(consumed) after=\(afterBytes) nil=\(value == nil)")
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

    static func parseTVPRowToken(
        from buffer: inout ByteBuffer,
        with colMetadata: TDSTokens.ColMetadataToken,
        ring: TDSTokenRing? = nil
    ) throws -> TDSTokens.TVPRowToken {
        // TVP_ROW follows NBCROW-like encoding with null bitmap then column values
        let nbc = try parseNbcRowToken(from: &buffer, with: colMetadata, ring: ring)
        return TDSTokens.TVPRowToken(nullBitmap: nbc.nullBitmap, colData: nbc.colData)
    }

    static func readColumnValue(
        for column: TDSTokens.ColMetadataToken.ColumnData,
        from buffer: inout ByteBuffer,
        ring: TDSTokenRing? = nil
    ) throws -> ByteBuffer? {
        switch column.dataType {
        case .sqlVariant:
            // Row-level sql_variant value layout per TDS: [LONGLEN UInt32 totalLength] followed by
            // exactly `totalLength` bytes of payload: baseType(1), propLen(1), properties(propLen), value(..)
            // We verify structure minimally to avoid hanging on inconsistent lengths.
            guard buffer.readableBytes >= MemoryLayout<UInt32>.size else {
                throw TDSError.needMoreData
            }
            var probe = buffer // work on a copy to avoid partially consuming on failure
            guard let totalLength = probe.readInteger(endianness: .little, as: UInt32.self) else {
                throw TDSError.protocolError("Failed to read SQLVARIANT total length")
            }
            // Per TDS, NULL sql_variant is encoded as totalLength = 0xFFFFFFFF. Some servers/components
            // have been observed to emit 0 for an effectively NULL/empty variant in catalog views.
            // In either case, we must consume the 4-byte length field to keep the stream aligned.
            if totalLength == 0xFFFF_FFFF || totalLength == 0 {
                _ = buffer.readInteger(endianness: .little, as: UInt32.self) as UInt32?
                return nil
            }
            let total = Int(totalLength)
            guard probe.readableBytes >= total else {
                // Not enough bytes yet for the full payload
                // Heuristic guard to avoid infinite waits on clearly-invalid lengths
                if total > probe.readableBytes + 8192 {
                    throw TDSError.protocolError("Invalid SQLVARIANT length=\(total) with only \(probe.readableBytes) bytes available")
                }
                ring?.record("sqlvariant total=\(total) need_more remaining=\(probe.readableBytes)")
                throw TDSError.needMoreData
            }
            // Optionally sanity-check header (baseType + propLen + properties) before committing
            guard let baseType = probe.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            guard let propLen = probe.readInteger(as: UInt8.self) else { throw TDSError.needMoreData }
            if propLen > 0 {
                guard probe.readableBytes >= Int(propLen) else { throw TDSError.needMoreData }
                _ = probe.readSlice(length: Int(propLen))
            }
            ring?.record("sqlvariant base=0x\(String(baseType, radix: 16)) props=\(propLen) valueBytes=\(total - 2 - Int(propLen)) avail=\(probe.readableBytes)")
            // Remaining bytes constitute the value; no further validation here
            // Now commit: consume 4 + total bytes from the original buffer and return the payload slice
            _ = buffer.readInteger(endianness: .little, as: UInt32.self) as UInt32?
            guard let payload = buffer.readSlice(length: total) else {
                throw TDSError.protocolError("Failed to read SQLVARIANT payload bytes")
            }
            return payload
            
        case .xml, .json, .vector:
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

        // BIGCHAR/BIGNCHAR/BIGBINARY in TDS row data use a USHORT length prefix just like BIGVARCHAR/BIGVARBINARY.
        // Treat them as variable-length here to preserve proper alignment.
        // (Legacy types remain handled in the varlen path below.)
        

        // Variable-length character/binary types: prefer 2-byte USHORTCHARBINLEN, but fall back to 1-byte length
        // if the 2-byte value is implausible for the remaining buffer or exceeds the column max length. This matches
        // observed server behavior across versions and avoids misalignment when legacy encodings appear.
        case .varchar, .nvarchar, .varbinary, .char, .nchar, .binary,
             .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
            let available = buffer.readableBytes
            guard available >= 1 else { throw TDSError.needMoreData }

            // Debug override to help isolate length prefix behavior:
            //  - TDS_FORCE_VARLEN_MODE=1 -> force 1-byte first
            //  - TDS_FORCE_VARLEN_MODE=2 -> force 2-byte only
            let forceMode = ProcessInfo.processInfo.environment["TDS_FORCE_VARLEN_MODE"]
            let mode = forceMode.flatMap { Int($0) } ?? 0
            if mode != 0 { ring?.record("varlen: forceMode=\(mode)") }

            // Helper closures
            func readLen8() throws -> ByteBuffer? {
                guard let len8 = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self) else { throw TDSError.needMoreData }
                let v8 = Int(len8)
                ring?.record("varlen1: len8=\(v8) avail=\(available) max=\(column.length)")
                if v8 == 0xFF { _ = buffer.readByte(); return nil }
                guard v8 <= (buffer.readableBytes - 1) else { throw TDSError.needMoreData }
                _ = buffer.readByte()
                guard let data = buffer.readSlice(length: v8) else {
                    throw TDSError.protocolError("Error while reading varlen (8)")
                }
                return data
            }
            func readLen16() throws -> ByteBuffer? {
                guard buffer.readableBytes >= 2 else { throw TDSError.needMoreData }
                guard let len16 = buffer.getInteger(at: buffer.readerIndex, endianness: .little, as: UInt16.self) else { throw TDSError.needMoreData }
                let v16 = Int(len16)
                ring?.record("varlen2: len16=\(v16) avail=\(available) max=\(column.length)")
                if len16 == 0xFFFF { _ = buffer.readUShortCharBinLen(); return nil }
                guard v16 <= (buffer.readableBytes - 2) else { throw TDSError.needMoreData }
                _ = buffer.readUShortCharBinLen()
                guard let data = buffer.readSlice(length: v16) else {
                    throw TDSError.protocolError("Error while reading varlen (16)")
                }
                return data
            }

            switch column.dataType {
            case .char, .nchar, .binary:
                // BIGCHAR/BIGNCHAR/BIGBINARY: 2-byte prefix only
                if mode == 1 { // forced 1-byte (debug)
                    if let data = try readLen8() { return data }
                    // readLen8() can only return non-nil or throw; fall through
                    throw TDSError.needMoreData
                }
                if let data = try readLen16() { return data }
                // readLen16() may return nil for NULL sentinel (0xFFFF)
                return nil
            case .varchar, .nvarchar, .varbinary:
                // BIGVAR types: PLP when maxLen==0xFFFF, otherwise 2-byte prefix
                if column.length == 0xFFFF {
                    guard let plpData = try buffer.readPLPBytes() else { return nil }
                    return plpData
                }
                if mode == 1 { // forced 1-byte (debug)
                    if let data = try readLen8() { return data }
                    // readLen8() may return nil for NULL sentinel (0xFF)
                    return nil
                }
                if let data = try readLen16() { return data }
                // readLen16() may return nil for NULL sentinel (0xFFFF)
                return nil
            case .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
                // Legacy types: 1-byte prefix only
                if mode == 2 { // forced 2-byte (debug)
                    if let data = try readLen16() { return data }
                    // readLen16() may return nil for NULL sentinel (0xFFFF)
                    return nil
                }
                if let data = try readLen8() { return data }
                // readLen8() may return nil for NULL sentinel (0xFF)
                return nil
            default:
                // Should not reach here under this case set
                ring?.record("varlen: unexpected type=\(column.dataType)")
                throw TDSError.protocolError("Unexpected varlen type dispatch")
            }

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
            // GUIDTYPE in TDS belongs to the BYTELEN class. Although its payload is
            // always 16 bytes when non-null, servers still prefix the value with a
            // 1-byte length (0x10) in ROW/NBCROW streams. Previously we read a fixed
            // 16 bytes here, which left the 1-byte length in the buffer and produced
            // values starting with 0x10 (e.g. "10aaaa...") and stream misalignment.
            //
            // Strategy:
            // - Prefer BYTELEN decoding: read 1 byte. If 0xFF -> NULL; if 0x10 -> read 16 bytes.
            // - If the next byte is neither 0xFF nor 0x10, fall back defensively:
            //   * If there are at least 16 bytes remaining and the column declares maxLen=16,
            //     assume a legacy/fixed layout and read 16 bytes (no prefix).
            //   * Otherwise, treat the byte as a length and try to consume that many bytes
            //     to keep the stream aligned, or ask for more data if insufficient.
            guard buffer.readableBytes >= 1 else { throw TDSError.needMoreData }
            guard let len8 = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self) else {
                throw TDSError.needMoreData
            }

            if len8 == 0xFF {
                // BYTELEN NULL sentinel
                _ = buffer.readByte()
                return nil
            }

            if len8 == 0x10 {
                // Canonical non-null GUID: consume 1 + 16 bytes
                guard buffer.readableBytes >= 1 + 16 else { throw TDSError.needMoreData }
                _ = buffer.readByte()
                guard let data = buffer.readSlice(length: 16) else {
                    throw TDSError.protocolError("Error while reading GUID(16) payload.")
                }
                return data
            }

            // Fallbacks
            let avail = buffer.readableBytes
            if avail >= 16 {
                // Some environments may omit the BYTELEN prefix; consume fixed 16 bytes.
                // Record diagnostic to aid future tightening.
                ring?.record("guid: no len prefix; assuming fixed16 avail=\(avail)")
                guard let data = buffer.readSlice(length: 16) else {
                    throw TDSError.protocolError("Error while reading GUID fixed16 payload.")
                }
                return data
            }

            // Treat unknown len8 as a claimed length to avoid stalling the stream.
            let claimed = Int(len8)
            ring?.record("guid: unexpected len8=\(claimed) avail=\(avail); consuming as-length when possible")
            guard avail >= 1 + claimed else { throw TDSError.needMoreData }
            _ = buffer.readByte()
            guard let data = buffer.readSlice(length: claimed) else {
                throw TDSError.protocolError("Error while reading GUID payload (len=\(claimed)).")
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
            // Fallback: treat CLR UDT payload as VARBINARY with USHORTCHARBINLEN length prefix.
            // This keeps the stream aligned and allows callers to render a hex string instead of failing.
            guard buffer.readableBytes >= MemoryLayout<UShortCharBinLen>.size else {
                throw TDSError.needMoreData
            }
            guard let len = buffer.readUShortCharBinLen() else {
                throw TDSError.protocolError("Error while reading CLR UDT length.")
            }
            if len == 0xFFFF {
                return nil
            }
            let requiredBytes = Int(len)
            guard buffer.readableBytes >= requiredBytes else {
                throw TDSError.needMoreData
            }
            guard let data = buffer.readSlice(length: requiredBytes) else {
                throw TDSError.protocolError("Error while reading CLR UDT payload.")
            }
            return data

        @unknown default:
            throw TDSError.protocolError("Unhandled TDS data type: \(column.dataType)")
        }
    }
}
