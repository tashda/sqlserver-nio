import Foundation
import NIOCore

/// Writes the BulkLoadBCP payload: COLMETADATA for the destination columns (as the server described
/// them, from a `SELECT TOP 0`), ROW tokens with each value in its column's wire format, and DONE.
///
/// Values are given as the column type's raw payload (for example 4 little-endian bytes for `int`,
/// UTF-16LE for `nvarchar`); the writer adds the length prefix or PLP framing the type needs. `nil`
/// writes NULL.
public struct TDSBulkLoadWriter {
    public let columns: [TDSColumnMetadata]
    public private(set) var buffer: ByteBuffer
    public private(set) var rowCount = 0

    /// - Parameter columnEncryption: the connection negotiated COLUMNENCRYPTION, so COLMETADATA
    ///   carries a CekTable (empty: the client sends no encrypted values).
    public init(columns: [TDSColumnMetadata], columnEncryption: Bool = false, allocator: ByteBufferAllocator = ByteBufferAllocator()) {
        self.columns = columns
        self.buffer = allocator.buffer(capacity: 64 * 1024)
        Self.writeColMetadata(columns, columnEncryption: columnEncryption, into: &buffer)
    }

    /// Whether the bulk load can carry this column type. Legacy LOBs need text pointers, and
    /// `sql_variant` and CLR types need values the client cannot produce from plain values.
    public static func supports(_ column: TDSColumnMetadata) -> Bool {
        // An Always Encrypted column described as such needs values encrypted with its key.
        guard column.encryption == nil else { return false }
        switch column.dataType {
        case .text, .nText, .image, .sqlVariant, .clrUdt, .null, .json, .vector:
            return false
        default:
            return true
        }
    }

    public mutating func appendRow(_ values: [[UInt8]?]) throws {
        guard values.count == columns.count else {
            throw TDSError.protocolError("Bulk load row has \(values.count) values for \(columns.count) columns")
        }
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        for (column, value) in zip(columns, values) {
            try Self.writeValue(value, for: column, into: &buffer)
        }
        rowCount += 1
    }

    /// The finished payload: the rows so far and a DONE token.
    public func finished() -> ByteBuffer {
        var result = buffer
        result.writeInteger(TDSTokens.TokenType.done.rawValue)
        result.writeInteger(UInt16(0x0010), endianness: .little) // DONE_COUNT
        result.writeInteger(UInt16(0), endianness: .little)      // CurCmd
        result.writeInteger(UInt64(rowCount), endianness: .little)
        return result
    }

    // MARK: - COLMETADATA

    static func writeColMetadata(_ columns: [TDSColumnMetadata], columnEncryption: Bool = false, into buffer: inout ByteBuffer) {
        buffer.writeInteger(TDSTokens.TokenType.colMetadata.rawValue)
        buffer.writeInteger(UInt16(columns.count), endianness: .little)
        if columnEncryption {
            buffer.writeInteger(UInt16(0), endianness: .little) // CekTable: no keys
        }
        for column in columns {
            buffer.writeInteger(column.userType, endianness: .little)
            buffer.writeInteger(column.flags & ~TDSTokens.ColMetadataToken.ColumnData.encryptedFlag, endianness: .little)
            writeTypeInfo(column, into: &buffer)
            let name = Array(column.colName.utf16)
            buffer.writeInteger(UInt8(min(name.count, 128)))
            for unit in name.prefix(128) { buffer.writeInteger(unit, endianness: .little) }
        }
    }

    /// TYPE_INFO, the mirror of the COLMETADATA parser.
    static func writeTypeInfo(_ column: TDSColumnMetadata, into buffer: inout ByteBuffer) {
        if column.dataType == .xml {
            // The server refuses the xml type in bulk-load metadata; like SqlClient, xml is sent as
            // nvarchar(max) without a collation (the value is UTF-16 either way).
            buffer.writeInteger(TDSDataType.nvarchar.rawValue)
            buffer.writeInteger(UInt16(0xFFFF), endianness: .little)
            buffer.writeBytes([0, 0, 0, 0, 0])
            return
        }
        buffer.writeInteger(column.dataType.rawValue)
        switch column.dataType {
        case .tinyInt, .bit, .smallInt, .int, .real, .smallMoney, .smallDateTime, .bigInt, .float, .money, .datetime, .date, .null:
            break
        case .guid, .intn, .floatn, .moneyn, .datetimen, .bitn, .decimal, .decimalLegacy, .numeric, .numericLegacy,
             .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
            buffer.writeInteger(UInt8(truncatingIfNeeded: column.length))
        case .time, .datetime2, .datetimeOffset:
            break // the scale follows below
        case .char, .varchar, .binary, .varbinary, .nchar, .nvarchar, .clrUdt, .json, .vector:
            buffer.writeInteger(UInt16(truncatingIfNeeded: column.length), endianness: .little)
        case .xml:
            break // written as nvarchar(max) above
        case .text, .nText, .image, .sqlVariant:
            buffer.writeInteger(column.length, endianness: .little)
        }
        switch column.dataType {
        case .varchar, .char, .nvarchar, .nchar, .text, .nText:
            buffer.writeBytes(column.collation.count == 5 ? column.collation : [0, 0, 0, 0, 0])
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            buffer.writeInteger(column.precision)
            buffer.writeInteger(column.scale)
        case .time, .datetime2, .datetimeOffset:
            buffer.writeInteger(column.scale)
        default:
            break
        }
    }

    // MARK: - Values

    static func writeValue(_ value: [UInt8]?, for column: TDSColumnMetadata, into buffer: inout ByteBuffer) throws {
        switch column.dataType {
        case .tinyInt, .bit, .smallInt, .int, .real, .smallMoney, .smallDateTime, .bigInt, .float, .money, .datetime:
            // Fixed-length types cannot be NULL (the server describes nullable columns as INTN and so on).
            guard let value else { throw TDSError.protocolError("NULL for the NOT NULL fixed-length column \(column.colName)") }
            buffer.writeBytes(value)
        case .guid, .intn, .floatn, .moneyn, .datetimen, .bitn, .decimal, .decimalLegacy, .numeric, .numericLegacy,
             .date, .time, .datetime2, .datetimeOffset:
            // BYTELEN: 0 is NULL.
            guard let value else { buffer.writeInteger(UInt8(0)); return }
            buffer.writeInteger(UInt8(value.count))
            buffer.writeBytes(value)
        case .char, .varchar, .binary, .varbinary, .nchar, .nvarchar, .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
            if column.length == 0xFFFF || column.length == -1 {
                writePLP(value, into: &buffer)
            } else if column.dataType == .charLegacy || column.dataType == .varcharLegacy
                        || column.dataType == .binaryLegacy || column.dataType == .varbinaryLegacy {
                guard let value else { buffer.writeInteger(UInt8(0xFF)); return }
                buffer.writeInteger(UInt8(value.count))
                buffer.writeBytes(value)
            } else {
                guard let value else { buffer.writeInteger(UInt16(0xFFFF), endianness: .little); return }
                guard value.count <= Int(column.length) else {
                    throw TDSError.protocolError("Value of \(value.count) bytes is too long for column \(column.colName) (\(column.length) bytes)")
                }
                buffer.writeInteger(UInt16(value.count), endianness: .little)
                buffer.writeBytes(value)
            }
        case .xml:
            writePLP(value, into: &buffer)
        case .text, .nText, .image, .sqlVariant, .clrUdt, .null, .json, .vector:
            throw TDSError.protocolError("Bulk load does not support column \(column.colName) of type \(column.dataType)")
        }
    }

    static func writePLP(_ value: [UInt8]?, into buffer: inout ByteBuffer) {
        guard let value else {
            buffer.writeInteger(UInt64.max, endianness: .little) // PLP_NULL
            return
        }
        // PLP_UNKNOWN_LEN: SQL Server's bulk load stops with "premature end-of-message" when the
        // total length is given, and reads the same chunks with an unknown length.
        buffer.writeInteger(UInt64.max - 1, endianness: .little)
        if !value.isEmpty {
            buffer.writeInteger(UInt32(value.count), endianness: .little)
            buffer.writeBytes(value)
        }
        buffer.writeInteger(UInt32(0), endianness: .little) // PLP_TERMINATOR
    }
}
