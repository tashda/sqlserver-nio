import Foundation
import SQLServerTDS
import NIOCore

/// Shared date formatter — ISO8601DateFormatter is expensive to create and should be reused.
/// Thread-safe: ISO8601DateFormatter is stateless after initialization.
nonisolated(unsafe) private let _sharedISO8601Formatter = ISO8601DateFormatter()

public struct SQLServerRow: Sendable {
    internal let base: TDSRow

    internal init(base: TDSRow) {
        self.base = base
    }

    /// Converts all column values to strings in a single pass without intermediate
    /// `[TDSData]` or `[SQLServerValue]` array allocations. Uses type-dispatched
    /// decoding (no cascade of failed type checks) and a cached date formatter.
    public func toStringArray() -> [String?] {
        let columnCount = base.columnMetadata.count
        var result: [String?] = []
        result.reserveCapacity(columnCount)
        for i in 0..<columnCount {
            guard i < base.columnData.count, let buffer = base.columnData[i].data else {
                result.append(nil)
                continue
            }
            result.append(Self.format(metadata: base.columnMetadata[i], buffer: buffer))
        }
        return result
    }

    /// The wire type of each column, for formatting spooled cells later with
    /// `SQLServerCellFormatter`.
    public var cellTypes: [SQLServerCellType] {
        base.columnMetadata.map(SQLServerCellType.init(metadata:))
    }

    /// Formats one cell. Shared by `toStringArray()` and
    /// `SQLServerCellFormatter`, so live and spooled cells cannot differ.
    /// Direct type dispatch avoids a cascade of failed type conversions.
    internal static func format(metadata: TDSTokens.ColMetadataToken.ColumnData, buffer: ByteBuffer) -> String? {
        let tdsData = TDSData(metadata: metadata, value: buffer)
        switch metadata.dataType {
        // String types — direct decode
        case .nvarchar, .nchar, .nText, .xml:
            return tdsData.string
        case .varchar, .varcharLegacy, .char, .text:
            return tdsData.string

        // Integer types — direct decode
        case .int:
            if let v = tdsData.int { return String(v) } else { return nil }
        case .bigInt:
            if let v = tdsData.int64 { return String(v) } else { return nil }
        case .smallInt:
            if let v = tdsData.int16 { return String(v) } else { return nil }
        case .tinyInt:
            if let v = tdsData.uint8 { return String(v) } else { return nil }
        case .intn:
            // Nullable integer — size determines width
            if let v = tdsData.int64 { return String(v) }
            else if let v = tdsData.int { return String(v) }
            else { return nil }

        // Bit
        case .bit, .bitn:
            if let v = tdsData.bool { return v ? "1" : "0" } else { return nil }

        // Float types
        case .float, .real, .floatn:
            if let v = tdsData.double { return String(v) } else { return nil }

        // Date/time types — use cached formatter
        case .datetime, .datetime2, .datetimen, .date, .smallDateTime:
            if let d = tdsData.date { return _sharedISO8601Formatter.string(from: d) }
            else { return nil }
        case .time:
            if let d = tdsData.date { return _sharedISO8601Formatter.string(from: d) }
            else if let s = tdsData.string { return s }
            else { return nil }
        case .datetimeOffset:
            if let d = tdsData.date { return _sharedISO8601Formatter.string(from: d) }
            else { return nil }

        // Decimal/money
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            if let d = tdsData.decimal { return NSDecimalNumber(decimal: d).stringValue }
            else { return nil }
        case .money, .smallMoney, .moneyn:
            if let d = tdsData.decimal { return NSDecimalNumber(decimal: d).stringValue }
            else if let v = tdsData.double { return String(v) }
            else { return nil }

        // UUID — use .string which applies the correct SQL Server mixed-endian byte swap
        case .guid:
            return tdsData.string

        // Binary
        case .varbinary, .varbinaryLegacy, .binary, .image:
            if let bytes = tdsData.bytes {
                let hex = bytes.map { String(format: "%02X", $0) }.joined()
                return "0x\(hex)"
            } else { return nil }

        // SQL Variant
        case .sqlVariant:
            if let s = tdsData.string { return s }
            else if let v = tdsData.int64 { return String(v) }
            else if let v = tdsData.double { return String(v) }
            else { return nil }

        default:
            // Unknown type / UDT — check hierarchyid/spatial, then string fallback
            if let udtInfo = metadata.udtInfo {
                let typeName = udtInfo.typeName
                if typeName.caseInsensitiveCompare("hierarchyid") == .orderedSame,
                   let bytes = tdsData.bytes,
                   let hid = SQLServerHierarchyID.string(from: bytes) {
                    return hid
                }
                if typeName.caseInsensitiveCompare("geometry") == .orderedSame ||
                    typeName.caseInsensitiveCompare("geography") == .orderedSame {
                    var spatialBuffer = buffer
                    if let spatial = SQLServerSpatial.decode(from: &spatialBuffer) {
                        return spatial.wkt
                    }
                }
            }

            if let s = tdsData.string { return s }
            else { return nil }
        }
    }

    /// Returns raw column ByteBuffers for zero-copy streaming. The caller stores
    /// these as binary row data and decodes to strings lazily at display time.
    /// This matches postgres-wire's approach of capturing ByteBuffer references
    /// in the streaming loop instead of converting to strings.
    public func rawColumnBuffers() -> (buffers: [ByteBuffer?], lengths: [Int], totalLength: Int) {
        let columnCount = base.columnMetadata.count
        var buffers: [ByteBuffer?] = []
        var lengths: [Int] = []
        var totalLength = 0
        buffers.reserveCapacity(columnCount)
        lengths.reserveCapacity(columnCount)

        for i in 0..<columnCount {
            if i < base.columnData.count, let buffer = base.columnData[i].data {
                let byteCount = buffer.readableBytes
                buffers.append(buffer)
                lengths.append(byteCount)
                totalLength += 5 + byteCount
            } else {
                buffers.append(nil)
                lengths.append(-1)
                totalLength += 1
            }
        }
        return (buffers, lengths, totalLength)
    }

    public func column(_ name: String) -> SQLServerValue? {
        base.column(name).map(SQLServerValue.init(base:))
    }

    public var columns: [SQLServerColumn] {
        base.columnMetadata.map(SQLServerColumn.init(base:))
    }

    public var columnMetadata: [SQLServerColumn] {
        columns
    }

    public var values: [SQLServerValue] {
        base.data.map(SQLServerValue.init(base:))
    }

    public var data: [SQLServerValue] {
        values
    }

    internal func droppingLastColumn() -> SQLServerRow {
        guard !base.columnMetadata.isEmpty, !base.columnData.isEmpty else {
            return self
        }
        return SQLServerRow(
            base: TDSRow(
                columnMetadata: Array(base.columnMetadata.dropLast()),
                columnData: Array(base.columnData.dropLast())
            )
        )
    }
}

public struct SQLServerColumn: Sendable {
    internal let base: TDSTokens.ColMetadataToken.ColumnData

    internal init(base: TDSTokens.ColMetadataToken.ColumnData) {
        self.base = base
    }

    public var name: String { base.colName }
    public var colName: String { name }
    public var dataType: SQLServerDataType { SQLServerDataType(base: base.dataType) }
    public var udtTypeName: String? { base.udtInfo?.typeName }
    public var typeName: String { udtTypeName ?? dataType.name }
    public var isNullable: Bool { (base.flags & 0x01) != 0 }
    public var maxLength: Int? { normalizedLength }
    public var length: Int { Int(base.length) }
    public var precision: Int? { base.precision == 0 ? nil : Int(base.precision) }
    public var scale: Int? { base.scale == 0 ? nil : Int(base.scale) }
    public var flags: UInt16 { base.flags }
    public var normalizedLength: Int? {
        guard base.length >= 0 else { return nil }
        switch base.dataType {
        case .nchar, .nvarchar, .nText:
            return Int(base.length) / 2
        default:
            return Int(base.length)
        }
    }
}

public struct SQLServerDataType: Sendable, Hashable, CustomStringConvertible {
    internal let base: TDSDataType

    internal init(base: TDSDataType) {
        self.base = base
    }

    public var rawValue: UInt8 { base.rawValue }
    public var name: String { String(describing: base) }
    public var description: String { name }
}

public enum SQLServerAuthentication: Sendable {
    case sqlPassword(username: String, password: String)
    case windowsIntegrated(username: String, password: String, domain: String?)
    /// Azure AD / Entra ID authentication with a pre-acquired OAuth2 access token (JWT).
    case accessToken(token: String)

    internal var tdsAuthentication: TDSAuthentication {
        switch self {
        case .sqlPassword(let username, let password):
            return .sqlPassword(username: username, password: password)
        case .windowsIntegrated(let username, let password, let domain):
            return .windowsIntegrated(username: username, password: password, domain: domain)
        case .accessToken(let token):
            return .accessToken(token: token)
        }
    }
}
