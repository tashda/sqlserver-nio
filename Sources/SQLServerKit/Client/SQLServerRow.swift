import Foundation
import SQLServerTDS

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

            let metadata = base.columnMetadata[i]
            let tdsData = TDSData(metadata: metadata, value: buffer)

            // Direct type dispatch — avoids the cascade of failed type checks
            // that made the old approach (try .string, try .int, try .date...)
            // do 6+ failed property accesses per datetime cell.
            switch metadata.dataType {
            // String types — direct decode
            case .nvarchar, .nchar, .nText, .xml:
                result.append(tdsData.string)
            case .varchar, .varcharLegacy, .char, .text:
                result.append(tdsData.string)

            // Integer types — direct decode
            case .int:
                if let v = tdsData.int { result.append(String(v)) } else { result.append(nil) }
            case .bigInt:
                if let v = tdsData.int64 { result.append(String(v)) } else { result.append(nil) }
            case .smallInt:
                if let v = tdsData.int16 { result.append(String(v)) } else { result.append(nil) }
            case .tinyInt:
                if let v = tdsData.uint8 { result.append(String(v)) } else { result.append(nil) }
            case .intn:
                // Nullable integer — size determines width
                if let v = tdsData.int64 { result.append(String(v)) }
                else if let v = tdsData.int { result.append(String(v)) }
                else { result.append(nil) }

            // Bit
            case .bit, .bitn:
                if let v = tdsData.bool { result.append(v ? "1" : "0") } else { result.append(nil) }

            // Float types
            case .float, .real, .floatn:
                if let v = tdsData.double { result.append(String(v)) } else { result.append(nil) }

            // Date/time types — use cached formatter
            case .datetime, .datetime2, .datetimen, .date, .smallDateTime:
                if let d = tdsData.date { result.append(_sharedISO8601Formatter.string(from: d)) }
                else { result.append(nil) }
            case .time:
                if let d = tdsData.date { result.append(_sharedISO8601Formatter.string(from: d)) }
                else if let s = tdsData.string { result.append(s) }
                else { result.append(nil) }
            case .datetimeOffset:
                if let d = tdsData.date { result.append(_sharedISO8601Formatter.string(from: d)) }
                else { result.append(nil) }

            // Decimal/money
            case .decimal, .numeric, .decimalLegacy, .numericLegacy:
                if let d = tdsData.decimal { result.append(NSDecimalNumber(decimal: d).stringValue) }
                else { result.append(nil) }
            case .money, .smallMoney, .moneyn:
                if let d = tdsData.decimal { result.append(NSDecimalNumber(decimal: d).stringValue) }
                else if let v = tdsData.double { result.append(String(v)) }
                else { result.append(nil) }

            // UUID
            case .guid:
                if let u = tdsData.uuid { result.append(u.uuidString) } else { result.append(nil) }

            // Binary
            case .varbinary, .varbinaryLegacy, .binary, .image:
                if let bytes = tdsData.bytes {
                    let hex = bytes.map { String(format: "%02X", $0) }.joined()
                    result.append("0x\(hex)")
                } else { result.append(nil) }

            // SQL Variant
            case .sqlVariant:
                if let s = tdsData.string { result.append(s) }
                else if let v = tdsData.int64 { result.append(String(v)) }
                else if let v = tdsData.double { result.append(String(v)) }
                else { result.append(nil) }

            default:
                // Unknown type / UDT — check hierarchyid/spatial, then string fallback
                if let udtInfo = (metadata as? TDSTokens.ColMetadataToken.ColumnData)?.udtInfo {
                    let typeName = udtInfo.typeName
                    if typeName.caseInsensitiveCompare("hierarchyid") == .orderedSame,
                       let bytes = tdsData.bytes,
                       let hid = SQLServerHierarchyID.string(from: bytes) {
                        result.append(hid)
                        continue
                    }
                    if (typeName.caseInsensitiveCompare("geometry") == .orderedSame || 
                        typeName.caseInsensitiveCompare("geography") == .orderedSame),
                       var buf = base.columnData[i].data,
                       let spatial = SQLServerSpatial.decode(from: &buf) {
                        result.append(spatial.wkt)
                        continue
                    }
                }
                
                if let s = tdsData.string { result.append(s) }
                else { result.append(nil) }
            }
        }
        return result
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
