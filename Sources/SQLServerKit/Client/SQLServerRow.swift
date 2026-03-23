import Foundation
import SQLServerTDS

/// Shared date formatter — ISO8601DateFormatter is expensive to create and should be reused.
private let _sharedISO8601Formatter = ISO8601DateFormatter()

public struct SQLServerRow: Sendable {
    internal let base: TDSRow

    internal init(base: TDSRow) {
        self.base = base
    }

    /// Converts all column values to strings in a single pass without intermediate
    /// `[TDSData]` or `[SQLServerValue]` array allocations. Uses a cached date
    /// formatter instead of allocating one per cell.
    public func toStringArray() -> [String?] {
        let columnCount = base.columnMetadata.count
        var result: [String?] = []
        result.reserveCapacity(columnCount)

        for i in 0..<columnCount {
            guard i < base.columnData.count, let buffer = base.columnData[i].data else {
                result.append(nil)
                continue
            }

            let tdsData = TDSData(metadata: base.columnMetadata[i], value: buffer)

            // Check hierarchyid UDT before generic string conversion
            if let udtInfo = (base.columnMetadata[i] as? TDSTokens.ColMetadataToken.ColumnData)?.udtInfo,
               udtInfo.typeName.caseInsensitiveCompare("hierarchyid") == .orderedSame,
               let bytes = tdsData.bytes,
               let hid = SQLServerHierarchyID.string(from: bytes) {
                result.append(hid)
                continue
            }

            // Fast type-specific conversion — ordered by frequency for typical queries
            if let string = tdsData.string { result.append(string); continue }
            if let int = tdsData.int { result.append(String(int)); continue }
            if let int64 = tdsData.int64 { result.append(String(int64)); continue }
            if let double = tdsData.double { result.append(String(double)); continue }
            if let bool = tdsData.bool { result.append(String(bool)); continue }
            if let date = tdsData.date {
                result.append(_sharedISO8601Formatter.string(from: date))
                continue
            }
            if let decimal = tdsData.decimal {
                result.append(NSDecimalNumber(decimal: decimal).stringValue)
                continue
            }
            if let uuid = tdsData.uuid { result.append(uuid.uuidString); continue }
            if let bytes = tdsData.bytes {
                let hex = bytes.map { String(format: "%02X", $0) }.joined()
                result.append("0x\(hex)")
                continue
            }
            result.append(nil)
        }
        return result
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
