import SQLServerTDS

public struct SQLServerRow: Sendable {
    internal let base: TDSRow

    internal init(base: TDSRow) {
        self.base = base
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
    public var typeName: String { dataType.name }
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

    internal var tdsAuthentication: TDSAuthentication {
        switch self {
        case .sqlPassword(let username, let password):
            return .sqlPassword(username: username, password: password)
        case .windowsIntegrated(let username, let password, let domain):
            return .windowsIntegrated(username: username, password: password, domain: domain)
        }
    }
}
