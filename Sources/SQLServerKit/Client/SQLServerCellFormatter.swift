import Foundation
import NIOCore
import SQLServerTDS

/// The wire type of a result column: everything needed to turn one of its
/// cells back into a display string after the row itself is gone.
///
/// Applications that spool rows to disk store each cell's raw bytes
/// (`SQLServerRow.rawColumnBuffers()`) and the column's `SQLServerCellType`
/// (as its `encoded` string), and later format a cell with
/// `SQLServerCellFormatter`. The result is identical to
/// `SQLServerRow.toStringArray()` for the live row, so spooled rows read
/// exactly like rows that were never spooled. The collation is kept because
/// `varchar`, `char` and `text` bytes are in the column's code page, not UTF-8.
public struct SQLServerCellType: Sendable, Hashable {
    public let tdsType: UInt8
    public let length: Int32
    public let precision: UInt8
    public let scale: UInt8
    /// The 5-byte TDS collation of character columns, empty for other types.
    public let collation: [UInt8]
    /// The CLR type name for `hierarchyid`, `geometry`, `geography` and other
    /// user-defined types.
    public let udtTypeName: String?

    internal init(metadata: TDSTokens.ColMetadataToken.ColumnData) {
        self.tdsType = metadata.dataType.rawValue
        self.length = metadata.length
        self.precision = metadata.precision
        self.scale = metadata.scale
        self.collation = metadata.collation
        self.udtTypeName = metadata.udtInfo?.typeName
    }

    internal var metadata: TDSTokens.ColMetadataToken.ColumnData? {
        guard let dataType = TDSDataType(rawValue: tdsType) else { return nil }
        let udtInfo = udtTypeName.map {
            TDSTokens.ColMetadataToken.ColumnData.UDTInfo(databaseName: "", schemaName: "", typeName: $0, assemblyName: "")
        }
        return TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: dataType,
            length: length,
            precision: precision,
            scale: scale,
            collation: collation,
            colName: "",
            udtInfo: udtInfo
        )
    }

    private static let prefix = "mssql1"

    /// A compact, stable text form for storing alongside spooled data, for
    /// example `mssql1:e7:100:0:0:0904d00034:`.
    public var encoded: String {
        let collationHex = collation.map { String(format: "%02x", $0) }.joined()
        let udt = udtTypeName?.addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? ""
        return "\(Self.prefix):\(String(format: "%02x", tdsType)):\(length):\(precision):\(scale):\(collationHex):\(udt)"
    }

    /// Parses `encoded`. Returns nil for any other string, so it can also
    /// tell whether a stored type descriptor came from this driver.
    public init?(encoded: String) {
        let parts = encoded.split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 7, parts[0] == Self.prefix,
              let tdsType = UInt8(parts[1], radix: 16),
              let length = Int32(parts[2]),
              let precision = UInt8(parts[3]),
              let scale = UInt8(parts[4]),
              parts[5].count % 2 == 0 else { return nil }
        var collation: [UInt8] = []
        var index = parts[5].startIndex
        while index < parts[5].endIndex {
            let next = parts[5].index(index, offsetBy: 2)
            guard let byte = UInt8(parts[5][index..<next], radix: 16) else { return nil }
            collation.append(byte)
            index = next
        }
        self.tdsType = tdsType
        self.length = length
        self.precision = precision
        self.scale = scale
        self.collation = collation
        self.udtTypeName = parts[6].isEmpty ? nil : String(parts[6]).removingPercentEncoding
    }
}

/// Formats one stored cell exactly as `SQLServerRow.toStringArray()` formats
/// the same cell of a live row.
public enum SQLServerCellFormatter {
    /// `bytes` is the cell's value as returned in `rawColumnBuffers()` (no
    /// length prefix). A NULL cell has no bytes and is not passed here.
    public static func string(bytes: UnsafeRawBufferPointer, type: SQLServerCellType) -> String? {
        guard let metadata = type.metadata else { return nil }
        var buffer = ByteBufferAllocator().buffer(capacity: bytes.count)
        buffer.writeBytes(bytes)
        return SQLServerRow.format(metadata: metadata, buffer: buffer)
    }

    public static func string(data: Data, type: SQLServerCellType) -> String? {
        data.withUnsafeBytes { string(bytes: $0, type: type) }
    }
}
