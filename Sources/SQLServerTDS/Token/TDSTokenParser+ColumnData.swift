
import NIOCore

extension TDSTokenParser {
    internal func parseColumnData() throws -> TDSTokens.ColMetadataToken.ColumnData? {
        guard let userType = streamParser.readUInt16LE() else {
            throw TDSError.needMoreData
        }

        guard let flags = streamParser.readUInt16LE() else {
            throw TDSError.needMoreData
        }

        guard let type = streamParser.readUInt8(), let dataType = TDSDataType(rawValue: type) else {
            throw TDSError.protocolError("Invalid data type")
        }

        let colName = streamParser.readBVarChar() ?? ""
        let tableName = streamParser.readUsVarChar() ?? ""

        let dataLength: Int
        switch dataType {
        case .sqlVariant, .nText, .text, .image:
            dataLength = Int(streamParser.readUInt32LE() ?? 0)
        case .char, .varchar, .nchar, .nvarchar, .binary, .varbinary:
            dataLength = Int(streamParser.readUInt16LE() ?? 0)
        default:
            dataLength = Int(streamParser.readUInt8() ?? 0)
        }

        var collation: [Byte] = []
        if dataType.isCollationType() {
            collation = streamParser.readBytes(count: 5) ?? []
        }

        var precision: Int? = nil
        if dataType.isPrecisionType() {
            precision = Int(streamParser.readUInt8() ?? 0)
        }

        var scale: Int? = nil
        if dataType.isScaleType() {
            scale = Int(streamParser.readUInt8() ?? 0)
        }

        return TDSTokens.ColMetadataToken.ColumnData(
            userType: UInt32(userType),
            flags: flags,
            dataType: dataType,
            length: dataLength,
            collation: collation,
            tableName: tableName,
            colName: colName,
            precision: precision,
            scale: scale
        )
    }
}
