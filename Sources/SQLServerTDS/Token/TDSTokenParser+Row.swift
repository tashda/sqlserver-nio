import NIOCore

extension TDSTokenParser {
    public func parseRowToken() throws -> TDSTokens.RowToken? {
        guard let tokenType = streamParser.readUInt8(), tokenType == TDSTokens.TokenType.row.rawValue else {
            // This is not a ROW token, so we should reset the position and return nil
            streamParser.position -= 1
            return nil
        }

        guard let colMetadata = self.colMetadata else {
            throw TDSError.protocolError("No COLMETADATA received")
        }

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for columnMetadata in colMetadata.colData {
            let columnData = try parseColumnValue(for: columnMetadata)
            columns.append(TDSTokens.RowToken.ColumnData(textPointer: [], timestamp: [], data: columnData.value))
        }

        return TDSTokens.RowToken(colData: columns)
    }
}