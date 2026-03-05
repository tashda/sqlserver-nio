import NIOCore

extension TDSTokenParser {
    public func parseRowToken() throws -> TDSTokens.RowToken? {
        // We need access to the logger, so let's read the logger from the main class
        let logger = Logger(label: "swift-tds")

        guard let tokenType = streamParser.readUInt8() else {
            // No data available, return nil without adjusting position
            return nil
        }

        guard tokenType == TDSTokens.TokenType.row.rawValue else {
            // This is not a ROW token, so we should reset the position and return nil
            streamParser.position -= 1
            return nil
        }

        guard let colMetadata = self.colMetadata else {
            logger.error("❌ parseRowToken: No COLMETADATA received")
            throw TDSError.protocolError("No COLMETADATA received")
        }

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for (_, columnMetadata) in colMetadata.colData.enumerated() {
            let columnData = try parseColumnValue(for: columnMetadata)
            columns.append(columnData)
        }
        return TDSTokens.RowToken(colData: columns)
    }
}
