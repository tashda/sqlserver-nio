import NIOCore

extension TDSTokenParser {
    public func parseRowToken() throws -> TDSTokens.RowToken? {
        // We need access to the logger, so let's read the logger from the main class
        let logger = Logger(label: "swift-tds")

        logger.debug("🔍 parseRowToken() called - position: \(streamParser.position), buffer readable: \(streamParser.buffer.readableBytes)")

        guard let tokenType = streamParser.readUInt8() else {
            logger.debug("❌ parseRowToken: No data available")
            // No data available, return nil without adjusting position
            return nil
        }

        logger.debug("🔍 parseRowToken: Read token type 0x\(String(tokenType, radix: 16))")

        guard tokenType == TDSTokens.TokenType.row.rawValue else {
            logger.debug("❌ parseRowToken: Not a ROW token (expected 0xD1, got 0x\(String(tokenType, radix: 16)))")
            // This is not a ROW token, so we should reset the position and return nil
            streamParser.position -= 1
            return nil
        }

        guard let colMetadata = self.colMetadata else {
            logger.error("❌ parseRowToken: No COLMETADATA received")
            throw TDSError.protocolError("No COLMETADATA received")
        }

        logger.debug("🔍 parseRowToken: Processing \(colMetadata.colData.count) columns")

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for (index, columnMetadata) in colMetadata.colData.enumerated() {
            logger.debug("🔍 parseRowToken: Parsing column \(index) of type \(columnMetadata.dataType)")
            let columnData = try parseColumnValue(for: columnMetadata)
            columns.append(columnData)
        }

        logger.info("✅ parseRowToken: Successfully parsed ROW with \(columns.count) columns")
        return TDSTokens.RowToken(colData: columns)
    }
}
