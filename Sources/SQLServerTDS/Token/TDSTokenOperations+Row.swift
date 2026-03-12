import NIOCore

extension TDSTokenOperations {
    internal func parseRowToken() throws -> TDSTokens.RowToken? {
        guard let colMetadata = self.colMetadata else {
            throw TDSError.protocolError("Received ROW token before COLMETADATA")
        }

        guard let tokenType = streamParser.readUInt8() else {
            throw TDSError.needMoreData
        }
        guard tokenType == TDSTokens.TokenType.row.rawValue else {
            throw TDSError.protocolError("Expected ROW token, found 0x\(String(format: "%02X", tokenType))")
        }

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for (_, columnMetadata) in colMetadata.colData.enumerated() {
            let columnData = try parseColumnValue(for: columnMetadata)
            columns.append(columnData)
        }

        return TDSTokens.RowToken(colMetadata: colMetadata.colData, colData: columns)
    }

    internal func parseNbcRowToken() throws -> TDSTokens.NbcRowToken? {
        guard let colMetadata = self.colMetadata else {
            throw TDSError.protocolError("Received NBCROW token before COLMETADATA")
        }

        guard let tokenType = streamParser.readUInt8() else {
            throw TDSError.needMoreData
        }
        guard tokenType == TDSTokens.TokenType.nbcRow.rawValue else {
            throw TDSError.protocolError("Expected NBCROW token, found 0x\(String(format: "%02X", tokenType))")
        }

        let nullBitmapBytes = (colMetadata.colData.count + 7) / 8
        guard let bitmap = streamParser.buffer.getBytes(at: streamParser.position, length: nullBitmapBytes) else {
            throw TDSError.needMoreData
        }
        streamParser.position += nullBitmapBytes

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for (index, columnMetadata) in colMetadata.colData.enumerated() {
            let isNull = (bitmap[index / 8] & (1 << (index % 8))) != 0
            if isNull {
                columns.append(TDSTokens.RowToken.ColumnData(textPointer: [], timestamp: [], data: nil))
            } else {
                let columnData = try parseColumnValue(for: columnMetadata)
                columns.append(columnData)
            }
        }

        return TDSTokens.NbcRowToken(nullBitmap: bitmap, colMetadata: colMetadata.colData, colData: columns)
    }

    internal func parseTVPRowToken() throws -> TDSTokens.TvpRowToken? {
        guard let colMetadata = self.colMetadata else {
            throw TDSError.protocolError("Received TVP_ROW token before COLMETADATA")
        }

        guard let tokenType = streamParser.readUInt8() else {
            throw TDSError.needMoreData
        }
        guard tokenType == TDSTokens.TokenType.tvpRow.rawValue else {
            throw TDSError.protocolError("Expected TVP_ROW token, found 0x\(String(format: "%02X", tokenType))")
        }

        let nullBitmapBytes = max(1, (colMetadata.colData.count + 7) / 8)
        guard let bitmap = streamParser.buffer.getBytes(at: streamParser.position, length: nullBitmapBytes) else {
            throw TDSError.needMoreData
        }
        streamParser.position += nullBitmapBytes

        var columns: [TDSTokens.RowToken.ColumnData] = []
        for (index, columnMetadata) in colMetadata.colData.enumerated() {
            let isNull = (bitmap[index / 8] & (1 << (index % 8))) != 0
            if isNull {
                columns.append(.init(data: nil))
            } else {
                columns.append(try parseColumnValue(for: columnMetadata))
            }
        }

        return TDSTokens.TvpRowToken(colMetadata: colMetadata.colData, colData: columns)
    }
}
