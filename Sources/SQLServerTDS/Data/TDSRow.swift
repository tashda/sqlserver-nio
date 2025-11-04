
public struct TDSRow: Sendable {
    public var columnMetadata: [TDSTokens.ColMetadataToken.ColumnData]
    public var columnData: [TDSTokens.RowToken.ColumnData]

    public init(token: TDSTokens.RowToken, columns: [TDSTokens.ColMetadataToken.ColumnData]) {
        self.columnMetadata = columns
        self.columnData = token.colData
    }

    public func column(_ name: String) -> TDSData? {
        guard let index = columnMetadata.firstIndex(where: { $0.colName == name }) else {
            return nil
        }
        guard index < columnData.count else {
            return nil
        }
        guard let data = columnData[index].data else {
            return nil
        }
        return TDSData(metadata: columnMetadata[index], value: data)
    }

    public var data: [TDSData] {
        var result: [TDSData] = []
        for i in 0..<columnMetadata.count {
            if let data = columnData[i].data {
                result.append(TDSData(metadata: columnMetadata[i], value: data))
            }
        }
        return result
    }
}
