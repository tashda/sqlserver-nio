
public struct TDSRow: CustomStringConvertible, @unchecked Sendable {
    final class LookupTable {
        let colMetadata: TDSTokens.ColMetadataToken

        struct Value {
            let index: Int
            let colData: TDSTokens.ColMetadataToken.ColumnData
        }

        private var _storage: [String: Value]?
        private var _caseInsensitiveStorage: [String: Value]?
        var storage: [String: Value] {
            if let existing = self._storage {
                return existing
            } else {
                let all = self.colMetadata.colData.enumerated().map { (index, colData) in
                    return (colData.colName, Value(index: index, colData: colData))
                }
                let storage = [String: Value](all) { a, b in
                    // take the first value
                    return a
                }
                self._storage = storage
                return storage
            }
        }

        var caseInsensitiveStorage: [String: Value] {
            if let existing = self._caseInsensitiveStorage {
                return existing
            } else {
                var values: [String: Value] = [:]
                values.reserveCapacity(self.colMetadata.colData.count)
                for (index, colData) in self.colMetadata.colData.enumerated() {
                    let lowercased = colData.colName.lowercased()
                    if values[lowercased] == nil {
                        values[lowercased] = Value(index: index, colData: colData)
                    }
                }
                self._caseInsensitiveStorage = values
                return values
            }
        }

        init(
            colMetadata: TDSTokens.ColMetadataToken
        ) {
            self.colMetadata = colMetadata
        }

        func lookup(column: String) -> Value? {
            if let value = self.storage[column] {
                return value
            } else {
                return self.caseInsensitiveStorage[column.lowercased()]
            }
        }
    }

    public var columnMetadata: TDSTokens.ColMetadataToken {
        self.lookupTable.colMetadata
    }

    let lookupTable: LookupTable
    private let values: [TDSData?]

    public func column(_ column: String) -> TDSData? {
        guard let entry = self.lookupTable.lookup(column: column) else {
            return nil
        }

        return values[entry.index]
    }

    public var description: String {
        var row: [String: TDSData?] = [:]
        for (index, col) in self.columnMetadata.colData.enumerated() {
            row[col.colName] = values[index]
        }
        return row.description
    }

    init(
        dataRow: TDSTokens.RowToken,
        lookupTable: LookupTable
    ) {
        self.lookupTable = lookupTable
        var computed: [TDSData?] = []
        computed.reserveCapacity(lookupTable.colMetadata.colData.count)
        for (index, metadata) in lookupTable.colMetadata.colData.enumerated() {
            if let cell = dataRow.colData[index].data {
                let base = TDSData(metadata: metadata, value: cell)
                if metadata.dataType == .sqlVariant {
                    if let resolved = base.sqlVariantResolved() {
                        computed.append(resolved)
                    } else {
                        // Fallback: minimally decode sql_variant textual NVARCHAR payload
                        var payload = cell
                        if let _ = payload.readInteger(as: UInt8.self),
                           let propLen = payload.readInteger(as: UInt8.self) {
                            if propLen > 0, payload.readableBytes >= Int(propLen) {
                                _ = payload.readSlice(length: Int(propLen))
                            }
                            if payload.readableBytes >= 2, let len = payload.readInteger(endianness: .little, as: UInt16.self) {
                                let remaining = payload.readableBytes
                                let bytesToRead = (Int(len) * 2 <= remaining) ? Int(len) * 2 : Int(len)
                                if bytesToRead > 0, let slice = payload.readSlice(length: bytesToRead) {
                                    let nvMeta = TypeMetadata(userType: 0, flags: 0, dataType: .nvarchar, collation: [], precision: nil, scale: nil)
                                    computed.append(TDSData(metadata: nvMeta, value: slice))
                                    continue
                                }
                            }
                        }
                        computed.append(base)
                    }
                } else {
                    computed.append(base)
                }
            } else {
                computed.append(nil)
            }
        }
        self.values = computed
    }
}
