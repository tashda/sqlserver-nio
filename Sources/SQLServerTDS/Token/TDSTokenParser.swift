
import NIOCore
import Logging

public class TDSTokenParser {
    internal let streamParser: TDSStreamParser
    private let logger: Logger
    private var state: State = .expectingColMetadata
    internal var colMetadata: TDSTokens.ColMetadataToken?

    private enum State {
        case expectingColMetadata
        case expectingRow
        case expectingDone
    }

    public init(streamParser: TDSStreamParser, logger: Logger) {
        self.streamParser = streamParser
        self.logger = logger
    }

    public func parse() throws -> [TDSToken] {
        var tokens: [TDSToken] = []

        while true {
            switch state {
            case .expectingColMetadata:
                if let colMetadataToken = try parseColMetadataToken() {
                    self.colMetadata = colMetadataToken
                    tokens.append(colMetadataToken)
                    state = .expectingRow
                } else {
                    // This is not a COLMETADATA token, so we should move on to the next state
                    state = .expectingRow
                }
            case .expectingRow:
                if let rowToken = try parseRowToken() {
                    tokens.append(rowToken)
                } else {
                    // This is not a ROW token, so we should move on to the next state
                    state = .expectingDone
                }
            case .expectingDone:
                if let doneToken = try parseDoneToken() {
                    tokens.append(doneToken)
                    state = .expectingColMetadata
                } else {
                    // This is not a DONE token, so we should break out of the loop
                    break
                }
            }
        }

        return tokens
    }

    internal func parseColumnValue(for column: TDSTokens.ColMetadataToken.ColumnData) throws -> TDSData {
        switch column.dataType {
        case .intn:
            guard let length = streamParser.readUInt8() else {
                throw TDSError.needMoreData
            }

            switch length {
            case 1:
                let value = streamParser.readUInt8() ?? 0
                var buffer = ByteBufferAllocator().buffer(capacity: 1)
                buffer.writeInteger(value, as: UInt8.self)
                return TDSData(metadata: TypeMetadata(dataType: .int), value: buffer)
            case 2:
                let value = streamParser.readUInt16LE() ?? 0
                var buffer = ByteBufferAllocator().buffer(capacity: 2)
                buffer.writeInteger(value, as: UInt16.self)
                return TDSData(metadata: TypeMetadata(dataType: .int), value: buffer)
            case 4:
                let value = streamParser.readUInt32LE() ?? 0
                var buffer = ByteBufferAllocator().buffer(capacity: 4)
                buffer.writeInteger(value, as: UInt32.self)
                return TDSData(metadata: TypeMetadata(dataType: .int), value: buffer)
            case 8:
                let value = streamParser.readUInt64LE() ?? 0
                var buffer = ByteBufferAllocator().buffer(capacity: 8)
                buffer.writeInteger(value, as: UInt64.self)
                return TDSData(metadata: TypeMetadata(dataType: .int), value: buffer)
            default:
                return TDSData(metadata: TypeMetadata(dataType: .int), value: nil)
            }
        default:
            fatalError("Not implemented")
        }
    }
}
