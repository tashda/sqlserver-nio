import Logging
import NIO
import Foundation

extension TDSConnection {
    public func rawSql(_ sqlText: String) -> EventLoopFuture<[TDSRow]> {
        var rows: [TDSRow] = []
        return rawSql(sqlText, onRow: { rows.append($0) }).map { rows }
    }
    
    public func rawSql(_ sqlText: String, onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(
            sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sqlText),
            logger: logger,
            onRow: onRow
        )
        return self.send(request, logger: logger)
    }


    func query(_ message: TDSMessages.RawSqlBatchMessage, _ onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: message, logger: logger, onRow: onRow)
        return self.send(request, logger: logger)
    }
}

public final class RawSqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessages.RawSqlBatchMessage
    var onRow: ((TDSRow) throws -> ())?
    var onMetadata: ((TDSTokens.ColMetadataToken) -> Void)?
    var onDone: ((TDSTokens.DoneToken) -> Void)?
    var onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)?
    var rowLookupTable: TDSRow.LookupTable?

    private let logger: Logger
    private let tokenParser: TDSTokenParser
    private var expectMoreResults: Bool = false
    private var finalDoneHasArrived: Bool = false

    public init(
        sqlBatch: TDSMessages.RawSqlBatchMessage,
        logger: Logger,
        onRow: ((TDSRow) throws -> ())? = nil,
        onMetadata: ((TDSTokens.ColMetadataToken) -> Void)? = nil,
        onDone: ((TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    ) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onDone = onDone
        self.onMessage = onMessage
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    public func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let parsedTokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
        try handleParsedTokens(parsedTokens)

        if expectMoreResults {
            return .continue
        }

        if finalDoneHasArrived {
            finalDoneHasArrived = false
            return .done
        }

        return .continue
    }

    public func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let textLength = sqlBatch.sqlText.utf16.count * 2
        let packets = try TDSMessage(payload: sqlBatch, allocator: allocator).packets
        let packetSummaries = packets.map { packet -> String in
            let length = packet.buffer.readableBytes
            let packetId = packet.header.packetId
            let status = packet.header.status.value
            let headerLength = packet.header.length
            return "length=\(length), packetId=\(packetId), status=\(status), headerLength=\(headerLength)"
        }
        let summaryString = packetSummaries.joined(separator: "; ")
        logger.trace("TDS RawSql sending batch text length: \(textLength) bytes, packets: [\(summaryString)]")
        return packets
    }

    public func log(to logger: Logger) {

    }
    
    func handleParsedTokens(_ tokens: [TDSToken]) throws {
        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in tokens {
            switch token.type {
            case .row:
                expectMoreResults = false
                guard let rowToken = token as? TDSTokens.RowToken else {
                    throw TDSError.protocolError("Error while reading row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
                if let onRow {
                    try onRow(row)
                }
            case .nbcRow:
                expectMoreResults = false
                guard let nbcRowToken = token as? TDSTokens.NbcRowToken else {
                    throw TDSError.protocolError("Error while reading NBC row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let synthesized = TDSTokens.RowToken(colData: nbcRowToken.colData)
                let row = TDSRow(dataRow: synthesized, lookupTable: rowLookupTable)
                if let onRow {
                    try onRow(row)
                }
            case .colMetadata:
                expectMoreResults = false
                guard let colMetadataToken = token as? TDSTokens.ColMetadataToken else {
                    throw TDSError.protocolError("Error reading column metadata token.")
                }
                rowLookupTable = TDSRow.LookupTable(colMetadata: colMetadataToken)
                onMetadata?(colMetadataToken)
            case .colInfo:
                break
            case .order:
                guard let _ = token as? TDSTokens.OrderToken else {
                    throw TDSError.protocolError("Error reading ORDER token.")
                }
            case .tabName:
                break
            case .returnStatus:
                if let returnStatusToken = token as? TDSTokens.ReturnStatusToken {
                    _ = returnStatusToken.value
                } else {
                    throw TDSError.protocolError("Error reading RETURNSTATUS token.")
                }
            case .done, .doneInProc, .doneProc:
                guard let doneToken = token as? TDSTokens.DoneToken else { continue }
                let moreResults = (doneToken.status & 0x01) != 0
                if moreResults {
                    expectMoreResults = true
                    finalDoneHasArrived = false
                    rowLookupTable = nil
                } else {
                    expectMoreResults = false
                    finalDoneHasArrived = true
                }
                onDone?(doneToken)
            case .error:
                if let errorToken = token as? TDSTokens.ErrorInfoToken {
                    logger.error("TDS RawSql error \(errorToken.number) [state=\(errorToken.state) class=\(errorToken.classValue)] \(errorToken.messageText)")
                    onMessage?(errorToken, true)
                } else {
                    logger.error("TDS RawSql encountered unknown ERROR token")
                }
            case .info:
                if let infoToken = token as? TDSTokens.ErrorInfoToken {
                    onMessage?(infoToken, false)
                }
            case .envchange:
                break
            default:
                break
            }
        }
    }
}
