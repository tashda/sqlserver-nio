import Logging
import NIO
import Foundation

extension TDSConnection {
    public func rawSql(_ sqlText: String) -> EventLoopFuture<[TDSRow]> {
        var rows: [TDSRow] = []
        return rawSql(sqlText, onRow: { rows.append($0) }).map { rows }
    }
    
    public func rawSql(_ sqlText: String, onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: TDSMessages.RawSqlBatchMessage(sqlText: sqlText), logger: logger, onRow)
        return self.send(request, logger: logger)
    }


    func query(_ message: TDSMessages.RawSqlBatchMessage, _ onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: message, logger: logger, onRow)
        return self.send(request, logger: logger)
    }
}

class RawSqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessages.RawSqlBatchMessage
    var onRow: (TDSRow) throws -> ()
    var rowLookupTable: TDSRow.LookupTable?

    private let logger: Logger
    private let tokenParser: TDSTokenParser
    private var expectMoreResults: Bool = false
    private var finalDoneHasArrived: Bool = false

    init(sqlBatch: TDSMessages.RawSqlBatchMessage, logger: Logger, _ onRow: @escaping (TDSRow) throws -> ()) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let parsedTokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
        try handleParsedTokens(parsedTokens)

        if expectMoreResults {
            logger.debug("TDS RawSql awaiting additional result sets")
            return .continue
        }

        guard packet.header.status == .eom else {
            return .continue
        }

        if finalDoneHasArrived {
            logger.debug("TDS RawSql observed final DONE token; completing request")
            finalDoneHasArrived = false
            return .done
        }

        return .continue
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        return try TDSMessage(payload: sqlBatch, allocator: allocator).packets
    }

    func log(to logger: Logger) {

    }
    
    func handleParsedTokens(_ tokens: [TDSToken]) throws {
        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in tokens {
            logger.debug("TDS RawSql token received: \(token.type)")
            switch token.type {
            case .row:
                expectMoreResults = false
                logger.info("TDS RawSql received ROW token")
                guard let rowToken = token as? TDSTokens.RowToken else {
                    throw TDSError.protocolError("Error while reading row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
                logger.debug("Row data: \(row)")
                try onRow(row)
            case .nbcRow:
                expectMoreResults = false
                logger.info("TDS RawSql received NBCROW token")
                guard let nbcRowToken = token as? TDSTokens.NbcRowToken else {
                    throw TDSError.protocolError("Error while reading NBC row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let synthesized = TDSTokens.RowToken(colData: nbcRowToken.colData)
                let row = TDSRow(dataRow: synthesized, lookupTable: rowLookupTable)
                try onRow(row)
            case .colMetadata:
                expectMoreResults = false
                guard let colMetadataToken = token as? TDSTokens.ColMetadataToken else {
                    throw TDSError.protocolError("Error reading column metadata token.")
                }
                logger.info("TDS RawSql received COLMETADATA token with count \(colMetadataToken.count)")
                for column in colMetadataToken.colData {
                    logger.debug("Column \(column.colName) type=\(column.dataType) length=\(column.length)")
                }
                rowLookupTable = TDSRow.LookupTable(colMetadata: colMetadataToken)
            case .colInfo:
                logger.debug("TDS RawSql received COLINFO token")
            case .order:
                guard let orderToken = token as? TDSTokens.OrderToken else {
                    throw TDSError.protocolError("Error reading ORDER token.")
                }
                logger.info("TDS RawSql received ORDER token for ordinals \(orderToken.columnOrdinals)")
            case .tabName:
                logger.debug("TDS RawSql received TABNAME token")
            case .returnStatus:
                if let returnStatusToken = token as? TDSTokens.ReturnStatusToken {
                    logger.debug("TDS RawSql received RETURNSTATUS token value=\(returnStatusToken.value)")
                } else {
                    throw TDSError.protocolError("Error reading RETURNSTATUS token.")
                }
            case .done, .doneInProc, .doneProc:
                guard let doneToken = token as? TDSTokens.DoneToken else { continue }
                let moreResults = (doneToken.status & 0x01) != 0
                logger.info("TDS RawSql received DONE token status=\(doneToken.status) rowCount=\(doneToken.doneRowCount) more=\(moreResults)")
                if moreResults {
                    expectMoreResults = true
                    finalDoneHasArrived = false
                    rowLookupTable = nil
                } else {
                    expectMoreResults = false
                    finalDoneHasArrived = true
                }
            case .error:
                if let errorToken = token as? TDSTokens.ErrorInfoToken {
                    logger.error("TDS RawSql error \(errorToken.number) [state=\(errorToken.state) class=\(errorToken.classValue)] \(errorToken.messageText)")
                } else {
                    logger.error("TDS RawSql encountered unknown ERROR token")
                }
            case .info:
                if let infoToken = token as? TDSTokens.ErrorInfoToken {
                    logger.info("TDS RawSql info \(infoToken.number): \(infoToken.messageText)")
                } else {
                    logger.info("TDS RawSql received INFO token")
                }
            case .envchange:
                if let token = token as? TDSTokens.EnvchangeToken<String> {
                    logger.debug("TDS RawSql ENVCHANGE \(token.envchangeType) newValue=\(token.newValue)")
                } else if let token = token as? TDSTokens.EnvchangeToken<[Byte]> {
                    logger.debug("TDS RawSql ENVCHANGE \(token.envchangeType) (binary payload length \(token.newValue.count))")
                } else if let routing = token as? TDSTokens.RoutingEnvchangeToken {
                    logger.debug("TDS RawSql ENVCHANGE routing to \(routing.newValue.alternateServer):\(routing.newValue.port)")
                } else {
                    logger.debug("TDS RawSql received ENVCHANGE token")
                }
            default:
                logger.info("TDS RawSql encountered unhandled token \(token.type)")
            }
        }
    }
}
