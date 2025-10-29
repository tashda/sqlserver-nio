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
            sqlBatch: TDSMessages.RawSqlBatchMessage(
                sqlText: sqlText, 
                transactionDescriptor: self.transactionDescriptor, 
                outstandingRequestCount: self.requestCount
            ),
            logger: logger,
            onRow: onRow,
            connection: self
        )
        return self.send(request, logger: logger)
    }


    func query(_ message: TDSMessages.RawSqlBatchMessage, _ onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RawSqlBatchRequest(sqlBatch: message, logger: logger, onRow: onRow, connection: self)
        return self.send(request, logger: logger)
    }
}

public final class RawSqlBatchRequest: TDSRequest {
    let sqlBatch: TDSMessages.RawSqlBatchMessage
    var onRow: ((TDSRow) throws -> ())?
    var onMetadata: ((TDSTokens.ColMetadataToken) -> Void)?
    var onDone: ((TDSTokens.DoneToken) -> Void)?
    var onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)?
    var onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)?
    var rowLookupTable: TDSRow.LookupTable?

    private let logger: Logger
    private let tokenParser: TDSTokenParser
    private var expectMoreResults: Bool = false
    private var finalDoneHasArrived: Bool = false
    private weak var connection: TDSConnection?
    private var stallTask: Scheduled<Void>?
    private var stallTimer: DispatchSourceTimer?
    private var lastActivityNanos: UInt64 = 0
    private var stallStartNanos: UInt64?
    private var stallEscalated: Bool = false
    // Only consider stalls after we have received at least one token for this request
    private var hasReceivedAnyToken: Bool = false
    // Coalesce stall snapshots to avoid noisy duplicate logs when nothing changes.
    private var lastStallHead: String?
    private var repeatedStallSnapshots: Int = 0
    // Hard cap: fail the request after N stall snapshots (2s cadence each)
    private let stallSnapshotLimit: Int = (ProcessInfo.processInfo.environment["TDS_STALL_SNAPSHOT_LIMIT"].flatMap { Int($0) }) ?? 3
    private var stallSnapshotCount: Int = 0

    public init(
        sqlBatch: TDSMessages.RawSqlBatchMessage,
        logger: Logger,
        onRow: ((TDSRow) throws -> ())? = nil,
        onMetadata: ((TDSTokens.ColMetadataToken) -> Void)? = nil,
        onDone: ((TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil,
        onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)? = nil,
        connection: TDSConnection? = nil
    ) {
        self.sqlBatch = sqlBatch
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onDone = onDone
        self.onMessage = onMessage
        self.onReturnValue = onReturnValue
        self.logger = logger
        self.tokenParser = TDSTokenParser(logger: logger, ring: connection?.tokenRing)
        self.connection = connection
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
        // Start a lightweight stall watchdog to aid diagnostics when a request appears to hang.
        if stallTimer == nil, let conn = connection {
            lastActivityNanos = NIODeadline.now().uptimeNanoseconds
            stallStartNanos = nil
            stallSnapshotCount = 0
            repeatedStallSnapshots = 0
            lastStallHead = nil
            let timer = DispatchSource.makeTimerSource(queue: DispatchQueue.global(qos: .utility))
            timer.schedule(deadline: .now() + .seconds(2), repeating: .seconds(2))
            timer.setEventHandler { [weak self] in self?.checkForStall() }
            timer.resume()
            stallTimer = timer
            // Ensure watchdog is torn down when the underlying channel closes
            conn.closeFuture.whenComplete { [weak self] _ in
                self?.stallTimer?.cancel(); self?.stallTimer = nil
                self?.stallTask?.cancel(); self?.stallTask = nil
            }
        }
        return packets
    }

    public func log(to logger: Logger) {

    }

    deinit {
        // Ensure any pending stall watchdog is cancelled to avoid leaking
        // scheduled promises when the underlying event loop shuts down.
        stallTimer?.cancel(); stallTimer = nil
        stallTask?.cancel(); stallTask = nil
    }
    
    private func handleTransactionEnvChange(_ envChangeToken: TDSTokens.EnvchangeToken<[Byte]>) {
        switch envChangeToken.envchangeType {
        case .beingTransaction:
            // Extract transaction descriptor from newValue (8 bytes) - exactly like Microsoft
            if envChangeToken.newValue.count >= 8 {
                let transactionDescriptor = Array(envChangeToken.newValue.prefix(8))
                connection?.updateTransactionState(descriptor: transactionDescriptor, requestCount: 1)
                let descriptorHex = transactionDescriptor.map { String(format: "%02x", $0) }.joined()
                logger.trace("TDS transaction started with descriptor: \(descriptorHex)")
            }
        case .commitTransaction, .rollbackTransaction, .transactionEnded:
            // Transaction ended, reset descriptor to all zeros like Microsoft
            connection?.updateTransactionState(descriptor: [0, 0, 0, 0, 0, 0, 0, 0], requestCount: 1)
            logger.trace("TDS transaction ended")
        default:
            break
        }
    }
    
    private func markActivity() {
        lastActivityNanos = NIODeadline.now().uptimeNanoseconds
        stallStartNanos = nil
        hasReceivedAnyToken = true
    }

    private func checkForStall() {
        guard let conn = connection, !conn.isClosed else {
            // Connection closed; stop watchdog to avoid interacting with a dead loop
            stallTimer?.cancel(); stallTimer = nil
            stallTask?.cancel(); stallTask = nil
            return
        }
        let now = NIODeadline.now().uptimeNanoseconds
        // If we haven't observed any tokens for > 2s and the request has not finalized,
        // log the token ring snapshot for diagnostics. Do not complete the request here.
        if !finalDoneHasArrived {
            // If we haven't started receiving tokens yet (likely queued behind another request),
            // do not treat lack of activity as a stall for this request.
            if !hasReceivedAnyToken { return }
            let elapsedNs = now &- lastActivityNanos
            if elapsedNs > 2_000_000_000 { // 2 seconds
                let snapshot = connection?.tokenRing.snapshot() ?? []
                let head = snapshot.first
                // Only emit the full snapshot if the head entry changed or we haven't logged recently.
                if head != lastStallHead || repeatedStallSnapshots < 1 {
                    logger.warning("TDS RawSql stall: no tokens for >2s; dumping token ring (most recent first):\n\(snapshot.joined(separator: "\n"))")
                    lastStallHead = head
                    repeatedStallSnapshots = 0
                } else {
                    // Compress repetitive noise to a single‑line heartbeat
                    repeatedStallSnapshots += 1
                    logger.warning("TDS RawSql stall: still stalled; ring head unchanged (suppressed duplicate snapshot)")
                }
                stallSnapshotCount += 1
                if stallSnapshotCount >= stallSnapshotLimit {
                    logger.error("TDS RawSql stall: exceeded \(stallSnapshotLimit) stall checks; failing current request immediately")
                    connection?.failActiveRequestTimeout()
                    stallTimer?.cancel(); stallTimer = nil
                    stallTask?.cancel(); stallTask = nil
                    return
                }
                // Record stall start if this is the first detection
                if stallStartNanos == nil { stallStartNanos = now }
                // Escalate by sending ATTENTION if we have been idle for > 5s since first stall.
                if let start = stallStartNanos, (now &- start) > 5_000_000_000, stallEscalated == false {
                    stallEscalated = true
                    logger.warning("TDS RawSql stall: escalating by sending ATTENTION to cancel current request")
                    connection?.sendAttention()
                }
                // As a last resort, if we have remained stalled for > 15s since first stall,
                // fail the current request promise with a timeout without closing the channel
                // to avoid triggering client-level retries due to connectionClosed.
                if let start = stallStartNanos, (now &- start) > 15_000_000_000 {
                    logger.error("TDS RawSql stall: failing current request with timeout after >15s without progress")
                    connection?.failActiveRequestTimeout()
                    stallTask?.cancel(); stallTask = nil
                    return
                }
                // Bump the activity timestamp to avoid spamming
                lastActivityNanos = now
            }
            // Repeating Dispatch timer continues running; no eventLoop reschedule required
        } else {
            stallTimer?.cancel(); stallTimer = nil
            stallTask?.cancel(); stallTask = nil
            lastStallHead = nil
            repeatedStallSnapshots = 0
        }
    }

    func handleParsedTokens(_ tokens: [TDSToken]) throws {
        // TODO: The following is an incomplete implementation of extracting data from rowTokens
        for token in tokens {
            markActivity()
            // Trace each token to help diagnose stalled responses
            switch token.type {
            case .colMetadata:
                if let t = token as? TDSTokens.ColMetadataToken { logger.trace("TDS token: colMetadata cols=\(t.colData.count)") } else { logger.trace("TDS token: colMetadata") }
            case .row:
                logger.trace("TDS token: row")
            case .nbcRow:
                logger.trace("TDS token: nbcRow")
            case .tvpRow:
                logger.trace("TDS token: tvpRow")
            case .done, .doneInProc, .doneProc:
                if let t = token as? TDSTokens.DoneToken {
                    let more = (t.status & 0x01) != 0
                    logger.trace("TDS token: done status=0x\(String(t.status, radix: 16)) moreResults=\(more) rowCount=\(t.doneRowCount)")
                } else {
                    logger.trace("TDS token: done")
                }
            case .error:
                logger.trace("TDS token: error")
            case .info:
                logger.trace("TDS token: info")
            case .envchange:
                logger.trace("TDS token: envchange")
            case .sessionState:
                logger.trace("TDS token: sessionState")
            case .dataClassification:
                logger.trace("TDS token: dataClassification")
            case .returnValue:
                logger.trace("TDS token: returnValue")
            default:
                logger.trace("TDS token: \(token.type)")
            }
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
            case .tvpRow:
                expectMoreResults = false
                guard let tvpRowToken = token as? TDSTokens.TVPRowToken else {
                    throw TDSError.protocolError("Error while reading TVP row results.")
                }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let synthesized = TDSTokens.RowToken(colData: tvpRowToken.colData)
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
                // Record a concise colMetadata snapshot into the token ring for diagnostics
                if let conn = self.connection {
                    let cols = colMetadataToken.colData.enumerated().map { (i, c) -> String in
                        let name = c.colName
                        let type = String(describing: c.dataType)
                        let len = c.length
                        return "#\(i) \(name): type=\(type) maxLen=\(len)"
                    }.joined(separator: " | ")
                    conn.tokenRing.record("colMetadata snapshot: \(cols)")
                }
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
                    logger.trace("TDS done: moreResults=true; awaiting next result set")
                } else {
                    expectMoreResults = false
                    finalDoneHasArrived = true
                    logger.trace("TDS done: final done observed; finishing request")
                    // Cancel stall watchdog immediately on final completion to
                    // prevent scheduled tasks from outliving the event loop.
                    stallTask?.cancel(); stallTask = nil
                    stallTimer?.cancel(); stallTimer = nil
                }
                onDone?(doneToken)
            case .error:
                if let errorToken = token as? TDSTokens.ErrorInfoToken {
                    logger.error("TDS RawSql error \(errorToken.number) [state=\(errorToken.state) class=\(errorToken.classValue)] \(errorToken.messageText)")
                    onMessage?(errorToken, true)
                } else {
                    logger.error("TDS RawSql encountered unknown ERROR token")
                }
                // On error, stop the watchdog as the request will complete.
                stallTask?.cancel()
                stallTask = nil
            case .info:
                if let infoToken = token as? TDSTokens.ErrorInfoToken {
                    onMessage?(infoToken, false)
                }
            case .envchange:
                if let envChangeToken = token as? TDSTokens.EnvchangeToken<[Byte]> {
                    handleTransactionEnvChange(envChangeToken)
                }
                break
            case .sessionState:
                if let ss = token as? TDSTokens.SessionStateToken {
                    var copy = ss.payload
                    let bytes = copy.readBytes(length: copy.readableBytes) ?? []
                    connection?.updateSessionState(payload: bytes)
                }
            case .dataClassification:
                if let dc = token as? TDSTokens.DataClassificationToken {
                    var copy = dc.payload
                    let bytes = copy.readBytes(length: copy.readableBytes) ?? []
                    connection?.updateDataClassification(payload: bytes)
                }
            case .returnValue:
                if let rv = token as? TDSTokens.ReturnValueToken {
                    onReturnValue?(rv)
                }
            default:
                break
            }
        }
    }
}
