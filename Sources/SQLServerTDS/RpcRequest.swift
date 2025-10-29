import Logging
import Foundation
import NIO

extension TDSConnection {
    public func rpc(_ procedureName: String, parameters: [TDSMessages.RpcParameter], onRow: @escaping (TDSRow) throws -> ()) -> EventLoopFuture<Void> {
        let request = RpcRequest(
            message: TDSMessages.RpcRequestMessage(
                procedureName: procedureName,
                parameters: parameters,
                transactionDescriptor: self.transactionDescriptor,
                outstandingRequestCount: self.requestCount
            ),
            logger: logger,
            onRow: onRow,
            connection: self
        )
        return self.send(request, logger: logger)
    }
}

public final class RpcRequest: TDSRequest {
    let message: TDSMessages.RpcRequestMessage
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

    public init(
        message: TDSMessages.RpcRequestMessage,
        logger: Logger,
        onRow: ((TDSRow) throws -> ())? = nil,
        onMetadata: ((TDSTokens.ColMetadataToken) -> Void)? = nil,
        onDone: ((TDSTokens.DoneToken) -> Void)? = nil,
        onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil,
        onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)? = nil,
        connection: TDSConnection? = nil
    ) {
        self.message = message
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
        let parsed = tokenParser.writeAndParseTokens(packet.messageBuffer)
        try handleParsedTokens(parsed)
        if expectMoreResults { return .continue }
        if finalDoneHasArrived { finalDoneHasArrived = false; return .done }
        return .continue
    }

    public func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let packets = try TDSMessage(payload: message, allocator: allocator).packets
        // Optional wire hex dump for diagnostics
        if ProcessInfo.processInfo.environment["TDS_LOG_RPC_HEX"] == "1" {
            func hexPreview(_ buf: ByteBuffer, limit: Int = 128) -> String {
                var copy = buf
                let n = min(limit, copy.readableBytes)
                guard let bytes = copy.readBytes(length: n) else { return "" }
                return bytes.map { String(format: "%02x", $0) }.joined()
            }
            func hexAll(_ buf: ByteBuffer) -> String {
                var copy = buf
                let n = copy.readableBytes
                guard let bytes = copy.readBytes(length: n) else { return "" }
                return bytes.map { String(format: "%02x", $0) }.joined()
            }
            if let conn = self.connection {
                conn.tokenRing.record("rpc out: proc=\(message.procedureName) params=\(message.parameters.count) packets=\(packets.count)")
                for (i, p) in packets.enumerated() {
                    let head = hexPreview(p.messageBuffer, limit: 96)
                    conn.tokenRing.record("rpc pkt#\(i+1) head=\(head)")
                    if ProcessInfo.processInfo.environment["TDS_LOG_RPC_HEX_FULL"] == "1" {
                        let full = hexAll(p.messageBuffer)
                        conn.tokenRing.record("rpc pkt#\(i+1) full=\(full)")
                    }
                }
            }
        }
        return packets
    }

    public func log(to logger: Logger) {}

    private func handleParsedTokens(_ tokens: [TDSToken]) throws {
        for token in tokens {
            switch token.type {
            case .row:
                expectMoreResults = false
                guard let rowToken = token as? TDSTokens.RowToken else { throw TDSError.protocolError("RPC row token invalid") }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = TDSRow(dataRow: rowToken, lookupTable: rowLookupTable)
                if let onRow { try onRow(row) }
            case .nbcRow:
                expectMoreResults = false
                guard let nb = token as? TDSTokens.NbcRowToken else { throw TDSError.protocolError("RPC nbc row token invalid") }
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = TDSRow(dataRow: TDSTokens.RowToken(colData: nb.colData), lookupTable: rowLookupTable)
                if let onRow { try onRow(row) }
            case .colMetadata:
                expectMoreResults = false
                guard let meta = token as? TDSTokens.ColMetadataToken else { throw TDSError.protocolError("RPC colmetadata invalid") }
                rowLookupTable = TDSRow.LookupTable(colMetadata: meta)
                onMetadata?(meta)
            case .order:
                _ = token as? TDSTokens.OrderToken
            case .tabName:
                _ = token as? TDSTokens.TabNameToken
            case .returnStatus:
                _ = token as? TDSTokens.ReturnStatusToken
            case .returnValue:
                if let rv = token as? TDSTokens.ReturnValueToken { onReturnValue?(rv) }
            case .done, .doneInProc, .doneProc:
                guard let done = token as? TDSTokens.DoneToken else { continue }
                let more = (done.status & 0x01) != 0
                if more { expectMoreResults = true; finalDoneHasArrived = false; rowLookupTable = nil } else { expectMoreResults = false; finalDoneHasArrived = true }
                onDone?(done)
            case .error:
                if let err = token as? TDSTokens.ErrorInfoToken {
                    onMessage?(err, true)
                    // Dump recent token ring to logs to aid diagnostics
                    if let conn = self.connection {
                        let tail = conn.tokenRing.snapshot().suffix(80).joined(separator: "\n")
                        logger.error("RPC error. Recent token ring:\n\(tail)")
                    }
                }
            case .info:
                if let info = token as? TDSTokens.ErrorInfoToken { onMessage?(info, false) }
            case .envchange:
                if let env = token as? TDSTokens.EnvchangeToken<[Byte]> {
                    handleTransactionEnvChange(env)
                }
                break
            default:
                break
            }
        }
    }

    private func handleTransactionEnvChange(_ envChangeToken: TDSTokens.EnvchangeToken<[Byte]>) {
        switch envChangeToken.envchangeType {
        case .beingTransaction:
            if envChangeToken.newValue.count >= 8 {
                let transactionDescriptor = Array(envChangeToken.newValue.prefix(8))
                connection?.updateTransactionState(descriptor: transactionDescriptor, requestCount: 1)
                let descriptorHex = transactionDescriptor.map { String(format: "%02x", $0) }.joined()
                logger.trace("TDS transaction started with descriptor: \(descriptorHex)")
            }
        case .commitTransaction, .rollbackTransaction, .transactionEnded:
            connection?.updateTransactionState(descriptor: [0,0,0,0,0,0,0,0], requestCount: 1)
            logger.trace("TDS transaction ended")
        default:
            break
        }
    }
}
