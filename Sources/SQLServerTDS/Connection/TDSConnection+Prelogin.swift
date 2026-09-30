import Logging
import NIO
import Foundation

extension TDSConnection {
    internal func prelogin(encryptionMode: TDSEncryptionMode, hasTLSConfiguration: Bool, fedAuthRequired: Bool = false) -> EventLoopFuture<Void> {
        let auth = PreloginRequest(encryptionMode: encryptionMode, hasTLSConfiguration: hasTLSConfiguration, fedAuthRequired: fedAuthRequired)
        return self.send(auth, logger: logger)
    }
}

// MARK: Private

internal final class PreloginRequest: TDSRequest {
    private let clientEncryption: TDSMessages.PreloginEncryption
    private let encryptionMode: TDSEncryptionMode
    private let fedAuthRequired: Bool
    private var requestLogger: Logger?

    private var accumulatedData = ByteBuffer()

    public let onRow: (@Sendable (TDSRow) -> Void)? = nil
    public let onMetadata: (@Sendable ([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? = nil
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil
    public let onEnvChange: (@Sendable (TDSTokens.EnvchangeToken<[Byte]>) -> Void)? = nil
    public let stream: Bool = false
    public let onData: (@Sendable (TDSData) -> Void)? = nil

    init(encryptionMode: TDSEncryptionMode, hasTLSConfiguration: Bool, fedAuthRequired: Bool = false) {
        self.encryptionMode = encryptionMode
        self.fedAuthRequired = fedAuthRequired
        switch encryptionMode {
        case .mandatory, .strict:
            self.clientEncryption = .encryptOn
        case .optional:
            // LOGIN7 contains credentials. Until login-only TLS is supported,
            // Optional is kept as a full-session encryption compatibility alias.
            self.clientEncryption = .encryptOn
        }
    }

    func log(to logger: Logger) {
        requestLogger = logger
        logger.debug("Sending Prelogin message (encryption mode: \(encryptionMode)).")
    }

    var packetType: TDSPacket.HeaderType { .prelogin }

    func serialize(into buffer: inout ByteBuffer) throws {
        try TDSMessages.PreloginMessage(version: "9.0.0", encryption: clientEncryption, fedAuthRequired: fedAuthRequired).serialize(into: &buffer)
    }

    func handle(dataStream: ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        var mutableDataStream = dataStream
        accumulatedData.writeBuffer(&mutableDataStream)
        guard accumulatedData.readableBytes <= 128 * 1024 else {
            throw TDSError.protocolError("PRELOGIN response exceeds maximum supported size")
        }

        if accumulatedData.readableBytes >= 8 {
            var dataCopy = accumulatedData
            let parsedMessage: TDSMessages.PreloginResponse
            do {
                parsedMessage = try TDSMessages.PreloginResponse.parse(from: &dataCopy)
            } catch TDSError.needMoreData {
                return .continue
            }

            let serverEncryption = parsedMessage.encryption
            return try negotiateEncryption(server: serverEncryption)
        }

        return .continue
    }

    private func negotiateEncryption(server: TDSMessages.PreloginEncryption) throws -> TDSPacketResponse {
        requestLogger?.debug("PRELOGIN encryption client=\(clientEncryption) server=\(server)")
        switch encryptionMode {
        case .strict:
            // TDS 8.0 established TLS before PRELOGIN. The server ignores the
            // encryption option and there must be no second TLS handshake.
            return .done

        case .mandatory:
            // We require encryption — server must support it
            switch server {
            case .encryptOn, .encryptReq, .encryptClientCertOn, .encryptClientCertReq:
                return .kickoffSSL
            case .encryptNotSup, .encryptOff:
                throw TDSError.protocolError("PRELOGIN Error: Server does not support encryption but encryption mode is \(encryptionMode)")
            default:
                throw TDSError.protocolError("PRELOGIN Error: Unexpected server encryption response: \(server)")
            }

        case .optional:
            switch server {
            case .encryptReq, .encryptOn, .encryptClientCertOn, .encryptClientCertReq:
                return .kickoffSSL
            case .encryptOff, .encryptNotSup:
                throw TDSError.protocolError("PRELOGIN Error: Server did not negotiate full-session encryption")
            default:
                throw TDSError.protocolError("PRELOGIN Error: Unexpected server encryption response: \(server)")
            }
        }
    }
}
