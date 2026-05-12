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
            // TDS spec distinguishes:
            //   ENCRYPT_OFF      → "I can encrypt, I just don't want to"
            //   ENCRYPT_NOT_SUP  → "I cannot encrypt at all"
            // SSMS sends ENCRYPT_OFF in Optional mode so a server that
            // requires TLS (ENCRYPT_REQ) can still upgrade us. We do the
            // same whenever TLS infrastructure is available. Only when no
            // TLSConfiguration is supplied do we honestly advertise
            // ENCRYPT_NOT_SUP — and that path will fail against any server
            // that mandates encryption.
            self.clientEncryption = hasTLSConfiguration ? .encryptOff : .encryptNotSup
        }
    }

    func log(to logger: Logger) {
        logger.debug("Sending Prelogin message (encryption mode: \(encryptionMode)).")
    }

    var packetType: TDSPacket.HeaderType { .prelogin }

    func serialize(into buffer: inout ByteBuffer) throws {
        try TDSMessages.PreloginMessage(version: "9.0.0", encryption: clientEncryption, fedAuthRequired: fedAuthRequired).serialize(into: &buffer)
    }

    func handle(dataStream: ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        var mutableDataStream = dataStream
        accumulatedData.writeBuffer(&mutableDataStream)

        if accumulatedData.readableBytes >= 8 {
            var dataCopy = accumulatedData
            guard let parsedMessage = try? TDSMessages.PreloginResponse.parse(from: &dataCopy) else {
                return .continue
            }

            let serverEncryption = parsedMessage.encryption
            return try negotiateEncryption(server: serverEncryption)
        }

        return .continue
    }

    private func negotiateEncryption(server: TDSMessages.PreloginEncryption) throws -> TDSPacketResponse {
        switch encryptionMode {
        case .mandatory, .strict:
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
            // Per TDS spec negotiation matrix. Client may send:
            //   ENCRYPT_OFF      — has TLS, doesn't insist (SSMS Optional default)
            //   ENCRYPT_ON       — explicit request (legacy path)
            //   ENCRYPT_NOT_SUP  — no TLS infrastructure available
            switch (server, clientEncryption) {
            // Any combination where either side wants/needs TLS and the other can do it.
            case (.encryptReq, .encryptOn),       (.encryptReq, .encryptOff),
                 (.encryptOn,  .encryptOn),       (.encryptOn,  .encryptOff),
                 (.encryptOff, .encryptOn),       (.encryptOff, .encryptOff),
                 (.encryptClientCertOn, .encryptOn),  (.encryptClientCertOn, .encryptOff),
                 (.encryptClientCertReq, .encryptOn), (.encryptClientCertReq, .encryptOff):
                return .kickoffSSL
            // Server can't encrypt and client didn't insist — plain TDS.
            case (.encryptNotSup, .encryptNotSup),
                 (.encryptNotSup, .encryptOn),
                 (.encryptNotSup, .encryptOff):
                return .done
            // Client has no TLS but server only offers/forces it — fall back to plain
            // when server merely supports it, fail when server requires it.
            case (.encryptOff, .encryptNotSup),
                 (.encryptOn,  .encryptNotSup):
                return .done
            case (.encryptReq, .encryptNotSup),
                 (.encryptClientCertReq, .encryptNotSup):
                throw TDSError.protocolError("PRELOGIN Error: Server requires encryption but client has no TLS configuration. Provide a TLSConfiguration or use a different encryption mode.")
            default:
                throw TDSError.protocolError("PRELOGIN Error: Incompatible client/server encryption configuration. Client: \(clientEncryption), Server: \(server)")
            }
        }
    }
}
