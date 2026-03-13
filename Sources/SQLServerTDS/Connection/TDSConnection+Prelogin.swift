import Logging
import NIO
import Foundation

extension TDSConnection {
    internal func prelogin(encryptionMode: TDSEncryptionMode, hasTLSConfiguration: Bool) -> EventLoopFuture<Void> {
        let auth = PreloginRequest(encryptionMode: encryptionMode, hasTLSConfiguration: hasTLSConfiguration)
        return self.send(auth, logger: logger)
    }
}

// MARK: Private

internal final class PreloginRequest: TDSRequest {
    private let clientEncryption: TDSMessages.PreloginEncryption
    private let encryptionMode: TDSEncryptionMode

    private var accumulatedData = ByteBuffer()

    public let onRow: (@Sendable (TDSRow) -> Void)? = nil
    public let onMetadata: (@Sendable ([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? = nil
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil
    public let onEnvChange: (@Sendable (TDSTokens.EnvchangeToken<[Byte]>) -> Void)? = nil
    public let stream: Bool = false
    public let onData: (@Sendable (TDSData) -> Void)? = nil

    init(encryptionMode: TDSEncryptionMode, hasTLSConfiguration: Bool) {
        self.encryptionMode = encryptionMode
        switch encryptionMode {
        case .mandatory, .strict:
            // Signal that we require encryption
            self.clientEncryption = .encryptOn
        case .optional:
            // Signal based on whether we have a TLS config
            self.clientEncryption = hasTLSConfiguration ? .encryptOn : .encryptNotSup
        }
    }

    func log(to logger: Logger) {
        logger.debug("Sending Prelogin message (encryption mode: \(encryptionMode)).")
    }

    var packetType: TDSPacket.HeaderType { .prelogin }

    func serialize(into buffer: inout ByteBuffer) throws {
        try TDSMessages.PreloginMessage(version: "9.0.0", encryption: clientEncryption).serialize(into: &buffer)
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
            switch (server, clientEncryption) {
            case (.encryptReq, .encryptOn),
                 (.encryptOn, .encryptOn),
                 (.encryptClientCertOn, .encryptOn),
                 (.encryptClientCertReq, .encryptOn):
                return .kickoffSSL
            case (.encryptNotSup, .encryptNotSup),
                 (.encryptOff, .encryptNotSup):
                return .done
            case (.encryptNotSup, .encryptOn),
                 (.encryptOff, .encryptOn):
                // Server doesn't support encryption — optional mode allows fallback
                return .done
            default:
                throw TDSError.protocolError("PRELOGIN Error: Incompatible client/server encryption configuration. Client: \(clientEncryption), Server: \(server)")
            }
        }
    }
}
