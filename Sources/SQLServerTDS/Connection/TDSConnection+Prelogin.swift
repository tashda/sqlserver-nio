import Logging
import NIO
import Foundation

extension TDSConnection {
    internal func prelogin(shouldNegotiateEncryption: Bool) -> EventLoopFuture<Void> {
        let auth = PreloginRequest(shouldNegotiateEncryption)
        return self.send(auth, logger: logger)
    }
}

// MARK: Private

internal final class PreloginRequest: TDSRequest {
    private let clientEncryption: TDSMessages.PreloginEncryption

    private var accumulatedData = ByteBuffer()

    public let onRow: ((TDSRow) -> Void)? = nil
    public let onMetadata: (([TDSTokens.ColMetadataToken.ColumnData]) -> Void)? = nil
    public let onDone: ((TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: ((TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    public let onReturnValue: ((TDSTokens.ReturnValueToken) -> Void)? = nil
    public let stream: Bool = false // Prelogin requests are always non-streaming
    public let onData: ((TDSData) -> Void)? = nil // Prelogin requests don't use onData

    init(_ shouldNegotiateEncryption: Bool) {
        self.clientEncryption = shouldNegotiateEncryption ? .encryptOn : .encryptNotSup
    }

    func log(to logger: Logger) {
        logger.debug("Sending Prelogin message.")
    }

    func handle(dataStream: ByteBuffer, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        var mutableDataStream = dataStream
        accumulatedData.writeBuffer(&mutableDataStream)

        // Check if this appears to be complete prelogin data
        // Prelogin responses are typically small and fit in one packet
        if accumulatedData.readableBytes >= 8 {
            var dataCopy = accumulatedData
            guard let parsedMessage = try? TDSMessages.PreloginResponse.parse(from: &dataCopy) else {
                // Need more data for complete prelogin response
                return .continue
            }

            // Encryption Negotiation - Supports all or nothing encryption
            if let serverEncryption = parsedMessage.encryption {
                switch (serverEncryption, clientEncryption) {
                case (.encryptReq, .encryptOn),
                     (.encryptOn, .encryptOn):
                    // encrypt connection
                    return .kickoffSSL
                case (.encryptNotSup, .encryptNotSup):
                    // no encryption
                    return .done
                default:
                    throw TDSError.protocolError("PRELOGIN Error: Incompatible client/server encyption configuration. Client: \(clientEncryption), Server: \(serverEncryption)")
                }
            }
        }

        return .continue
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let prelogin = TDSMessages.PreloginMessage(version: "9.0.0", encryption: clientEncryption)
        let message = try TDSMessage(payload: prelogin, allocator: allocator)
        return message.packets
    }
}