import Foundation
import NIOCore
import Logging

/// Sends an SSPI (Security Support Provider Interface) response token
/// back to the server during Kerberos/SPNEGO authentication exchange.
///
/// This is sent as a TDS packet of type 0x11 (SSPI message) containing
/// the raw SPNEGO response token from the GSSAPI context negotiation.
public class SSPIRequest: TDSRequest, @unchecked Sendable {
    private let tokenData: Data

    public let onRow: (@Sendable (TDSRow) -> Void)? = nil
    public let onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)? = nil
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil
    public let stream: Bool = false
    public let onData: (@Sendable (TDSData) -> Void)? = nil

    public var packetType: TDSPacket.HeaderType { .sspi }

    public init(tokenData: Data) {
        self.tokenData = tokenData
    }

    public func log(to logger: Logger) {
        logger.debug("Sending SSPI response token (\(tokenData.count) bytes)")
    }

    public func serialize(into buffer: inout ByteBuffer) throws {
        buffer.writeBytes(tokenData)
    }
}
