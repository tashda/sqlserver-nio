import Foundation
import NIOCore
import Logging

public class LoginRequest: TDSRequest, @unchecked Sendable {
    private let payload: TDSMessages.Login7Message

    /// Captured server error message from ErrorInfoToken during login.
    public internal(set) var serverErrorMessage: String?

    /// Authenticator for SSPI token exchange (nil for SQL password auth).
    /// Supports both Kerberos (via GSS.framework) and NTLMv2 (pure challenge-response).
    internal let authenticator: (any TDSAuthenticator)?

    /// Reference to the connection for sending SSPI response packets.
    internal weak var connection: TDSConnection?

    public let onRow: (@Sendable (TDSRow) -> Void)? = nil
    public let onMetadata: (@Sendable ([TDSColumnMetadata]) -> Void)? = nil
    public let onDone: (@Sendable (TDSTokens.DoneToken) -> Void)? = nil
    public let onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)?
    public let onReturnValue: (@Sendable (TDSTokens.ReturnValueToken) -> Void)? = nil
    public let stream: Bool = false // Login requests are always non-streaming
    public let onData: (@Sendable (TDSData) -> Void)? = nil // Login requests don't use onData

    public var packetType: TDSPacket.HeaderType { .tds7Login }

    public init(
        payload: TDSMessages.Login7Message,
        onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    ) {
        self.payload = payload
        self.authenticator = nil
        self.connection = nil
        self.onMessage = onMessage
    }

    internal init(
        payload: TDSMessages.Login7Message,
        authenticator: (any TDSAuthenticator)?,
        connection: TDSConnection?,
        onMessage: (@Sendable (TDSTokens.ErrorInfoToken, Bool) -> Void)? = nil
    ) {
        self.payload = payload
        self.authenticator = authenticator
        self.connection = connection
        self.onMessage = onMessage
    }

    public func log(to logger: Logger) {
        logger.debug("Sending LOGIN7 request")
    }

    public func serialize(into buffer: inout ByteBuffer) throws {
        try payload.serialize(into: &buffer)
    }
}
