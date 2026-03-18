import Foundation

/// Protocol for TDS authentication mechanisms (Kerberos, NTLMv2, etc.).
///
/// Conforming types implement a multi-step authentication exchange:
/// 1. `initialToken()` produces the first token to embed in the LOGIN7 SSPI field.
/// 2. The server responds with a challenge token (delivered via SSPI token 0xED).
/// 3. `continueAuthentication(serverToken:)` processes the challenge and returns a response.
/// 4. Steps 2-3 may repeat until authentication completes.
protocol TDSAuthenticator: Sendable {
    /// Generates the initial authentication token to include in the LOGIN7 SSPI field.
    func initialToken() throws -> Data

    /// Processes a server challenge token and returns the response.
    /// - Parameter serverToken: The SSPI token received from the server.
    /// - Returns: A tuple of (optional response data, completion flag).
    ///   When the response is non-nil, it should be sent back via SSPIRequest.
    ///   When the flag is `true`, authentication is complete with no further tokens.
    func continueAuthentication(serverToken: Data) throws -> (Data?, Bool)
}
