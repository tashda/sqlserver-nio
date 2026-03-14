import Foundation

public enum TDSAuthentication: Sendable {
    case sqlPassword(username: String, password: String)
    case windowsIntegrated(username: String, password: String, domain: String?)
    /// Azure AD / Entra ID authentication with a pre-acquired OAuth2 access token (JWT).
    case accessToken(token: String)
}

public struct TDSLoginConfiguration: Sendable {
    public var serverName: String
    public var port: Int
    public var database: String
    public var authentication: TDSAuthentication

    public init(serverName: String, port: Int, database: String, authentication: TDSAuthentication) {
        self.serverName = serverName
        self.port = port
        self.database = database
        self.authentication = authentication
    }
}
