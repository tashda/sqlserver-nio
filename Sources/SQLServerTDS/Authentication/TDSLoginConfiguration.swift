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
    /// When true, signals read-only application intent for AG secondary routing.
    public var readOnlyIntent: Bool
    /// Reported to the server as APP_NAME().
    public var applicationName: String
    /// The network packet size to ask for at login (512...32767; the server may accept less).
    public var packetSize: Int
    /// The Kerberos service principal name to ask a ticket for, instead of `MSSQLSvc/<serverName>:<port>`.
    public var serverSPN: String?
    /// Ask the server to describe Always Encrypted columns (COLUMNENCRYPTION).
    public var columnEncryption: Bool

    public init(
        serverName: String,
        port: Int,
        database: String,
        authentication: TDSAuthentication,
        readOnlyIntent: Bool = false,
        applicationName: String = TDSMessages.Login7Message.defaultApplicationName,
        packetSize: Int = TDSPacket.requestedPacketLength,
        serverSPN: String? = nil,
        columnEncryption: Bool = false
    ) {
        self.serverName = serverName
        self.port = port
        self.database = database
        self.authentication = authentication
        self.readOnlyIntent = readOnlyIntent
        self.applicationName = applicationName
        self.packetSize = packetSize
        self.serverSPN = serverSPN
        self.columnEncryption = columnEncryption
    }
}
