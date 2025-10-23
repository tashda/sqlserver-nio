import Foundation

public enum TDSAuthentication {
    case sqlPassword(username: String, password: String)
    case windowsIntegrated(username: String, password: String, domain: String?)
}

public struct TDSLoginConfiguration {
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
