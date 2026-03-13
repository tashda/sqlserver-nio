import NIOSSL

public typealias SQLServerTLSConfiguration = TLSConfiguration

extension SQLServerTLSConfiguration {
    public static var clientDefault: SQLServerTLSConfiguration {
        .makeClientConfiguration()
    }
}

extension SQLServerClient {
    public struct Configuration: Sendable {
        public var connection: SQLServerConnection.Configuration
        public var poolConfiguration: SQLServerConnectionPool.Configuration
        public var retryConfiguration: SQLServerRetryConfiguration {
            get { connection.retryConfiguration }
            set { connection.retryConfiguration = newValue }
        }
        public var metadataConfiguration: SQLServerMetadataOperations.Configuration {
            get { connection.metadataConfiguration }
            set { connection.metadataConfiguration = newValue }
        }

        public init(
            connection: SQLServerConnection.Configuration,
            poolConfiguration: SQLServerConnectionPool.Configuration = .init()
        ) {
            self.connection = connection
            self.poolConfiguration = poolConfiguration
        }

        public init(
            hostname: String,
            port: Int = 1433,
            login: SQLServerConnection.Configuration.Login,
            tlsConfiguration: SQLServerTLSConfiguration? = .clientDefault,
            poolConfiguration: SQLServerConnectionPool.Configuration = .init(),
            metadataConfiguration: SQLServerMetadataOperations.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init(),
            transparentNetworkIPResolution: Bool = true
        ) {
            self.connection = SQLServerConnection.Configuration(
                hostname: hostname,
                port: port,
                login: login,
                tlsConfiguration: tlsConfiguration,
                metadataConfiguration: metadataConfiguration,
                retryConfiguration: retryConfiguration,
                sessionOptions: .ssmsDefaults,
                transparentNetworkIPResolution: transparentNetworkIPResolution
            )
            self.poolConfiguration = poolConfiguration
        }

        public init(
            hostname: String,
            port: Int = 1433,
            database: String = "master",
            authentication: SQLServerAuthentication,
            tlsEnabled: Bool,
            poolConfiguration: SQLServerConnectionPool.Configuration = .init(),
            metadataConfiguration: SQLServerMetadataOperations.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init(),
            transparentNetworkIPResolution: Bool = true
        ) {
            self.init(
                hostname: hostname,
                port: port,
                database: database,
                authentication: authentication,
                tlsConfiguration: tlsEnabled ? .clientDefault : nil,
                poolConfiguration: poolConfiguration,
                metadataConfiguration: metadataConfiguration,
                retryConfiguration: retryConfiguration,
                transparentNetworkIPResolution: transparentNetworkIPResolution
            )
        }

        public init(
            hostname: String,
            port: Int = 1433,
            database: String = "master",
            authentication: SQLServerAuthentication,
            tlsConfiguration: SQLServerTLSConfiguration? = .clientDefault,
            poolConfiguration: SQLServerConnectionPool.Configuration = .init(),
            metadataConfiguration: SQLServerMetadataOperations.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init(),
            transparentNetworkIPResolution: Bool = true
        ) {
            self.init(
                hostname: hostname,
                port: port,
                login: .init(database: database, authentication: authentication),
                tlsConfiguration: tlsConfiguration,
                poolConfiguration: poolConfiguration,
                metadataConfiguration: metadataConfiguration,
                retryConfiguration: retryConfiguration,
                transparentNetworkIPResolution: transparentNetworkIPResolution
            )
        }

        public var hostname: String {
            get { connection.hostname }
            set { connection.hostname = newValue }
        }

        public var port: Int {
            get { connection.port }
            set { connection.port = newValue }
        }

        public var login: SQLServerConnection.Configuration.Login {
            get { connection.login }
            set { connection.login = newValue }
        }

        public var tlsConfiguration: SQLServerTLSConfiguration? {
            get { connection.tlsConfiguration }
            set { connection.tlsConfiguration = newValue }
        }
        
        public var transparentNetworkIPResolution: Bool {
            get { connection.transparentNetworkIPResolution }
            set { connection.transparentNetworkIPResolution = newValue }
        }
    }
}
