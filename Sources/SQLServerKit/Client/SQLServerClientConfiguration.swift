import NIOSSL

public typealias SQLServerTLSConfiguration = TLSConfiguration

extension SQLServerTLSConfiguration {
    public static var clientDefault: SQLServerTLSConfiguration {
        .makeClientConfiguration()
    }

    /// A TLS configuration that skips server certificate validation.
    /// Equivalent to JDBC's `trustServerCertificate=true`.
    public static var trustingServerCertificate: SQLServerTLSConfiguration {
        var config = makeClientConfiguration()
        config.certificateVerification = .none
        return config
    }

    /// A TLS configuration that uses a custom CA certificate for server verification.
    /// - Parameter path: Path to a PEM-encoded CA certificate file.
    public static func withCACertificate(atPath path: String) -> SQLServerTLSConfiguration {
        var config = makeClientConfiguration()
        config.certificateVerification = .noHostnameVerification
        config.trustRoots = .file(path)
        return config
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
            trustServerCertificate: Bool = false,
            caCertificatePath: String? = nil,
            poolConfiguration: SQLServerConnectionPool.Configuration = .init(),
            metadataConfiguration: SQLServerMetadataOperations.Configuration = .init(),
            retryConfiguration: SQLServerRetryConfiguration = .init(),
            transparentNetworkIPResolution: Bool = true
        ) {
            let tlsConfig: SQLServerTLSConfiguration?
            if tlsEnabled {
                if trustServerCertificate {
                    tlsConfig = .trustingServerCertificate
                } else if let caPath = caCertificatePath {
                    tlsConfig = .withCACertificate(atPath: caPath)
                } else {
                    tlsConfig = .clientDefault
                }
            } else {
                tlsConfig = nil
            }
            self.init(
                hostname: hostname,
                port: port,
                database: database,
                authentication: authentication,
                tlsConfiguration: tlsConfig,
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
