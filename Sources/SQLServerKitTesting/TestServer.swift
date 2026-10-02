import Foundation
import SQLServerKit

/// A SQL Server for integration tests, read from one URL variable.
///
/// | Variable | Setup |
/// |---|---|
/// | `SQLSERVER_TEST_URL` | a plain server |
/// | `SQLSERVER_TEST_TLS_URL` | a server that requires TLS; the URL carries the mode and the CA |
/// | `SQLSERVER_TEST_KERBEROS_URL` | Kerberos logins; `krb5Config` names the Kerberos settings file |
/// | `SQLSERVER_TEST_AG_URLS` | availability-group replicas, primary first, comma-separated |
/// | `SQLSERVER_TEST_PROXY_URL`, `SQLSERVER_TEST_PROXY_CONTROL` | the server through Toxiproxy, and its HTTP API |
///
/// A test whose variable is missing is skipped, naming the variable; with
/// `SQLSERVER_TEST_REQUIRED=1` it fails instead.
///
/// ```
/// sqlserver://sa:pass@localhost:1433/master?encrypt=mandatory&trustServerCertificate=true
/// sqlserver://sa:pass@host:1433/master?encrypt=strict&trustServerCertificate=false&caFile=/path/ca.pem
/// sqlserver://labuser%40LAB.TEST@host:1433/master?authentication=kerberos&serviceHost=sql.lab.test&krb5Config=/krb5.conf
/// ```
///
/// User and password are percent-encoded. The path names the database to connect to first
/// (`master` when empty). Query keys: `encrypt` (`optional`, `mandatory`, `strict`; also
/// `true`/`false`), `trustServerCertificate`, `caFile`, `hostNameInCertificate`,
/// `authentication=kerberos`, `serviceHost` and `krb5Config`, and `columnEncryption=true` (Always
/// Encrypted metadata; ODBC's `ColumnEncryption=Enabled`).
public struct TestServer: Sendable {
    public static let defaultVariable = "SQLSERVER_TEST_URL"
    public static let tlsVariable = "SQLSERVER_TEST_TLS_URL"
    public static let kerberosVariable = "SQLSERVER_TEST_KERBEROS_URL"
    public static let availabilityGroupVariable = "SQLSERVER_TEST_AG_URLS"
    public static let proxyVariable = "SQLSERVER_TEST_PROXY_URL"
    public static let proxyControlVariable = "SQLSERVER_TEST_PROXY_CONTROL"
    public static let requiredVariable = "SQLSERVER_TEST_REQUIRED"

    public struct Kerberos: Sendable, Equatable {
        /// The name to connect to: the service ticket is for it.
        public var serviceHost: String?
        /// The Kerberos settings file (`KRB5_CONFIG`).
        public var krb5Config: String?

        public init(serviceHost: String?, krb5Config: String?) {
            self.serviceHost = serviceHost
            self.krb5Config = krb5Config
        }
    }

    /// The variable the URL came from.
    public let variable: String
    public let hostname: String
    public let port: Int
    public let username: String
    public let password: String
    public let database: String
    public let encrypt: SQLServerEncryptionMode
    public let trustServerCertificate: Bool
    public let caFile: String?
    public let hostNameInCertificate: String?
    /// Set for `authentication=kerberos`.
    public let kerberos: Kerberos?
    /// `columnEncryption=true`: connections negotiate Always Encrypted metadata.
    public let columnEncryption: Bool

    /// The server for the running test, set by the `.testServer` trait.
    @TaskLocal public static var current: TestServer?

    /// Whether a missing variable fails instead of skipping (`SQLSERVER_TEST_REQUIRED=1`).
    public static var isRequired: Bool { isRequired(in: ProcessInfo.processInfo.environment) }

    public static func isRequired(in environment: [String: String]) -> Bool {
        ["1", "true", "yes"].contains(environment[requiredVariable]?.lowercased() ?? "")
    }

    /// What a test that needs `variable` does.
    public enum Availability: Sendable {
        case available(TestServer)
        /// The variable is not set: skip with this message.
        case skip(String)
        /// The variable is not set and `SQLSERVER_TEST_REQUIRED=1`, or the URL cannot be read.
        case fail(String)
    }

    public static func availability(
        of variable: String = defaultVariable,
        in environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> Availability {
        do {
            if let server = try load(variable, environment: environment) { return .available(server) }
        } catch {
            return .fail(String(describing: error))
        }
        let message = missingMessage(variable)
        return isRequired(in: environment) ? .fail(message + " and \(requiredVariable)=1") : .skip(message)
    }

    /// The server named by `variable`, or nil when the variable is not set or not a valid URL
    /// (``load(_:environment:)`` says why).
    public static func url(_ variable: String = defaultVariable) -> TestServer? {
        try? load(variable)
    }

    /// The server named by `variable`; nil when it is not set, an error when it is not a valid URL.
    public static func load(
        _ variable: String = defaultVariable,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws -> TestServer? {
        guard let text = environment[variable], !text.isEmpty else { return nil }
        return try parse(text, variable: variable)
    }

    /// The replicas in `SQLSERVER_TEST_AG_URLS`, primary first; nil when it is not set.
    public static func availabilityGroup(
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws -> [TestServer]? {
        guard let text = environment[availabilityGroupVariable], !text.isEmpty else { return nil }
        return try text.split(separator: ",").map { try parse(String($0).trimmingCharacters(in: .whitespaces), variable: availabilityGroupVariable) }
    }

    /// Toxiproxy's HTTP API for the server in `SQLSERVER_TEST_PROXY_URL`.
    public static var proxyControl: URL? {
        ProcessInfo.processInfo.environment[proxyControlVariable].flatMap(URL.init(string:))
    }

    /// Why a test that needs a server cannot run: its variable is missing while
    /// `SQLSERVER_TEST_REQUIRED=1`, or its URL cannot be read.
    public struct Unavailable: Error, CustomStringConvertible {
        public let description: String
        public init(_ description: String) { self.description = description }
    }

    public struct InvalidURL: Error, CustomStringConvertible {
        public let variable: String
        public let reason: String
        public var description: String { "\(variable) is not a valid sqlserver:// URL: \(reason)" }
    }

    public static func parse(_ text: String, variable: String = defaultVariable) throws -> TestServer {
        func invalid(_ reason: String) -> InvalidURL { InvalidURL(variable: variable, reason: reason) }
        guard let components = URLComponents(string: text) else { throw invalid("cannot be read as a URL") }
        guard components.scheme?.lowercased() == "sqlserver" else { throw invalid("the scheme must be sqlserver://") }
        guard let host = components.host, !host.isEmpty else { throw invalid("there is no host") }
        // URLComponents decodes user and password; percentEncodedUser keeps "@", ":" and "/" intact.
        let username = components.percentEncodedUser.map { $0.removingPercentEncoding ?? $0 } ?? ""
        let password = components.percentEncodedPassword.map { $0.removingPercentEncoding ?? $0 } ?? ""
        var query: [String: String] = [:]
        for item in components.queryItems ?? [] { query[item.name.lowercased()] = item.value ?? "" }

        let encrypt: SQLServerEncryptionMode
        switch query["encrypt"]?.lowercased() {
        case nil, "", "mandatory", "true", "yes": encrypt = .mandatory
        case "optional", "false", "no": encrypt = .optional
        case "strict": encrypt = .strict
        case let other?: throw invalid("encrypt=\(other) is not optional, mandatory or strict")
        }
        let trust: Bool
        switch query["trustservercertificate"]?.lowercased() {
        case nil, "", "false", "no": trust = false
        case "true", "yes": trust = true
        case let other?: throw invalid("trustServerCertificate=\(other) is not true or false")
        }
        var kerberos: Kerberos?
        switch query["authentication"]?.lowercased() {
        case nil, "", "sql", "sqlpassword": break
        case "kerberos": kerberos = Kerberos(serviceHost: query["servicehost"], krb5Config: query["krb5config"])
        case let other?: throw invalid("authentication=\(other) is not kerberos")
        }
        if kerberos == nil, username.isEmpty { throw invalid("there is no user") }
        let columnEncryption: Bool
        switch query["columnencryption"]?.lowercased() {
        case nil, "", "false", "disabled", "no": columnEncryption = false
        case "true", "enabled", "yes": columnEncryption = true
        case let other?: throw invalid("columnEncryption=\(other) is not true or false")
        }
        let path = components.path.hasPrefix("/") ? String(components.path.dropFirst()) : components.path
        let database = path.removingPercentEncoding ?? path

        return TestServer(
            variable: variable,
            hostname: host,
            port: components.port ?? 1433,
            username: username,
            password: password,
            database: database.isEmpty ? "master" : database,
            encrypt: encrypt,
            trustServerCertificate: trust,
            caFile: query["cafile"].flatMap { $0.isEmpty ? nil : $0 },
            hostNameInCertificate: query["hostnameincertificate"].flatMap { $0.isEmpty ? nil : $0 },
            kerberos: kerberos,
            columnEncryption: columnEncryption
        )
    }

    /// The TLS settings the URL asks for: the CA file, trusting any certificate, or the system's trust store.
    public var tlsConfiguration: SQLServerTLSConfiguration {
        if let caFile { return .withCACertificate(atPath: caFile) }
        if trustServerCertificate { return .trustingServerCertificate }
        return .clientDefault
    }

    /// The login: SQL authentication, or for Kerberos the principal (`user@REALM`) with the
    /// password when the URL has one, else the ticket already in the cache.
    public var authentication: SQLServerAuthentication {
        guard kerberos != nil else { return .sqlPassword(username: username, password: password) }
        let parts = username.split(separator: "@", maxSplits: 1).map(String.init)
        return .windowsIntegrated(username: parts.first ?? username, password: password, domain: parts.count > 1 ? parts[1] : nil)
    }

    /// A connection configuration for this server. For Kerberos the service ticket is for
    /// `serviceHost` (`serverSPN`), while the connection goes to the URL's host.
    public var configuration: SQLServerConnection.Configuration {
        var configuration = SQLServerConnection.Configuration(
            hostname: hostname,
            port: port,
            login: .init(database: database, authentication: authentication),
            tlsConfiguration: tlsConfiguration,
            encryptionMode: encrypt,
            hostNameInCertificate: hostNameInCertificate
        )
        configuration.transparentNetworkIPResolution = false
        configuration.columnEncryption = columnEncryption
        if let serviceHost = kerberos?.serviceHost {
            configuration.serverSPN = "MSSQLSvc/\(serviceHost):\(port)"
        }
        return configuration
    }

    /// A pooled client configuration for this server.
    public var clientConfiguration: SQLServerClient.Configuration {
        SQLServerClient.Configuration(connection: configuration)
    }

    /// Points this process at the URL's Kerberos settings (`KRB5_CONFIG`). Call before the first
    /// Kerberos login; the setting is process-wide.
    public func useKerberosSettings() {
        if let path = kerberos?.krb5Config { setenv("KRB5_CONFIG", path, 1) }
    }

    /// The message a test skips (or fails) with when `variable` is not set.
    public static func missingMessage(_ variable: String) -> String {
        "\(variable) is not set: this test needs a SQL Server (see TESTING.md)"
    }

}
