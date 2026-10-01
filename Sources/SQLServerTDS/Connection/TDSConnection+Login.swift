import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(configuration: TDSLoginConfiguration) -> EventLoopFuture<Void> {
        // Always perform login work on the channel's event loop to avoid races.
        if !self.eventLoop.inEventLoop {
            return self.eventLoop.flatSubmit { self.login(configuration: configuration) }
        }
        // Coalesce concurrent calls: if one exists (including a previously succeeded one), return it.
        if let existing = self._loginFuture {
            self.logger.debug("[login] Coalescing to existing in-flight/completed login future")
            return existing
        }
        var payload: TDSMessages.Login7Message
        var authenticator: (any TDSAuthenticator)?

        switch configuration.authentication {
        case .sqlPassword(let username, let password):
            payload = TDSMessages.Login7Message(
                username: username,
                password: password,
                serverName: configuration.serverName,
                database: configuration.database,
                useIntegratedSecurity: false,
                sspiData: nil,
                readOnlyIntent: configuration.readOnlyIntent,
                applicationName: configuration.applicationName,
                packetSize: configuration.packetSize
            )

        case .windowsIntegrated(let username, let password, let domain):
            do {
                let (authenticatorInstance, initialToken) = try Self.windowsAuthenticator(
                    username: username,
                    password: password,
                    domain: domain,
                    server: configuration.serverName,
                    port: configuration.port,
                    servicePrincipalName: configuration.serverSPN,
                    logger: logger
                )
                let loginUsername = domain.flatMap { "\($0)\\\(username)" } ?? username
                payload = TDSMessages.Login7Message(
                    username: loginUsername,
                    password: "",
                    serverName: configuration.serverName,
                    database: configuration.database,
                    useIntegratedSecurity: true,
                    sspiData: initialToken,
                    readOnlyIntent: configuration.readOnlyIntent,
                    applicationName: configuration.applicationName,
                packetSize: configuration.packetSize
                )
                authenticator = authenticatorInstance
            } catch {
                return eventLoop.makeFailedFuture(error)
            }

        case .accessToken(let token):
            payload = TDSMessages.Login7Message(
                username: "",
                password: "",
                serverName: configuration.serverName,
                database: configuration.database,
                useIntegratedSecurity: false,
                sspiData: nil,
                fedAuthAccessToken: token,
                readOnlyIntent: configuration.readOnlyIntent,
                applicationName: configuration.applicationName,
                packetSize: configuration.packetSize
            )
        }
        payload.requestColumnEncryption = configuration.columnEncryption
        // Create a promise and publish immediately to prevent a second LoginRequest enqueuing.
        let promise: EventLoopPromise<Void> = self.eventLoop.makePromise()
        self._loginFuture = promise.futureResult

        // Create the login request with SSPI continuation support
        let loginRequest = LoginRequest(
            payload: payload,
            authenticator: authenticator,
            connection: self
        )

        self.logger.debug("[login] Sending LoginRequest to server \(configuration.serverName) database \(configuration.database)")
        self.send(loginRequest, logger: self.logger).whenComplete { result in
            switch result {
            case .success:
                // Replace with succeeded future for subsequent calls.
                self._loginFuture = self.eventLoop.makeSucceededFuture(())
                self.logger.info("TDS login completed for database \(configuration.database)")
                promise.succeed(())
            case .failure(let error):
                // Clear so callers may retry a new login later.
                self._loginFuture = nil
                promise.fail(error)
            }
        }
        return promise.futureResult
    }

    public func login(username: String, password: String, server: String, database: String) -> EventLoopFuture<Void> {
        let configuration = TDSLoginConfiguration(
            serverName: server,
            port: 0,
            database: database,
            authentication: .sqlPassword(username: username, password: password)
        )
        return login(configuration: configuration)
    }
}

extension TDSConnection {
    /// Windows authentication chooses like SSPI's Negotiate package (what SqlClient uses): Kerberos
    /// first, with a ticket from the given password or, without one, from the ticket cache; NTLMv2
    /// when Kerberos is not available for this server (no KDC for the realm, no service principal,
    /// no GSS on this platform). SQL Server on Linux accepts only Kerberos.
    static func windowsAuthenticator(
        username: String,
        password: String,
        domain: String?,
        server: String,
        port: Int,
        servicePrincipalName: String? = nil,
        logger: Logger
    ) throws -> (any TDSAuthenticator, Data) {
        do {
            let kerberos = try KerberosAuthenticator(
                username: username, password: password, domain: domain,
                server: server, port: port, servicePrincipalName: servicePrincipalName, logger: logger
            )
            let token = try kerberos.initialToken()
            logger.debug("[login] Windows authentication: Kerberos")
            return (kerberos, token)
        } catch {
            guard !username.isEmpty, !password.isEmpty else { throw error }
            logger.debug("[login] Kerberos unavailable (\(error)); Windows authentication: NTLMv2")
            let ntlm = try NTLMv2Authenticator(
                username: username, password: password, domain: domain ?? "",
                server: server, port: port, logger: logger
            )
            return (ntlm, try ntlm.initialToken())
        }
    }
}
