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
        let payload: TDSMessages.Login7Message


        switch configuration.authentication {
        case .sqlPassword(let username, let password):
            payload = TDSMessages.Login7Message(
                username: username,
                password: password,
                serverName: configuration.serverName,
                database: configuration.database,
                useIntegratedSecurity: false,
                sspiData: nil
            )

        case .windowsIntegrated(let username, let password, let domain):
            do {
                let authenticatorInstance = try KerberosAuthenticator(
                    username: username,
                    password: password,
                    domain: domain,
                    server: configuration.serverName,
                    port: configuration.port,
                    logger: logger
                )
                let initialToken = try authenticatorInstance.initialToken()
                let loginUsername = domain.flatMap { "\($0)\\\(username)" } ?? username
                payload = TDSMessages.Login7Message(
                    username: loginUsername,
                    password: "",
                    serverName: configuration.serverName,
                    database: configuration.database,
                    useIntegratedSecurity: true,
                    sspiData: initialToken
                )

            } catch {
                return eventLoop.makeFailedFuture(error)
            }
        }
        // Create a promise and publish immediately to prevent a second LoginRequest enqueuing.
        let promise: EventLoopPromise<Void> = self.eventLoop.makePromise()
        self._loginFuture = promise.futureResult
        self.logger.debug("[login] Sending LoginRequest to server \(configuration.serverName) database \(configuration.database)")
        self.send(LoginRequest(payload: payload), logger: self.logger).flatMap { _ in
            return self.send(RawSqlRequest(sql: "SET FMTONLY OFF;"), logger: self.logger)
        }.whenComplete { result in
            switch result {
            case .success:
                // Replace with succeeded future for subsequent calls.
                self._loginFuture = self.eventLoop.makeSucceededFuture(())
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


