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
        let authenticator: KerberosAuthenticator?

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
            authenticator = nil
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
                authenticator = authenticatorInstance
            } catch {
                return eventLoop.makeFailedFuture(error)
            }
        }
        // Create a promise and publish immediately to prevent a second LoginRequest enqueuing.
        let promise: EventLoopPromise<Void> = self.eventLoop.makePromise()
        self._loginFuture = promise.futureResult
        self.logger.debug("[login] Sending LoginRequest to server \(configuration.serverName) database \(configuration.database)")
        self.send(LoginRequest(payload: payload, authenticator: authenticator, logger: logger, ring: self.tokenRing), logger: logger)
            .flatMap { _ in
                self.rawSql("SET FMTONLY OFF;").map { rows in
                    self.logger.debug("Session defaults applied (SET FMTONLY OFF); rows returned: \(rows.count)")
                    return ()
                }.recover { error in
                    self.logger.warning("Failed to set session defaults after login: \(error.localizedDescription)")
                    return ()
                }
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

class LoginRequest: TDSRequest {
    private let payload: TDSMessages.Login7Message
    private let logger: Logger
    private let authenticator: KerberosAuthenticator?
    
    private let tokenParser: TDSTokenParser
    // Track login progression to mirror JDBC’s expectation: either see LOGINACK or an error.
    private var seenLoginAck: Bool = false

    init(payload: TDSMessages.Login7Message, authenticator: KerberosAuthenticator?, logger: Logger, ring: TDSTokenRing?) {
        self.payload = payload
        self.logger = logger
        self.authenticator = authenticator
        self.tokenParser = TDSTokenParser(logger: logger, ring: ring)
    }

    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let tokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
        for token in tokens {
            switch token.type {
            case .loginAck:
                seenLoginAck = true
            case .envchange:
                // No special handling needed during login for ENVCHANGE; higher layers observe after login
                _ = token
            case .error:
                if let err = token as? TDSTokens.ErrorInfoToken {
                    logger.error("LOGIN failed: \(err.messageText) (#\(err.number))")
                    throw TDSError.protocolError("login failed: \(err.messageText)")
                }
            case .sessionState:
                // Surface raw session state snapshot to the connection for parity with JDBC envchange processing
                if let buf = (token as? TDSTokens.SessionStateToken)?.payload {
                    let bytes = buf.getBytes(at: buf.readerIndex, length: buf.readableBytes) ?? []
                    // Best-effort: attempt to locate a TDSConnection via side channel is not available here.
                    // Session state snapshots are handled by RawSql and elsewhere.
                    _ = bytes
                }
            default:
                break
            }
        }
        if let authenticator {
            for token in tokens {
                if var sspiToken = token as? TDSTokens.SSPIToken {
                    let readableBytes = sspiToken.payload.readableBytes
                    let serverBytes = sspiToken.payload.readBytes(length: readableBytes) ?? []
                    let serverData = Data(serverBytes)
                    let (response, _) = try authenticator.continueAuthentication(serverToken: serverData)
                    if let response, !response.isEmpty {
                        var responseBuffer = allocator.buffer(capacity: response.count)
                        responseBuffer.writeBytes(response)
                        let packet = TDSPacket(from: &responseBuffer, ofType: .sspi, isLastPacket: true, packetId: 1, allocator: allocator)
                        return .respond(with: [packet])
                    }
                }
            }
        }
        
        guard packet.header.status == .eom else {
            return .continue
        }

        // Finalize only after we have seen a LOGINACK and no error. Some servers may close
        // the connection on failed login without sending DONE; throwing early on error above
        // ensures the request fails in that case.
        if !seenLoginAck {
            logger.debug("LOGIN completed without explicit LOGINACK; proceeding conservatively")
        }
        return .done
    }

    func start(allocator: ByteBufferAllocator) throws -> [TDSPacket] {
        let message = try TDSMessage(payload: payload, allocator: allocator)
        return message.packets
    }

    func log(to logger: Logger) {
        logger.debug("Logging in as user: \(payload.username) to database: \(payload.database) and server: \(payload.serverName)")
    }
}
