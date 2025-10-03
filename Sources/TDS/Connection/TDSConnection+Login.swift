import Logging
import NIO
import Foundation

extension TDSConnection {
    public func login(configuration: TDSLoginConfiguration) -> EventLoopFuture<Void> {
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

        return self.send(LoginRequest(payload: payload, authenticator: authenticator, logger: logger), logger: logger)
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
    
    init(payload: TDSMessages.Login7Message, authenticator: KerberosAuthenticator?, logger: Logger) {
        self.payload = payload
        self.logger = logger
        self.authenticator = authenticator
        self.tokenParser = TDSTokenParser(logger: logger)
    }

    func handle(packet: TDSPacket, allocator: ByteBufferAllocator) throws -> TDSPacketResponse {
        // Add packet to token parser stream
        let tokens = tokenParser.writeAndParseTokens(packet.messageBuffer)
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
                        let packet = TDSPacket(from: &responseBuffer, ofType: .sspi, isLastPacket: true, allocator: allocator)
                        return .respond(with: [packet])
                    }
                }
            }
        }
        
        guard packet.header.status == .eom else {
            return .continue
        }
        
        // TODO: Set logged in ready state
        // TODO: React to envchange request from server
        
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
