import Foundation
import SQLServerKit
import SQLServerTDS
import NIOPosix

// MARK: - Connection Configuration

/// The connection configuration for the server in `SQLSERVER_TEST_URL`, with the settings the
/// integration tests rely on (metadata options, retries on dropped connections).
///
/// Call `requireSQLServerTestServer()` (XCTest) or use the `.testServer` trait first: without the
/// variable this returns a configuration for a host that cannot be resolved, so a test that forgot
/// fails at once instead of reaching a real server.
public func makeSQLServerConnectionConfiguration(_ variable: String = TestServer.defaultVariable) -> SQLServerConnection.Configuration {
    let server = TestServer.url(variable)
    var cfg = SQLServerConnection.Configuration(
        hostname: server?.configuration.hostname ?? "sqlserver-test-url-not-set.invalid",
        port: server?.port ?? 1433,
        login: .init(
            database: server?.database ?? "master",
            authentication: server?.authentication ?? .sqlPassword(username: "", password: "")
        ),
        tlsConfiguration: server?.tlsConfiguration ?? .trustingServerCertificate,
        encryptionMode: server?.encrypt ?? .mandatory,
        hostNameInCertificate: server?.hostNameInCertificate,
        metadataConfiguration: SQLServerMetadataOperations.Configuration(
            includeSystemSchemas: false,
            enableColumnCache: true,
            includeRoutineDefinitions: true,
            includeTriggerDefinitions: true,
            commandTimeout: 10,
            extractParameterDefaults: false
        ),
        retryConfiguration: SQLServerRetryConfiguration(
            maximumAttempts: 5,
            backoffStrategy: { attempt in
                let base = 0.25
                return base * Double(1 << max(0, attempt - 1))
            },
            shouldRetry: { error in
                if let se = error as? SQLServerError {
                    switch se {
                    case .connectionClosed, .transient:
                        return true
                    case .timeout:
                        return false
                    default:
                        return false
                    }
                }
                if let tds = error as? TDSError {
                    if case .connectionClosed = tds { return true }
                    if case .protocolError(let message) = tds, message.localizedCaseInsensitiveContains("timeout") { return false }
                }
                if let ch = error as? ChannelError {
                    switch ch {
                    case .ioOnClosedChannel, .outputClosed, .eof, .alreadyClosed:
                        return true
                    default:
                        break
                    }
                }
                if error is NIOConnectionError { return true }
                return false
            }
        )
    )
    cfg.transparentNetworkIPResolution = false
    cfg.serverSPN = server?.configuration.serverSPN
    return cfg
}

public func makeSQLServerClientConfiguration(_ variable: String = TestServer.defaultVariable) -> SQLServerClient.Configuration {
    let pool = SQLServerConnectionPool.Configuration(
        maximumConcurrentConnections: 8,
        minimumIdleConnections: 0,
        connectionIdleTimeout: nil,
        validationQuery: nil
    )

    return SQLServerClient.Configuration(
        connection: makeSQLServerConnectionConfiguration(variable),
        poolConfiguration: pool
    )
}
