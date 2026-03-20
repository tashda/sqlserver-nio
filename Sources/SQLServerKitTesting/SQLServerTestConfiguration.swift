import Foundation
import SQLServerKit
import SQLServerTDS
import NIOPosix

// MARK: - Connection Configuration

public func makeSQLServerConnectionConfiguration() -> SQLServerConnection.Configuration {
    if envFlagEnabled("USE_DOCKER") {
        setenv("TDS_HOSTNAME", "127.0.0.1", 1)
        setenv("TDS_PORT", env("TDS_DOCKER_PORT") ?? "14331", 1)
        do {
            _ = try ensureSQLServerTestFixture(requireAdventureWorks: envFlagEnabled("TDS_LOAD_ADVENTUREWORKS"))
        } catch {
            fatalError("Failed to start SQL Server Docker test environment: \(error)")
        }
    }

    // Use centralized environment configuration
    let config = TestEnvironmentManager.currentConfig

    let hostname = env("TDS_HOSTNAME") ?? config.hostname
    let port = env("TDS_PORT").flatMap(Int.init) ?? config.port
    let username = env("TDS_USERNAME") ?? config.username
    let password = env("TDS_PASSWORD") ?? config.password
    let database = env("TDS_DATABASE") ?? config.database

    var cfg = SQLServerConnection.Configuration(
        hostname: hostname,
        port: port,
        login: .init(
            database: database,
            authentication: .sqlPassword(username: username, password: password)
        ),
        tlsConfiguration: nil,
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
    return cfg
}

public func makeSQLServerClientConfiguration() -> SQLServerClient.Configuration {
    let pool = SQLServerConnectionPool.Configuration(
        maximumConcurrentConnections: 8,
        minimumIdleConnections: 0,
        connectionIdleTimeout: nil,
        validationQuery: nil
    )

    return SQLServerClient.Configuration(
        connection: makeSQLServerConnectionConfiguration(),
        poolConfiguration: pool
    )
}
