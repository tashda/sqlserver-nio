import Foundation
import Logging

// MARK: - Environment Configuration

public enum TestEnvironment: String, CaseIterable {
    case production = "production"
    case staging = "staging"
    case development = "development"
    case local = "local"
    case sql2025 = "sql2025"

    public var displayName: String {
        switch self {
        case .production: return "Production Server"
        case .staging: return "Staging Server"
        case .development: return "Development Server"
        case .local: return "Local Server"
        case .sql2025: return "SQL Server 2025"
        }
    }

    public var configuration: TestEnvironmentConfig {
        switch self {
        case .production, .staging, .development, .local:
            return TestEnvironmentConfig(
                hostname: "localhost",
                port: 1433,
                database: "master",
                username: "sa",
                password: "<your_password>"
            )
        case .sql2025:
            return TestEnvironmentConfig(
                hostname: "localhost",
                port: 1433,
                database: "master",
                username: "sa",
                password: "<your_password>"
            )
        }
    }
}

public struct TestEnvironmentConfig {
    public let hostname: String
    public let port: Int
    public let database: String
    public let username: String
    public let password: String

    public init(hostname: String, port: Int, database: String, username: String, password: String) {
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username
        self.password = password
    }
}

public class TestEnvironmentManager {
    public static var currentEnvironment: TestEnvironment {
        let envName = ProcessInfo.processInfo.environment["TDS_ENV"] ?? "local"
        return TestEnvironment(rawValue: envName) ?? .local
    }

    public static var currentConfig: TestEnvironmentConfig {
        return currentEnvironment.configuration
    }

    public static func loadEnvironmentVariables() {
        // Use TDS_ENV to select config, but only set each var as a default
        // (overwrite=0) so that shell-provided values take precedence.
        let config = currentConfig
        setenv("TDS_HOSTNAME", config.hostname, 0)
        setenv("TDS_PORT", String(config.port), 0)
        setenv("TDS_DATABASE", config.database, 0)
        setenv("TDS_USERNAME", config.username, 0)
        setenv("TDS_PASSWORD", config.password, 0)
    }
}

// MARK: - Logging Infrastructure

public let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()

// MARK: - Environment Utilities

public func env(_ name: String) -> String? {
    if let value = ProcessInfo.processInfo.environment[name] {
        return value
    }
    return getenv(name).flatMap { String(cString: $0) }
}

public func envFlagEnabled(_ key: String) -> Bool {
    guard let value = env(key) else { return false }
    return value == "1" || value.lowercased() == "true" || value.lowercased() == "yes"
}
