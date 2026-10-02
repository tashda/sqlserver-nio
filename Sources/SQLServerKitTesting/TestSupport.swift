import Foundation
import Logging

// MARK: - Logging

public let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .info
        return handler
    }
    return true
}()

// MARK: - Environment

/// An environment variable (the `TDS_` debug switches, `LOG_LEVEL`). Servers come from
/// ``TestServer``, never from separate host or credential variables.
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
