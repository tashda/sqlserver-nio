// In your test directory, create EnvironmentConfig.swift
public enum TestEnvironment: String, CaseIterable {
    case local = "local"
    
    public var configuration: TestEnvironmentConfig {
        switch self {
        case .local:
            return TestEnvironmentConfig(
                hostname: "YOUR_SQL_SERVER_IP",  // e.g., "127.0.0.1" or actual IP
                port: 1433,                     // Your SQL Server port
                database: "AdventureWorks2022",   // Default database
                username: "sa",                  // Your username
                password: "YourPassword123"       // Your password
            )
        }
    }
}
