#!/usr/bin/env swift

import Foundation
import NIO
import Logging

// Import SQLServerKit modules - adjust paths as needed
let packageDir = "/Users/k/Development/sqlserver-nio"

print("🚀 Starting Agent API Test...")

// Load TestInfrastructure functions manually
let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = .debug
        return handler
    }
    return true
}()

// Environment variable helpers
func env(_ name: String) -> String? {
    if let value = ProcessInfo.processInfo.environment[name] {
        return value
    }
    return getenv(name).flatMap { String(cString: $0) }
}

func envFlagEnabled(_ key: String) -> Bool {
    guard let value = env(key) else { return false }
    return value == "1" || value.lowercased() == "true" || value.lowercased() == "yes"
}

// Test configuration
let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

// Simple timeout implementation
func withTimeout<T>(_ timeout: TimeInterval, operation: @escaping () async throws -> T) async throws -> T {
    return try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            return try await operation()
        }

        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            throw NSError(domain: "TimeoutError", code: -1, userInfo: [NSLocalizedDescriptionKey: "Operation timed out after \(timeout) seconds"])
        }

        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}

// Test configuration structs
struct TestEnvironmentConfig {
    let hostname: String
    let port: Int
    let database: String
    let username: String
    let password: String
}

func makeSQLServerClientConfiguration() -> SQLServerClient.Configuration {
    let hostname = env("TDS_HOSTNAME") ?? "localhost"
    let port = env("TDS_PORT").flatMap(Int.init) ?? 1433
    let username = env("TDS_USERNAME") ?? "swift_tds_user"
    let password = env("TDS_PASSWORD") ?? "SwiftTDS!"
    let database = env("TDS_DATABASE") ?? "swift_tds_database"

    var cfg = SQLServerConnection.Configuration(
        hostname: hostname,
        port: port,
        login: .init(
            database: database,
            authentication: .sqlPassword(username: username, password: password)
        ),
        tlsConfiguration: nil,
        metadataConfiguration: SQLServerMetadataClient.Configuration(
            includeSystemSchemas: false,
            enableColumnCache: true,
            includeRoutineDefinitions: true
        )
    )
    cfg.transparentNetworkIPResolution = false

    let pool = SQLServerConnectionPool.Configuration(
        maximumConcurrentConnections: 4,
        minimumIdleConnections: 0,
        connectionIdleTimeout: nil,
        validationQuery: "SELECT 1;"
    )

    return SQLServerClient.Configuration(
        connection: cfg,
        poolConfiguration: pool
    )
}

@main
struct AgentAPITest {
    static func main() async throws {
        print("🔍 [AgentAPITest] Setting up test environment...")

        // Check if agent tests are enabled
        guard envFlagEnabled("TDS_ENABLE_AGENT_TESTS") else {
            print("⚠️ [AgentAPITest] Agent tests disabled. Set TDS_ENABLE_AGENT_TESTS=1 to enable.")
            return
        }

        // Set up connection
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! group.syncShutdownGracefully() }

        let config = makeSQLServerClientConfiguration()
        let client = try await SQLServerClient.connect(
            configuration: config,
            eventLoopGroupProvider: .shared(group)
        ).get()

        defer {
            try? client.shutdownGracefully().wait()
        }

        print("✅ [AgentAPITest] Connected to SQL Server")

        // Test 1: Enhanced API
        print("\n🔍 [AgentAPITest] Testing enhanced listJobsDetailed() API...")
        do {
            let agent = try await withTimeout(TIMEOUT) {
                try await client.withConnection { connection in
                    SQLServerAgentClient(connection: connection)
                }
            }

            let jobs = try await withTimeout(TIMEOUT) {
                try await agent.listJobsDetailed()
            }

            print("✅ [AgentAPITest] Enhanced API succeeded: loaded \(jobs.count) jobs")

            if let firstJob = jobs.first {
                print("🔍 [AgentAPITest] Sample enhanced job data:")
                print("  - jobId: \(firstJob.jobId)")
                print("  - name: \(firstJob.name)")
                print("  - enabled: \(firstJob.enabled)")
                print("  - owner: \(firstJob.ownerLoginName ?? "nil")")
                print("  - category: \(firstJob.categoryName ?? "nil")")
                print("  - description: \(firstJob.description ?? "nil")")
                print("  - hasSchedule: \(firstJob.hasSchedule)")
                print("  - lastRunOutcome: \(firstJob.lastRunOutcome ?? "nil")")
            }

            // Test 2: Job detail for specific job
            if let testJob = jobs.first {
                print("\n🔍 [AgentAPITest] Testing getJobDetail() for \(testJob.name)...")
                let jobDetail = try await withTimeout(TIMEOUT) {
                    try await agent.getJobDetail(jobName: testJob.name)
                }

                if let detail = jobDetail {
                    print("✅ [AgentAPITest] getJobDetail succeeded")
                    print("  - jobId: \(detail.jobId)")
                    print("  - name: \(detail.name)")
                    print("  - enabled: \(detail.enabled)")
                    print("  - description: \(detail.description ?? "nil")")
                } else {
                    print("⚠️ [AgentAPITest] getJobDetail returned nil")
                }
            }

        } catch {
            print("❌ [AgentAPITest] Enhanced API failed: \(error.localizedDescription)")
        }

        // Test 3: Basic API Fallback
        print("\n🔍 [AgentAPITest] Testing basic listJobs() API fallback...")
        do {
            let agent = try await withTimeout(TIMEOUT) {
                try await client.withConnection { connection in
                    SQLServerAgentClient(connection: connection)
                }
            }

            let jobs = try await withTimeout(TIMEOUT) {
                try await agent.listJobs()
            }

            print("✅ [AgentAPITest] Basic API succeeded: loaded \(jobs.count) jobs")

            if let firstJob = jobs.first {
                print("🔍 [AgentAPITest] Basic job data check:")
                print("  - name: '\(firstJob.name)'")
                print("  - enabled: \(firstJob.enabled)")
                print("  - lastRunOutcome: '\(firstJob.lastRunOutcome ?? "nil")'")
            }

        } catch {
            print("❌ [AgentAPITest] Basic API failed: \(error.localizedDescription)")
        }

        print("\n🎉 [AgentAPITest] Agent API testing complete!")
    }
}