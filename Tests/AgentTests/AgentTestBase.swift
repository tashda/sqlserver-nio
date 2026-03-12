import Foundation
@testable import SQLServerKit
import SQLServerKitTesting
import XCTest

class AgentTestBase: XCTestCase, @unchecked Sendable {
    var client: SQLServerClient!
    var managedJobNames: [String] = []
    var managedScheduleNames: [String] = []

    let operationTimeout: TimeInterval = Double(env("TDS_TEST_OPERATION_operationTimeout_SECONDS") ?? "30") ?? 30

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables()

        if envFlagEnabled("USE_DOCKER") {
            try SQLServerDockerManager.shared.startIfNeeded()
        }

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0
        self.client = try await SQLServerClient.connect(configuration: config, numberOfThreads: 1)

        _ = try await withTimeout(operationTimeout) {
            try await self.client.query("SELECT 1")
        }

        let agentStatus = try await withTimeout(operationTimeout) {
            try await self.client.metadata.fetchAgentStatus()
        }

        if agentStatus.isSqlAgentRunning && !agentStatus.isSqlAgentEnabled {
            _ = try await client.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;")
            _ = try await client.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;")
        }
    }

    override func tearDown() async throws {
        if let client = client {
            let agent = SQLServerAgentOperations(client: client)
            for jobName in managedJobNames.reversed() {
                _ = try? await agent.deleteJob(named: jobName)
            }
            for scheduleName in managedScheduleNames.reversed() {
                _ = try? await agent.deleteSchedule(named: scheduleName)
            }
            managedJobNames.removeAll()
            managedScheduleNames.removeAll()
        }

        do {
            try await client?.shutdownGracefully()
        } catch {
            let message = error.localizedDescription
            if !message.contains("Already closed") && !message.contains("ChannelError error 6") {
                throw error
            }
        }
    }

    @discardableResult
    func createManagedJob(includeSchedule: Bool = false) async throws -> (jobName: String, scheduleName: String?) {
        let agent = SQLServerAgentOperations(client: self.client)
        let jobName = "agent_test_\(UUID().uuidString.prefix(8))"
        let scheduleName = includeSchedule ? "agent_sched_\(UUID().uuidString.prefix(8))" : nil

        let builder = SQLServerAgentJobBuilder(
            agent: agent,
            jobName: jobName,
            description: "Managed integration test job",
            enabled: true
        )

        _ = builder.addStep(SQLServerAgentJobStep(
            name: "step_select_1",
            subsystem: .tsql,
            command: "SELECT 1;",
            database: "master"
        ))

        if let scheduleName {
            _ = builder.addSchedule(SQLServerAgentJobSchedule(
                name: scheduleName,
                enabled: true,
                kind: .daily(everyDays: 1, startTime: 120000)
            ))
        }

        _ = try await withTimeout(operationTimeout) {
            try await builder.commit()
        }

        managedJobNames.append(jobName)
        if let scheduleName {
            managedScheduleNames.append(scheduleName)
        }

        return (jobName, scheduleName)
    }
}
