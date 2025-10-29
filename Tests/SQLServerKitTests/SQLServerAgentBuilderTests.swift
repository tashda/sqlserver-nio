import Foundation
import NIO
@testable import SQLServerKit
import XCTest

final class SQLServerAgentBuilderTests: XCTestCase {
    let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "60") ?? 60

    func testCreateJobWithStepAndSchedule() throws {
        loadEnvFileIfPresent()
        // Gate on agent tests; deep covers create/attach and verification
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")
        try requireEnvFlag("TDS_ENABLE_AGENT_DEEP_TESTS", description: "SQL Agent deep lifecycle tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let metadata = SQLServerMetadataClient(connection: conn)
        let status = try waitForResult(metadata.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        if !status.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        // Ensure Agent XPs on if service is running
        if !status.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE; EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)

        let jobName = "tds_builder_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let step = SQLServerAgentJobStep(name: "s1", subsystem: .tsql, command: "SELECT 1;", database: "master")
        let schedule = SQLServerAgentJobSchedule(name: "sched_" + String(jobName.suffix(8)), enabled: true, kind: .daily(everyDays: 1, startTime: 90000))

        if #available(macOS 12.0, *) {
            defer { _ = try? waitForResult(agent.deleteJob(named: jobName), timeout: TIMEOUT, description: "cleanup job") }
            do {
                let builder = SQLServerAgentJobBuilder(agent: agent, jobName: jobName, description: "builder test", enabled: true, ownerLoginName: nil, categoryName: nil, autoAttachServer: true)
                    .addStep(step)
                    .addSchedule(schedule)
                    .setStartStepId(1)

                let (_, jobId) = try waitForAsync(timeout: TIMEOUT, description: "commit job") {
                    try await builder.commit()
                }
                XCTAssertNotNil(jobId)

                let steps = try waitForResult(agent.listSteps(jobName: jobName), timeout: TIMEOUT, description: "list steps")
                XCTAssertTrue(steps.contains(where: { $0.name == "s1" }))
            } catch {
                XCTFail("Builder commit failed: \(error)")
            }
        }
    }

    func testWeeklyScheduleAndFlowControl() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")
        try requireEnvFlag("TDS_ENABLE_AGENT_DEEP_TESTS", description: "SQL Agent deep lifecycle tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let metadata = SQLServerMetadataClient(connection: conn)
        let status = try waitForResult(metadata.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        if !status.isSqlAgentRunning { throw XCTSkip("Agent not running") }
        if !status.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE; EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)
        let jobName = "tds_builder_flow_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer { _ = try? waitForResult(agent.deleteJob(named: jobName), timeout: TIMEOUT, description: "cleanup job") }

        _ = try waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_builder_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_builder_marker; CREATE TABLE tempdb.dbo.tds_builder_marker (id INT IDENTITY(1,1) PRIMARY KEY);"), timeout: TIMEOUT, description: "setup marker")
        defer { _ = try? waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_builder_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_builder_marker;"), timeout: TIMEOUT, description: "drop marker") }

        var step1 = SQLServerAgentJobStep(name: "fail", subsystem: .tsql, command: "RAISERROR('boom',16,1);", database: "master")
        step1.onSuccess = .goToNextStep
        step1.onFail = .goToStep(2)
        let step2 = SQLServerAgentJobStep(name: "ok", subsystem: .tsql, command: "INSERT INTO tempdb.dbo.tds_builder_marker DEFAULT VALUES;", database: "master")

        let schedule = SQLServerAgentJobSchedule(name: "weekly_" + String(jobName.suffix(6)), enabled: true, kind: .weekly(days: [.monday, .wednesday, .friday], everyWeeks: 1, startTime: 90000))

        if #available(macOS 12.0, *) {
            let builder = SQLServerAgentJobBuilder(agent: agent, jobName: jobName, description: "flow control", enabled: true)
                .addStep(step1)
                .addStep(step2)
                .addSchedule(schedule)
                .setStartStepId(1)
            _ = try waitForAsync(timeout: TIMEOUT, description: "commit job") {
                try await builder.commit()
            }

            // Start and verify marker
            _ = try? waitForResult(agent.startJob(named: jobName), timeout: TIMEOUT, description: "start job")
            let t0 = Date()
            var ok = false
            while Date().timeIntervalSince(t0) < 20 {
                let rows = try waitForResult(conn.query("SELECT COUNT(*) AS c FROM tempdb.dbo.tds_builder_marker;"), timeout: TIMEOUT, description: "check marker")
                if (rows.first?.column("c")?.int ?? 0) > 0 { ok = true; break }
                Thread.sleep(forTimeInterval: 0.5)
            }
            XCTAssertTrue(ok, "Expected marker row after flow control")
        }
    }

    func testNotificationOperatorEnsure() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")
        try requireEnvFlag("TDS_ENABLE_AGENT_DEEP_TESTS", description: "SQL Agent deep lifecycle tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }
        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let metadata = SQLServerMetadataClient(connection: conn)
        let status = try waitForResult(metadata.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        if !status.isSqlAgentRunning { throw XCTSkip("Agent not running") }
        if !status.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE; EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)
        let jobName = "tds_builder_notify_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer { _ = try? waitForResult(agent.deleteJob(named: jobName), timeout: TIMEOUT, description: "cleanup job") }

        let step = SQLServerAgentJobStep(name: "s1", subsystem: .tsql, command: "SELECT 1;", database: "master")
        let opName = "tds_op_" + String(jobName.suffix(6))
        if #available(macOS 12.0, *) {
            let builder = SQLServerAgentJobBuilder(agent: agent, jobName: jobName, description: "notify", enabled: true)
                .addStep(step)
                .setNotification(SQLServerAgentJobNotification(operatorName: opName, level: .onFailure))
            _ = try waitForAsync(timeout: TIMEOUT, description: "commit job") {
                try await builder.commit()
            }

            // Verify notify level and operator mapping via msdb
            let rows = try waitForResult(conn.query("""
                SELECT j.notify_level_email, o.name AS operator_name
                FROM msdb.dbo.sysjobs AS j
                LEFT JOIN msdb.dbo.sysoperators AS o ON o.id = j.notify_email_operator_id
                WHERE j.name = N'\(jobName)'
            """), timeout: TIMEOUT, description: "verify notify")
            XCTAssertEqual(rows.first?.column("notify_level_email")?.int, 2)
            XCTAssertEqual(rows.first?.column("operator_name")?.string, opName)
        }
    }
}
