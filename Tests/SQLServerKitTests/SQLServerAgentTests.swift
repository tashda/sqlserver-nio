import Foundation
import NIO
@testable import SQLServerKit
import XCTest

final class SQLServerAgentTests: XCTestCase {
    let TIMEOUT: TimeInterval = Double(env("TDS_TEST_OPERATION_TIMEOUT_SECONDS") ?? "30") ?? 30

    func testAgentStatusViaMetadata() throws {
        loadEnvFileIfPresent()

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let metadata = SQLServerMetadataClient(connection: conn)
        var status = try waitForResult(metadata.fetchAgentStatus(), timeout: TIMEOUT, description: "fetch agent status")

        // Basic sanity: values should be boolean-like
        XCTAssertTrue([false, true].contains(status.isSqlAgentEnabled), "isSqlAgentEnabled must be boolean")
        XCTAssertTrue([false, true].contains(status.isSqlAgentRunning), "isSqlAgentRunning must be boolean")

        // Some environments start SQL Agent but leave Agent XPs disabled. If so, enable them and re-check.
        if status.isSqlAgentRunning && !status.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable advanced options")
            _ = try waitForResult(conn.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable Agent XPs")
        }

        // Re-check and only then run the preflight (to avoid failing before XPs are enabled).
        status = try waitForResult(metadata.fetchAgentStatus(), timeout: TIMEOUT, description: "fetch agent status (refreshed)")
        // Preflight environment and surface actionable guidance after attempted enable. Use softFail to make advisory-only here.
        _ = try? assertAgentPreflight(conn, requireProxyPrereqs: false, timeout: TIMEOUT, softFail: true)
        XCTAssertTrue(status.isSqlAgentRunning, "SQL Agent service must be running on test server")
    }

    func testAgentClientJobLifecycle() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        // Skip if the agent service is not running
        let metadata = SQLServerMetadataClient(connection: conn)
        let agentStatus = try waitForResult(metadata.fetchAgentStatus(), timeout: TIMEOUT, description: "fetch agent status")
        // Fail fast with actionable guidance if Agent/XPs/roles are not in place.
        // Advisory-only preflight prior to attempting to enable XPs
        _ = try? assertAgentPreflight(conn, timeout: TIMEOUT, softFail: true)
        if !agentStatus.isSqlAgentRunning {
            throw XCTSkip("Not applicable: SQL Server Agent service not running on target instance")
        }

        let jobName = "tds_agentcli_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let stepName = "step_main"

        func runSql(_ sql: String, description: String) throws {
            _ = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: description)
        }

        // Ensure Agent XPs are enabled when the service is running, required to start jobs.
        if !agentStatus.isSqlAgentEnabled {
            try runSql("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;", description: "enable advanced options")
            try runSql("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;", description: "enable Agent XPs")
        }

        // Create a job that inserts into a tempdb marker table.
        try runSql("""
            EXEC msdb.dbo.sp_add_job
                @job_name = N'\(jobName)',
                @enabled = 1,
                @description = N'tds-nio agent client test job';
            """ , description: "create agent job")

        defer {
            _ = try? waitForResult(conn.query("EXEC msdb.dbo.sp_delete_job @job_name = N'\(jobName)';"), timeout: TIMEOUT, description: "cleanup job")
        }

        try runSql("""
            EXEC msdb.dbo.sp_add_jobstep
                @job_name = N'\(jobName)',
                @step_name = N'\(stepName)',
                @subsystem = N'TSQL',
                @command = N'SET NOCOUNT ON; INSERT INTO tempdb.dbo.tds_agent_marker DEFAULT VALUES;',
                @database_name = N'master';
            """, description: "add job step")

        try runSql("EXEC msdb.dbo.sp_add_jobserver @job_name = N'\(jobName)';", description: "attach job server")

        try runSql("IF OBJECT_ID('tempdb.dbo.tds_agent_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_marker;", description: "drop agent marker table")
        try runSql("CREATE TABLE tempdb.dbo.tds_agent_marker (id INT IDENTITY(1,1) PRIMARY KEY);", description: "create agent marker table")
        defer {
            _ = try? waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_marker;"), timeout: TIMEOUT, description: "cleanup agent marker table")
        }

        // Exercise the SQLServerAgentClient
        let agentClient = SQLServerAgentClient(connection: conn)
        // Fetch current principal Agent role memberships (best-effort)
        _ = try? waitForResult(agentClient.fetchCurrentPrincipalAgentRoles(), timeout: TIMEOUT, description: "fetch roles")
        let jobs = try waitForResult(agentClient.listJobs(), timeout: TIMEOUT, description: "list agent jobs")
        XCTAssertTrue(jobs.contains(where: { $0.name == jobName }), "Created job should appear in Agent job list")

        try waitForResult(agentClient.startJob(named: jobName), timeout: TIMEOUT, description: "start agent job via client")

        // Observe completion either via history or marker table
        func markerCount() throws -> Int {
            let rows = try waitForResult(conn.query("SELECT COUNT(*) AS cnt FROM tempdb.dbo.tds_agent_marker;"), timeout: TIMEOUT, description: "check marker rows")
            return rows.first?.column("cnt")?.int ?? 0
        }

        let deadline = Date().addingTimeInterval(30)
        var completed = false
        while Date() < deadline {
            if try markerCount() >= 1 {
                completed = true
                break
            }
            Thread.sleep(forTimeInterval: 1)
        }

        if !completed {
            XCTFail("Agent job did not complete within allotted time.")
        }
    }

    func testAgentClientFullJobControlAndHistory() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")
        try requireEnvFlag("TDS_ENABLE_AGENT_DEEP_TESTS", description: "SQL Agent deep lifecycle tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "fetch agent status")
        if !state.isSqlAgentRunning {
            throw XCTSkip("Not applicable: SQL Server Agent service not running on target instance")
        }

        // Ensure Agent XPs are enabled when the service is running
        do {
            func runSql(_ sql: String, description: String) throws {
                _ = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: description)
            }
            if !state.isSqlAgentEnabled {
                try runSql("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;", description: "enable advanced options")
                try runSql("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;", description: "enable Agent XPs")
            }
        }

        // After attempting to enable XPs, run an advisory preflight to surface any residual guidance.
        _ = try? assertAgentPreflight(conn, timeout: TIMEOUT, softFail: true)

        func runSql(_ sql: String, description: String) throws {
            _ = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: description)
        }

        let jobName = "tds_agentcli_full_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let stepName = "step_wait"
        let agent = SQLServerAgentClient(connection: conn)

        // Create job disabled, then enable later
        try waitForResult(agent.createJob(named: jobName, description: "full control test", enabled: false), timeout: TIMEOUT, description: "create job")
        defer { _ = try? waitForResult(agent.deleteJob(named: jobName), timeout: TIMEOUT, description: "cleanup job") }

        // Created as disabled; verify state via listJobs
        do {
            let jobs = try waitForResult(agent.listJobs(), timeout: TIMEOUT, description: "list jobs pre-enable")
            XCTAssertEqual(jobs.first(where: { $0.name == jobName })?.enabled, false)
        }

        try waitForResult(agent.addTSQLStep(jobName: jobName, stepName: stepName, command: "SET NOCOUNT ON; WAITFOR DELAY '00:00:03';"), timeout: TIMEOUT, description: "add step")
        // Update step command to a shorter delay and verify via listSteps
        try waitForResult(agent.updateTSQLStep(jobName: jobName, stepName: stepName, newCommand: "SET NOCOUNT ON; WAITFOR DELAY '00:00:02';"), timeout: TIMEOUT, description: "update step")
        do {
            let steps = try waitForResult(agent.listSteps(jobName: jobName), timeout: TIMEOUT, description: "list steps")
            XCTAssertTrue(steps.contains(where: { $0.name == stepName && ($0.command ?? "").contains("00:00:02") }))
        }
        do {
            try waitForResult(agent.addJobServer(jobName: jobName), timeout: TIMEOUT, description: "attach server")
        } catch {
            // Some environments auto-target the local server; ignore duplicate target errors
        }

        // Disabled job should not be startable; some servers still allow start while disabled, so just toggle enabled state explicitly
        try waitForResult(agent.enableJob(named: jobName, enabled: true), timeout: TIMEOUT, description: "enable job")

        // Start job then quickly observe it in running list
        try waitForResult(agent.startJob(named: jobName), timeout: TIMEOUT, description: "start job")

        var seenRunning = false
        let t0 = Date()
        while Date().timeIntervalSince(t0) < 10 {
            let running = try waitForResult(agent.listRunningJobs(), timeout: TIMEOUT, description: "list running")
            if running.contains(where: { $0.name == jobName }) {
                seenRunning = true
                break
            }
            Thread.sleep(forTimeInterval: 0.5)
        }
        XCTAssertTrue(seenRunning, "Expected job to appear in running list after start")

        // Try to stop; if it already finished, this might fail with "is not currently running" which is fine
        do {
            try waitForResult(agent.stopJob(named: jobName), timeout: TIMEOUT, description: "stop job")
        } catch {
            // Ignore if job already completed
        }

        // Wait for job to be absent from running list to avoid racing history writes
        do {
            let settleStart = Date()
            while Date().timeIntervalSince(settleStart) < 15 {
                let running = try waitForResult(agent.listRunningJobs(), timeout: TIMEOUT, description: "list running (settle)")
                if !running.contains(where: { $0.name == jobName }) { break }
                Thread.sleep(forTimeInterval: 0.5)
            }
        } catch {
            // Best-effort; proceed to history even if this check fails
        }

        // Read history and assert at least one entry exists
        let history = try waitForResult(agent.listJobHistory(jobName: jobName, top: 5), timeout: TIMEOUT, description: "history")
        XCTAssertFalse(history.isEmpty, "Expected at least one history entry for job")

        // Delete step to cover removal path
        _ = try? waitForResult(agent.deleteStep(jobName: jobName, stepName: stepName), timeout: TIMEOUT, description: "delete step")

        // Disable again, verify flag, and delete
        try waitForResult(agent.enableJob(named: jobName, enabled: false), timeout: TIMEOUT, description: "disable job")
        do {
            let jobs = try waitForResult(agent.listJobs(), timeout: TIMEOUT, description: "list jobs post-disable")
            XCTAssertEqual(jobs.first(where: { $0.name == jobName })?.enabled, false)
        }
    }

    func testAgentSchedulesAttachDetach() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_SCHEDULE_TESTS", description: "SQL Agent schedule tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        // Advisory-only preflight prior to attempting to enable XPs
        _ = try? assertAgentPreflight(conn, timeout: TIMEOUT, softFail: true)
        if !state.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        // Ensure Agent XPs are enabled when the service is running
        if !state.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable advanced options")
            _ = try waitForResult(conn.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable Agent XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)
        let job = "tds_agent_sched_job_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let schedule = "tds_agent_sched_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer {
            _ = try? waitForResult(agent.deleteSchedule(named: schedule), timeout: TIMEOUT, description: "delete schedule")
            _ = try? waitForResult(agent.deleteJob(named: job), timeout: TIMEOUT, description: "delete job")
        }

        try waitForResult(agent.createJob(named: job, description: "schedule attach test", enabled: true), timeout: TIMEOUT, description: "create job")
        try waitForResult(agent.addTSQLStep(jobName: job, stepName: "main", command: "SELECT 1;", database: "master"), timeout: TIMEOUT, description: "add step")

        // Create a simple daily schedule (freq_type=4) starting at midnight
        try waitForResult(agent.createSchedule(named: schedule, enabled: true, freqType: 4, freqInterval: 1, activeStartTime: 0), timeout: TIMEOUT, description: "create schedule")
        try waitForResult(agent.attachSchedule(scheduleName: schedule, toJob: job), timeout: TIMEOUT, description: "attach schedule")

        do {
            let schedules = try waitForResult(agent.listSchedules(forJob: job), timeout: TIMEOUT, description: "list job schedules")
            XCTAssertTrue(schedules.contains(where: { $0.name == schedule }))
        }

        try waitForResult(agent.detachSchedule(scheduleName: schedule, fromJob: job), timeout: TIMEOUT, description: "detach schedule")
        do {
            let schedules = try waitForResult(agent.listSchedules(forJob: job), timeout: TIMEOUT, description: "list job schedules after detach")
            XCTAssertFalse(schedules.contains(where: { $0.name == schedule }))
        }
    }

    func testAgentOperatorsAndAlerts() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_ALERT_TESTS", description: "SQL Agent operator/alert tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        // Advisory-only preflight prior to attempting to enable XPs
        _ = try? assertAgentPreflight(conn, timeout: TIMEOUT, softFail: true)
        if !state.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        let agent = SQLServerAgentClient(connection: conn)
        let opName = "tds_operator_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let alertName = "tds_alert_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")

        defer {
            _ = try? waitForResult(agent.deleteNotification(alertName: alertName, operatorName: opName), timeout: TIMEOUT, description: "delete notification")
            _ = try? waitForResult(agent.deleteAlert(name: alertName), timeout: TIMEOUT, description: "delete alert")
            _ = try? waitForResult(agent.deleteOperator(name: opName), timeout: TIMEOUT, description: "delete operator")
        }

        try waitForResult(agent.createOperator(name: opName, emailAddress: "devnull@example.com", enabled: true), timeout: TIMEOUT, description: "create operator")
        do {
            let ops = try waitForResult(agent.listOperators(), timeout: TIMEOUT, description: "list operators")
            XCTAssertTrue(ops.contains(where: { $0.name == opName }))
        }

        // Create alert based on severity 1 (no firing expected in test)
        try waitForResult(agent.createAlert(name: alertName, severity: 1, enabled: true), timeout: TIMEOUT, description: "create alert")
        do {
            let alerts = try waitForResult(agent.listAlerts(), timeout: TIMEOUT, description: "list alerts")
            XCTAssertTrue(alerts.contains(where: { $0.name == alertName }))
        }

        // Connect alert to operator via notification
        try waitForResult(agent.addNotification(alertName: alertName, operatorName: opName, method: 1), timeout: TIMEOUT, description: "add notification")
    }

    func testAgentPropertiesFailSafeOperator() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_ALERT_TESTS", description: "SQL Agent properties tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        // Advisory-only preflight prior to attempting to enable XPs
        _ = try? assertAgentPreflight(conn, timeout: TIMEOUT, softFail: true)
        if !state.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        // Ensure Agent XPs are enabled when the service is running
        if !state.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable advanced options")
            _ = try waitForResult(conn.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable Agent XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)
        let opName = "tds_fail_op_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer { _ = try? waitForResult(agent.deleteOperator(name: opName), timeout: TIMEOUT, description: "delete operator") }
        _ = try waitForResult(agent.createOperator(name: opName, emailAddress: "devnull@example.com"), timeout: TIMEOUT, description: "create operator")

        _ = try waitForResult(agent.setAgentProperties(failSafeOperatorName: opName), timeout: TIMEOUT, description: "set failsafe operator")
        let props = try waitForResult(agent.getAgentProperties(), timeout: TIMEOUT, description: "get agent properties")
        // sp_get_sqlagent_properties returns columns like failsafe_operator
        let failSafe = props["failsafe_operator"]?.string ?? props["failsafe_operator_name"]?.string
        XCTAssertEqual(failSafe, opName)
        _ = try? waitForResult(agent.setAgentProperties(failSafeOperatorName: nil), timeout: TIMEOUT, description: "clear failsafe")
    }

    func testAgentStartFromSpecificStepAndFlowControl() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")
        try requireEnvFlag("TDS_ENABLE_AGENT_DEEP_TESTS", description: "SQL Agent deep flow-control tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        if !state.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        // Ensure Agent XPs are enabled when the service is running
        if !state.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable advanced options")
            _ = try waitForResult(conn.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable Agent XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)
        // For proxy coverage, prefer advisory preflight so Linux/non-proxy environments aren't marked failed.
        _ = try? assertAgentPreflight(conn, requireProxyPrereqs: true, timeout: TIMEOUT, softFail: true)
        let job = "tds_agent_flow_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer { _ = try? waitForResult(agent.deleteJob(named: job), timeout: TIMEOUT, description: "delete job") }

        // Create job with two steps: step1 fails, step2 inserts marker. Configure step1 to go to step2 on failure.
        _ = try waitForResult(agent.createJob(named: job, description: "flow control test", enabled: true), timeout: TIMEOUT, description: "create job")

        _ = try waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_flow_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_flow_marker; CREATE TABLE tempdb.dbo.tds_agent_flow_marker (id INT IDENTITY(1,1) PRIMARY KEY);"), timeout: TIMEOUT, description: "setup marker table")
        defer { _ = try? waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_flow_marker', 'U') IS NOT NULL DROP TABLE tempdb.dbo.tds_agent_flow_marker;"), timeout: TIMEOUT, description: "drop marker table") }

        _ = try waitForResult(agent.addTSQLStep(jobName: job, stepName: "step1_fail", command: "RAISERROR('boom',16,1);", database: "master"), timeout: TIMEOUT, description: "add step1")
        _ = try waitForResult(agent.addTSQLStep(jobName: job, stepName: "step2_ok", command: "INSERT INTO tempdb.dbo.tds_agent_flow_marker DEFAULT VALUES;", database: "master"), timeout: TIMEOUT, description: "add step2")

        // Get step ids
        let steps = try waitForResult(agent.listSteps(jobName: job), timeout: TIMEOUT, description: "list steps")
        let id1 = steps.first(where: { $0.name == "step1_fail" })?.id ?? 1
        let id2 = steps.first(where: { $0.name == "step2_ok" })?.id ?? 2

        _ = try waitForResult(agent.configureStep(jobName: job, stepName: "step1_fail", onSuccessAction: 1, onFailAction: 4, onFailStepId: id2, retryAttempts: 0), timeout: TIMEOUT, description: "configure flow")
        _ = try waitForResult(agent.setJobStartStep(jobName: job, stepId: id1), timeout: TIMEOUT, description: "set start step")

        // Start explicitly from step2 then verify marker
        _ = try waitForResult(conn.query("IF OBJECT_ID('tempdb.dbo.tds_agent_flow_marker', 'U') IS NOT NULL DELETE FROM tempdb.dbo.tds_agent_flow_marker;"), timeout: TIMEOUT, description: "clear marker")
        _ = try waitForResult(conn.query("EXEC msdb.dbo.sp_start_job @job_name = N'\(job)', @step_name = N'step2_ok';"), timeout: TIMEOUT, description: "start from step2")

        let begin = Date()
        var ok = false
        while Date().timeIntervalSince(begin) < 20 {
            let rows = try waitForResult(conn.query("SELECT COUNT(*) AS c FROM tempdb.dbo.tds_agent_flow_marker;"), timeout: TIMEOUT, description: "check marker")
            if (rows.first?.column("c")?.int ?? 0) > 0 { ok = true; break }
            Thread.sleep(forTimeInterval: 0.5)
        }
        XCTAssertTrue(ok, "Expected marker after starting from step2")

        // Now start normally; step1 fails but flows to step2
        _ = try waitForResult(conn.query("DELETE FROM tempdb.dbo.tds_agent_flow_marker;"), timeout: TIMEOUT, description: "reset marker")
        _ = try waitForResult(agent.startJob(named: job), timeout: TIMEOUT, description: "start job")
        let begin2 = Date()
        ok = false
        while Date().timeIntervalSince(begin2) < 20 {
            let rows = try waitForResult(conn.query("SELECT COUNT(*) AS c FROM tempdb.dbo.tds_agent_flow_marker;"), timeout: TIMEOUT, description: "check marker2")
            if (rows.first?.column("c")?.int ?? 0) > 0 { ok = true; break }
            Thread.sleep(forTimeInterval: 0.5)
        }
        XCTAssertTrue(ok, "Expected marker after failing step1 then step2")
    }

    func testAgentJobCategoryRenameOwnerNotificationAndNextRun() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_CATEGORY_TESTS", description: "SQL Agent category/rename/owner/notification tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        if !state.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        func run(_ sql: String, _ desc: String) throws { _ = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: desc) }

        let agent = SQLServerAgentClient(connection: conn)
        let categoryName = "tds_cat_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let jobName = "tds_cat_job_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let jobNewName = jobName + "_ren"
        let opName = "tds_cat_op_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")

        defer {
            _ = try? waitForResult(agent.deleteJob(named: jobNewName), timeout: TIMEOUT, description: "delete job")
            _ = try? waitForResult(agent.deleteJob(named: jobName), timeout: TIMEOUT, description: "delete job old")
            _ = try? waitForResult(agent.deleteCategory(name: categoryName), timeout: TIMEOUT, description: "delete category")
            _ = try? waitForResult(agent.deleteOperator(name: opName), timeout: TIMEOUT, description: "delete operator")
        }

        // Create job and category
        try waitForResult(agent.createJob(named: jobName, description: "category test", enabled: true), timeout: TIMEOUT, description: "create job")
        try waitForResult(agent.createCategory(name: categoryName), timeout: TIMEOUT, description: "create category")
        try waitForResult(agent.setJobCategory(named: jobName, categoryName: categoryName), timeout: TIMEOUT, description: "set job category")

        // Rename job
        try waitForResult(agent.renameJob(named: jobName, to: jobNewName), timeout: TIMEOUT, description: "rename job")

        // Change owner to current login
        let loginRows = try waitForResult(conn.query("SELECT SUSER_SNAME() AS name"), timeout: TIMEOUT, description: "get current login")
        let currentLogin = loginRows.first?.column("name")?.string ?? "sa"
        try waitForResult(agent.changeJobOwner(named: jobNewName, ownerLoginName: currentLogin), timeout: TIMEOUT, description: "change owner")

        // Verify mappings via msdb metadata
        do {
            let rows = try waitForResult(conn.query("""
                SELECT j.name, cat.name AS category_name, SUSER_SNAME(j.owner_sid) AS owner_name
                FROM msdb.dbo.sysjobs AS j
                LEFT JOIN msdb.dbo.syscategories AS cat ON cat.category_id = j.category_id
                WHERE j.name = N'\(jobNewName)'
            """), timeout: TIMEOUT, description: "verify job category/owner")
            XCTAssertEqual(rows.first?.column("category_name")?.string, categoryName)
            XCTAssertEqual(rows.first?.column("owner_name")?.string, currentLogin)
        }

        // Create operator and set job email notification (level 2 = failure)
        try waitForResult(agent.createOperator(name: opName, emailAddress: "devnull@example.com"), timeout: TIMEOUT, description: "create operator")
        try waitForResult(agent.setJobEmailNotification(jobName: jobNewName, operatorName: opName, notifyLevel: 2), timeout: TIMEOUT, description: "set job notification")

        do {
            let rows = try waitForResult(conn.query("""
                SELECT j.notify_level_email, o.name AS operator_name
                FROM msdb.dbo.sysjobs AS j
                LEFT JOIN msdb.dbo.sysoperators AS o ON o.id = j.notify_email_operator_id
                WHERE j.name = N'\(jobNewName)'
            """), timeout: TIMEOUT, description: "verify job notification")
            XCTAssertEqual(rows.first?.column("notify_level_email")?.int, 2)
            XCTAssertEqual(rows.first?.column("operator_name")?.string, opName)
        }

        // Clear notification
        try waitForResult(agent.setJobEmailNotification(jobName: jobNewName, operatorName: nil, notifyLevel: 0), timeout: TIMEOUT, description: "clear notification")

        // Create and attach schedule; then fetch next run time
        let schedule = "tds_cat_sched_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer { _ = try? waitForResult(agent.deleteSchedule(named: schedule), timeout: TIMEOUT, description: "delete schedule") }
        try waitForResult(agent.createSchedule(named: schedule, enabled: true, freqType: 4, freqInterval: 1, activeStartTime: 0), timeout: TIMEOUT, description: "create schedule")
        try waitForResult(agent.attachSchedule(scheduleName: schedule, toJob: jobNewName), timeout: TIMEOUT, description: "attach schedule")
        let nextRuns = try waitForResult(agent.listJobNextRunTimes(jobName: jobNewName), timeout: TIMEOUT, description: "list next run times")
        XCTAssertTrue(nextRuns.contains(where: { $0.jobName == jobNewName }))
    }

    func testAgentProxiesAndCredentialsMetadata() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_PROXY_TESTS", description: "SQL Agent proxy/credential tests (requires elevated perms)")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        // Skip on Linux hosts where SQL Agent proxies require Windows user principals
        do {
            let rows = try waitForResult(conn.query("SELECT host_platform FROM sys.dm_os_host_info"), timeout: TIMEOUT, description: "host platform")
            if let platform = rows.first?.column("host_platform")?.string, platform.lowercased().contains("linux") {
                throw XCTSkip("Skipping Agent proxy tests on Linux host; proxies require Windows user principals.")
            }
        } catch { /* ignore if DMV unavailable */ }

        // Also require explicit Windows identity/secret to reduce accidental runs without prerequisites
        if (env("TDS_PROXY_WINDOWS_IDENTITY") ?? "").isEmpty || (env("TDS_PROXY_WINDOWS_SECRET") ?? "").isEmpty {
            throw XCTSkip("Skipping Agent proxy tests; missing TDS_PROXY_WINDOWS_IDENTITY/SECRET.")
        }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "agent status")
        if !state.isSqlAgentRunning { throw XCTSkip("Agent not running") }

        let agent = SQLServerAgentClient(connection: conn)
        let login = "tds_proxy_login_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let pwd = "P@ssw0rd!aA1"
        let cred = "tds_cred_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let proxy = "tds_proxy_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")

        defer {
            _ = try? waitForResult(agent.deleteProxy(name: proxy), timeout: TIMEOUT, description: "delete proxy")
            _ = try? waitForResult(agent.deleteCredential(name: cred), timeout: TIMEOUT, description: "delete credential")
            _ = try? waitForResult(conn.query("IF SUSER_ID(N'\(login)') IS NOT NULL DROP LOGIN [\(login)];"), timeout: TIMEOUT, description: "drop login")
        }

        // Create login and supporting credential
        _ = try waitForResult(conn.query("IF SUSER_ID(N'\(login)') IS NULL CREATE LOGIN [\(login)] WITH PASSWORD = N'\(pwd)', CHECK_POLICY = OFF;"), timeout: TIMEOUT, description: "create login")
        _ = try waitForResult(agent.createCredential(name: cred, identity: "id_\(login)", secret: "s3cr3t!"), timeout: TIMEOUT, description: "create credential")

        // Create proxy and grant login + subsystem
        _ = try waitForResult(agent.createProxy(name: proxy, credentialName: cred, description: "test proxy", enabled: true), timeout: TIMEOUT, description: "create proxy")
        _ = try waitForResult(agent.grantLoginToProxy(proxyName: proxy, loginName: login), timeout: TIMEOUT, description: "grant login to proxy")
        _ = try waitForResult(agent.grantProxyToSubsystem(proxyName: proxy, subsystem: "CmdExec"), timeout: TIMEOUT, description: "grant proxy to subsystem")

        // Verify proxy shows up in metadata
        do {
            let proxies = try waitForResult(agent.listProxies(), timeout: TIMEOUT, description: "list proxies")
            XCTAssertTrue(proxies.contains(where: { $0.name == proxy && $0.credentialName == cred }))
        }

        // Revoke and cleanup handled by defer
    }

    func testAgentListRunningJobs() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_TESTS", description: "SQL Agent integration tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        let meta = SQLServerMetadataClient(connection: conn)
        let state = try waitForResult(meta.fetchAgentStatus(), timeout: TIMEOUT, description: "fetch agent status")
        // Advisory-only preflight prior to attempting to enable XPs
        _ = try? assertAgentPreflight(conn, timeout: TIMEOUT, softFail: true)
        if !state.isSqlAgentRunning {
            throw XCTSkip("Not applicable: SQL Server Agent service not running on target instance")
        }

        // Ensure Agent XPs are enabled when the service is running
        if !state.isSqlAgentEnabled {
            _ = try waitForResult(conn.query("EXEC sp_configure 'show advanced options', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable advanced options")
            _ = try waitForResult(conn.query("EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;"), timeout: TIMEOUT, description: "enable Agent XPs")
        }

        let agent = SQLServerAgentClient(connection: conn)
        let jobName = "tds_agentcli_running_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")

        // Create a job with a brief wait so it shows up in running list
        try waitForResult(agent.createJob(named: jobName, description: "running list test", enabled: true), timeout: TIMEOUT, description: "create job")
        defer { _ = try? waitForResult(agent.deleteJob(named: jobName), timeout: TIMEOUT, description: "cleanup job") }
        try waitForResult(agent.addTSQLStep(jobName: jobName, stepName: "step_wait", command: "WAITFOR DELAY '00:00:05';"), timeout: TIMEOUT, description: "add step")
        try waitForResult(agent.addJobServer(jobName: jobName), timeout: TIMEOUT, description: "attach server")

        try waitForResult(agent.startJob(named: jobName), timeout: TIMEOUT, description: "start job")

        var seen = false
        let begin = Date()
        while Date().timeIntervalSince(begin) < 10 {
            let running = try waitForResult(agent.listRunningJobs(), timeout: TIMEOUT, description: "list running")
            if running.contains(where: { $0.name == jobName }) { seen = true; break }
            Thread.sleep(forTimeInterval: 0.5)
        }

        XCTAssertTrue(seen, "Expected job to be visible in running jobs")
    }

    func testAgentPermissionsRoles() throws {
        loadEnvFileIfPresent()
        try requireEnvFlag("TDS_ENABLE_AGENT_SECURITY_TESTS", description: "SQL Agent security role tests")

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let conn = try waitForResult(connectSQLServer(on: group.next()), timeout: TIMEOUT, description: "connect")
        defer { _ = try? waitForResult(conn.close(), timeout: TIMEOUT, description: "close") }

        func run(_ sql: String, description: String) throws {
            _ = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: description)
        }

        let login = "tds_agent_user_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let pwd = "P@ssw0rd!aA1"
        let jobName = "tds_agent_sec_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defer {
            _ = try? waitForResult(conn.query("USE msdb; IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'\(login)') DROP USER [\(login)];"), timeout: TIMEOUT, description: "drop msdb user")
            _ = try? waitForResult(conn.query("IF SUSER_ID(N'\(login)') IS NOT NULL DROP LOGIN [\(login)];"), timeout: TIMEOUT, description: "drop login")
        }

        try run("IF SUSER_ID(N'\(login)') IS NULL CREATE LOGIN [\(login)] WITH PASSWORD = N'\(pwd)', CHECK_POLICY = OFF;", description: "create login")
        try run("USE msdb; IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'\(login)') CREATE USER [\(login)] FOR LOGIN [\(login)];", description: "create msdb user")

        // Attempt to create a job while impersonated, expect failure prior to role membership
        do {
            let sql = """
            EXECUTE AS LOGIN = N'\(login)';
            BEGIN TRY
                EXEC msdb.dbo.sp_add_job @job_name = N'\(jobName)';
                SELECT outcome = 'succeeded';
            END TRY
            BEGIN CATCH
                SELECT outcome = 'failed';
            END CATCH;
            REVERT;
            """
            let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "impersonated create job attempt")
            let outcome = rows.last?.column("outcome")?.string
            XCTAssertEqual(outcome, "failed", "Expected job creation to fail without Agent role membership")
        }

        // Grant minimal Agent role and retry create with ownership set to that login
        try run("USE msdb; EXEC sp_addrolemember N'SQLAgentUserRole', N'\(login)';", description: "grant Agent role")
        // Verify membership recorded in msdb
        do {
            let rows = try waitForResult(conn.query("""
                SELECT COUNT(*) AS cnt
                FROM msdb.sys.database_role_members AS drm
                JOIN msdb.sys.database_principals AS r ON r.principal_id = drm.role_principal_id
                JOIN msdb.sys.database_principals AS u ON u.principal_id = drm.member_principal_id
                WHERE r.name = N'SQLAgentUserRole' AND u.name = N'\(login)';
            """), timeout: TIMEOUT, description: "verify role membership")
            XCTAssertEqual(rows.first?.column("cnt")?.int, 1)
        }

        do {
            let sql = """
            EXECUTE AS LOGIN = N'\(login)';
            BEGIN TRY
                EXEC msdb.dbo.sp_add_job @job_name = N'\(jobName)', @owner_login_name = N'\(login)';
                SELECT outcome = 'succeeded';
            END TRY
            BEGIN CATCH
                SELECT outcome = 'failed';
            END CATCH;
            REVERT;
            """
            let rows = try waitForResult(conn.query(sql), timeout: TIMEOUT, description: "impersonated create job as owner")
            let outcome = rows.last?.column("outcome")?.string
            XCTAssertEqual(outcome, "succeeded", "Expected job creation to succeed with Agent role membership")
        }

        // Cleanup any leftover job
        _ = try? waitForResult(conn.query("EXEC msdb.dbo.sp_delete_job @job_name = N'\(jobName)';"), timeout: TIMEOUT, description: "cleanup job")
    }
}
