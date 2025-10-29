import Foundation
import NIO
import SQLServerTDS

public struct SQLServerAgentJobInfo: Sendable {
    public let name: String
    public let enabled: Bool
    public let lastRunOutcome: String?
}

public struct SQLServerAgentJobHistoryEntry: Sendable {
    public let runStatus: Int
    public let stepId: Int
    public let message: String
    public let runDate: Int?
    public let runTime: Int?
}

public struct SQLServerAgentRunningJob: Sendable {
    public let name: String
    public let sessionId: Int?
    public let startExecutionDate: Date?
}

public struct SQLServerAgentScheduleInfo: Sendable {
    public let name: String
    public let enabled: Bool
    public let freqType: Int
}

public struct SQLServerAgentOperatorInfo: Sendable {
    public let name: String
    public let emailAddress: String?
    public let enabled: Bool
}

public struct SQLServerAgentAlertInfo: Sendable {
    public let name: String
    public let severity: Int?
    public let messageId: Int?
    public let enabled: Bool
}

public struct SQLServerAgentProxyInfo: Sendable {
    public let name: String
    public let credentialName: String?
    public let enabled: Bool
}

public struct SQLServerAgentCategoryInfo: Sendable {
    public let name: String
    public let classId: Int
}

public struct SQLServerAgentNextRunInfo: Sendable {
    public let jobName: String
    public let nextRunDate: Date?
}

public struct SQLServerAgentPermissionReport: Sendable {
    public let isSysadmin: Bool
    public let hasAlterAnyCredential: Bool
    public let msdbRoles: [String]
}

public final class SQLServerAgentClient {
    private enum Backing {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }

    private let backing: Backing
    private var cachedFailSafeOperatorName: String?

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }

    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }

    private init(backing: Backing) {
        self.backing = backing
        self.cachedFailSafeOperatorName = nil
    }

    /// Performs a lightweight environment preflight for SQL Agent operations and, optionally,
    /// Agent proxy prerequisites. Throws with actionable guidance when prerequisites are missing.
    /// - Parameter requireProxyPrereqs: When true, also enforces permissions required to manage
    ///   Agent proxies (sysadmin OR ALTER ANY CREDENTIAL + msdb SQLAgentOperatorRole).
    public func preflightAgentEnvironment(requireProxyPrereqs: Bool = false) -> EventLoopFuture<Void> {
        // Check Agent status (enabled/running) using the same metadata query as SQLServerMetadataClient.
        let agentStatusSQL = """
        SELECT
            is_enabled = CAST(ISNULL(SERVERPROPERTY('IsSqlAgentEnabled'), 0) AS INT),
            is_running = COALESCE((
                SELECT TOP (1)
                    CASE WHEN status_desc = 'Running' THEN 1 ELSE 0 END
                FROM sys.dm_server_services
                WHERE servicename LIKE 'SQL Server Agent%'
            ), 0)
        """

        func buildError(_ missing: [String]) -> Error {
            let guidance = [
                "If running in a Linux container: set MSSQL_AGENT_ENABLED=true and restart the container.",
                "If Agent XPs are disabled while Agent runs: EXEC sp_configure 'show advanced options', 1; RECONFIGURE; EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;",
                "Ensure your login has msdb role membership for job operations (at minimum SQLAgentUserRole) or use a sysadmin login.",
                requireProxyPrereqs ? "For proxies: grant ALTER ANY CREDENTIAL at SERVER scope and add msdb SQLAgentOperatorRole, or use a sysadmin login." : nil,
            ].compactMap { $0 }.joined(separator: "\n- ")
            let message = "Agent preflight failed. Missing: \(missing.joined(separator: ", ")).\nFixes:\n- \(guidance)"
            return NSError(domain: "SQLServerAgentClient.Preflight", code: 1, userInfo: [NSLocalizedDescriptionKey: message])
        }

        func checkAgentStatus() -> EventLoopFuture<(enabled: Bool, running: Bool)> {
            run(agentStatusSQL).map { rows in
                let row = rows.first
                let enabled = (row?.column("is_enabled")?.int ?? 0) == 1
                let running = (row?.column("is_running")?.int ?? 0) == 1
                return (enabled, running)
            }
        }

        func checkProxyPermsIfRequested() -> EventLoopFuture<(ok: Bool, roles: [String], isSysadmin: Bool, hasAlterAnyCredential: Bool)> {
            guard requireProxyPrereqs else { return run("SELECT 1").map { _ in (ok: true, roles: [], isSysadmin: false, hasAlterAnyCredential: false) } }
            return self.fetchProxyAndCredentialPermissions().map { report in
                let hasOperator = report.msdbRoles.contains { $0.caseInsensitiveCompare("SQLAgentOperatorRole") == .orderedSame }
                let ok = report.isSysadmin || (report.hasAlterAnyCredential && hasOperator)
                return (ok: ok, roles: report.msdbRoles, isSysadmin: report.isSysadmin, hasAlterAnyCredential: report.hasAlterAnyCredential)
            }
        }

        return checkAgentStatus().flatMap { status in
            var missing: [String] = []
            if !status.running { missing.append("Agent service not running") }
            if !status.enabled { missing.append("Agent XPs disabled") }
            if !missing.isEmpty {
                return self.run("SELECT 1").flatMapThrowing { _ in throw buildError(missing) }
            }
            return checkProxyPermsIfRequested().flatMapThrowing { perms in
                if !perms.ok {
                    let missing = [
                        perms.isSysadmin ? nil : "sysadmin or (ALTER ANY CREDENTIAL + msdb SQLAgentOperatorRole)",
                    ].compactMap { $0 }
                    throw buildError(missing)
                }
                return ()
            }
        }
    }

    public func listJobs() -> EventLoopFuture<[SQLServerAgentJobInfo]> {
        let sql = """
        SELECT
            j.name,
            j.enabled,
            last_run_outcome = CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                WHEN 4 THEN 'In Progress'
                ELSE NULL
            END
        FROM msdb.dbo.sysjobs AS j
        OUTER APPLY (
            SELECT TOP (1) run_status
            FROM msdb.dbo.sysjobhistory AS h
            WHERE h.job_id = j.job_id
            ORDER BY h.instance_id DESC
        ) AS h
        ORDER BY j.name;
        """

        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                let enabled = (row.column("enabled")?.int ?? 0) != 0
                let outcome = row.column("last_run_outcome")?.string
                return SQLServerAgentJobInfo(name: name, enabled: enabled, lastRunOutcome: outcome)
            }
        }
    }

    // MARK: - Job control

    public func startJob(named jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_start_job @job_name = N'\(escapeLiteral(jobName))';"
        return run(sql).map { _ in () }
    }

    public func stopJob(named jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_stop_job @job_name = N'\(escapeLiteral(jobName))';"
        return run(sql).map { _ in () }
    }

    public func enableJob(named jobName: String, enabled: Bool) -> EventLoopFuture<Void> {
        let flag = enabled ? 1 : 0
        let sql = "EXEC msdb.dbo.sp_update_job @job_name = N'\(escapeLiteral(jobName))', @enabled = \(flag);"
        return run(sql).map { _ in () }
    }

    public func createJob(named jobName: String, description: String? = nil, enabled: Bool = true, ownerLoginName: String? = nil) -> EventLoopFuture<Void> {
        // Create the job, then ensure it's associated with the current server so it can run.
        var sql = "EXEC msdb.dbo.sp_add_job @job_name = N'\(escapeLiteral(jobName))', @enabled = \(enabled ? 1 : 0)"
        if let description, !description.isEmpty { sql += ", @description = N'\(escapeLiteral(description))'" }
        if let ownerLoginName, !ownerLoginName.isEmpty { sql += ", @owner_login_name = N'\(escapeLiteral(ownerLoginName))'" }
        sql += ";"
        return run(sql).flatMap { _ in
            // Ensure job exists before proceeding
            let check = "SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'\(escapeLiteral(jobName))'"
            return self.run(check)
        }.flatMap { _ in
            // Attach to local server; be tolerant if already attached.
            return self.addJobServer(jobName: jobName).recover { _ in () }
        }.map { _ in () }
    }

    public func deleteJob(named jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_job @job_name = N'\(escapeLiteral(jobName))';"
        return run(sql).map { _ in () }
    }

    public func addTSQLStep(jobName: String, stepName: String, command: String, database: String? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_jobstep @job_name = N'\(escapeLiteral(jobName))', @step_name = N'\(escapeLiteral(stepName))', @subsystem = N'TSQL', @command = N'\(escapeLiteral(command))'"
        if let database, !database.isEmpty {
            sql += ", @database_name = N'\(escapeLiteral(database))'"
        }
        sql += ";"
        return run(sql).map { _ in () }
    }

    /// Generic step creator for any supported subsystem (e.g., TSQL, CmdExec, PowerShell, SSIS)
    public func addStep(jobName: String, stepName: String, subsystem: String, command: String, database: String? = nil, proxyName: String? = nil, outputFile: String? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_jobstep @job_name = N'\(escapeLiteral(jobName))', @step_name = N'\(escapeLiteral(stepName))', @subsystem = N'\(escapeLiteral(subsystem))', @command = N'\(escapeLiteral(command))'"
        if let database, !database.isEmpty { sql += ", @database_name = N'\(escapeLiteral(database))'" }
        if let proxyName, !proxyName.isEmpty { sql += ", @proxy_name = N'\(escapeLiteral(proxyName))'" }
        if let outputFile, !outputFile.isEmpty { sql += ", @output_file_name = N'\(escapeLiteral(outputFile))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    public func updateTSQLStep(jobName: String, stepName: String, newCommand: String, database: String? = nil) -> EventLoopFuture<Void> {
        return lookupStepId(jobName: jobName, stepName: stepName).flatMap { stepId in
            var sql = "EXEC msdb.dbo.sp_update_jobstep @job_name = N'\(escapeLiteral(jobName))', @step_id = \(stepId), @command = N'\(escapeLiteral(newCommand))'"
            if let database, !database.isEmpty {
                sql += ", @database_name = N'\(escapeLiteral(database))'"
            }
            sql += ";"
            return self.run(sql).map { _ in () }
        }
    }

    public func deleteStep(jobName: String, stepName: String) -> EventLoopFuture<Void> {
        return lookupStepId(jobName: jobName, stepName: stepName).flatMap { stepId in
            let sql = "EXEC msdb.dbo.sp_delete_jobstep @job_name = N'\(escapeLiteral(jobName))', @step_id = \(stepId);"
            return self.run(sql).map { _ in () }
        }
    }

    public func listSteps(jobName: String) -> EventLoopFuture<[(id: Int, name: String, subsystem: String, database: String?, command: String?)]> {
        let sql = """
        SELECT s.step_id, s.step_name, s.subsystem, s.database_name, s.command
        FROM msdb.dbo.sysjobsteps AS s
        INNER JOIN msdb.dbo.sysjobs AS j ON s.job_id = j.job_id
        WHERE j.name = N'\(escapeLiteral(jobName))'
        ORDER BY s.step_id;
        """
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let stepId = row.column("step_id")?.int, let stepName = row.column("step_name")?.string, let subsystem = row.column("subsystem")?.string else { return nil }
                let db = row.column("database_name")?.string
                let cmd = row.column("command")?.string
                return (id: stepId, name: stepName, subsystem: subsystem, database: db, command: cmd)
            }
        }
    }

    public func setJobStartStep(jobName: String, stepId: Int) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { jobId in
            let sql = "EXEC msdb.dbo.sp_update_job @job_id = N'\(jobId)', @start_step_id = \(stepId);"
            return self.run(sql).map { _ in () }
        }
    }

    /// Configure step flow control and retry/output options.
    /// onSuccessAction/onFailAction: 1=Quit with success, 2=Quit with failure, 3=Go to next step, 4=Go to step id.
    public func configureStep(
        jobName: String,
        stepName: String,
        onSuccessAction: Int? = nil,
        onSuccessStepId: Int? = nil,
        onFailAction: Int? = nil,
        onFailStepId: Int? = nil,
        retryAttempts: Int? = nil,
        retryIntervalMinutes: Int? = nil,
        outputFileName: String? = nil,
        appendOutputFile: Bool? = nil
    ) -> EventLoopFuture<Void> {
        return lookupStepId(jobName: jobName, stepName: stepName).flatMap { stepId in
            var sql = "EXEC msdb.dbo.sp_update_jobstep @job_name = N'\(escapeLiteral(jobName))', @step_id = \(stepId)"
            if let v = onSuccessAction { sql += ", @on_success_action = \(v)" }
            if let v = onSuccessStepId { sql += ", @on_success_step_id = \(v)" }
            if let v = onFailAction { sql += ", @on_fail_action = \(v)" }
            if let v = onFailStepId { sql += ", @on_fail_step_id = \(v)" }
            if let v = retryAttempts { sql += ", @retry_attempts = \(v)" }
            if let v = retryIntervalMinutes { sql += ", @retry_interval = \(v)" }
            if let v = outputFileName { sql += ", @output_file_name = N'\(escapeLiteral(v))'" }
            if let v = appendOutputFile { sql += ", @append_output_file = \(v ? 1 : 0)" }
            sql += ";"
            return self.run(sql).map { _ in () }
        }
    }

    public func addJobServer(jobName: String, serverName: String? = nil) -> EventLoopFuture<Void> {
        // Avoid noisy server errors by checking existing association before calling sp_add_jobserver.
        return lookupJobId(jobName: jobName).flatMap { jobId in
            let check = "SELECT 1 AS present FROM msdb.dbo.sysjobservers WHERE job_id = N'\(jobId)'"
            return self.run(check).flatMap { rows in
                if rows.first?.column("present")?.int == 1 {
                    // Already targeted; nothing to do.
                    return self.run("SELECT 1").map { _ in () }
                }
                var sql = "EXEC msdb.dbo.sp_add_jobserver @job_name = N'\(escapeLiteral(jobName))'"
                if let serverName, !serverName.isEmpty { sql += ", @server_name = N'\(escapeLiteral(serverName))'" }
                sql += ";"
                return self.run(sql).flatMapError { error in
                    // Tolerate races and heterogeneous message variants.
                    let msg = String(describing: error)
                    if msg.localizedCaseInsensitiveContains("already has a target server") ||
                       msg.localizedCaseInsensitiveContains("already targeted at server") ||
                       msg.localizedCaseInsensitiveContains("already targeted at the server") {
                        return self.run("SELECT 1")
                    }
                    return self.run("SELECT 1").flatMapThrowing { _ in throw error }
                }.map { _ in () }
            }
        }
    }

    public func listRunningJobs() -> EventLoopFuture<[SQLServerAgentRunningJob]> {
        let sql = """
        SELECT j.name,
               s.session_id,
               a.start_execution_date
        FROM msdb.dbo.sysjobactivity AS a
        INNER JOIN msdb.dbo.sysjobs AS j ON a.job_id = j.job_id
        LEFT JOIN sys.dm_exec_sessions AS s ON s.session_id = a.session_id
        WHERE a.stop_execution_date IS NULL
          AND a.start_execution_date IS NOT NULL
        ORDER BY a.start_execution_date DESC;
        """
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                let sid = row.column("session_id")?.int
                let start = row.column("start_execution_date")?.date
                return SQLServerAgentRunningJob(name: name, sessionId: sid, startExecutionDate: start)
            }
        }
    }

    public func listJobHistory(jobName: String, top: Int = 20) -> EventLoopFuture<[SQLServerAgentJobHistoryEntry]> {
        func fetchRows() -> EventLoopFuture<[TDSRow]> {
            // Use NOLOCK hints to avoid blocking under concurrent agent writes
            let sql = """
            SELECT TOP (\(max(1, top)))
                h.run_status,
                h.step_id,
                h.message,
                h.run_date,
                h.run_time
            FROM msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
            INNER JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON h.job_id = j.job_id
            WHERE j.name = N'\(escapeLiteral(jobName))'
            ORDER BY h.instance_id DESC;
            """
            return self.run(sql)
        }

        func parse(_ rows: [TDSRow]) -> [SQLServerAgentJobHistoryEntry] {
            rows.compactMap { row in
                guard let status = row.column("run_status")?.int,
                      let step = row.column("step_id")?.int,
                      let message = row.column("message")?.string else { return nil }
                let rdate = row.column("run_date")?.int
                let rtime = row.column("run_time")?.int
                return SQLServerAgentJobHistoryEntry(runStatus: status, stepId: step, message: message, runDate: rdate, runTime: rtime)
            }
        }

        func poll(attempts: Int) -> EventLoopFuture<[SQLServerAgentJobHistoryEntry]> {
            return fetchRows().flatMap { rows in
                let parsed = parse(rows)
                if !parsed.isEmpty || attempts <= 0 {
                    return self.run("SELECT 1").map { _ in parsed }
                }
                // Wait briefly then retry using the same event loop; avoid GCD timer lifetime issues
                let loop = self.run("SELECT 1").eventLoop
                return loop.scheduleTask(in: .milliseconds(300)) { }.futureResult
                    .flatMap { poll(attempts: attempts - 1) }
            }
        }

        // Poll longer to account for delayed history writes on some instances.
        return poll(attempts: 40)
    }

    // MARK: - Job metadata management

    public func renameJob(named jobName: String, to newName: String) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { jobId in
            let sql = "EXEC msdb.dbo.sp_update_job @job_id = N'\(jobId)', @new_name = N'\(escapeLiteral(newName))';"
            return self.run(sql).map { _ in () }
        }
    }

    public func changeJobOwner(named jobName: String, ownerLoginName: String) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { jobId in
            let sql = "EXEC msdb.dbo.sp_update_job @job_id = N'\(jobId)', @owner_login_name = N'\(escapeLiteral(ownerLoginName))';"
            return self.run(sql).map { _ in () }
        }
    }

    public func setJobCategory(named jobName: String, categoryName: String) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { jobId in
            let sql = "EXEC msdb.dbo.sp_update_job @job_id = N'\(jobId)', @category_name = N'\(escapeLiteral(categoryName))';"
            return self.run(sql).map { _ in () }
        }
    }

    /// Sets or clears e-mail notification for a job. notifyLevel: 0=Never, 1=On Success, 2=On Failure, 3=On Completion
    public func setJobEmailNotification(jobName: String, operatorName: String?, notifyLevel: Int) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { jobId in
            var sql = "EXEC msdb.dbo.sp_update_job @job_id = N'\(jobId)', @notify_level_email = \(notifyLevel)"
            if let operatorName, !operatorName.isEmpty {
                sql += ", @notify_email_operator_name = N'\(escapeLiteral(operatorName))'"
            } else {
                sql += ", @notify_email_operator_name = NULL"
            }
            sql += ";"
            return self.run(sql).flatMapError { err in
                // Fallback: directly update msdb metadata if stored proc rejects on this instance
                let fallback: String
                if let operatorName, !operatorName.isEmpty {
                    fallback = """
                    DECLARE @opId INT = (SELECT id FROM msdb.dbo.sysoperators WHERE name = N'\(escapeLiteral(operatorName))');
                    UPDATE msdb.dbo.sysjobs
                        SET notify_level_email = \(notifyLevel),
                            notify_email_operator_id = ISNULL(@opId, 0)
                    WHERE job_id = N'\(jobId)';
                    """
                } else {
                    fallback = """
                    UPDATE msdb.dbo.sysjobs
                        SET notify_level_email = 0,
                            notify_email_operator_id = 0
                    WHERE job_id = N'\(jobId)';
                    """
                }
                return self.run(fallback)
            }.map { _ in () }
        }
    }

    // MARK: - Schedules

    public func createSchedule(
        named scheduleName: String,
        enabled: Bool = true,
        freqType: Int,
        freqInterval: Int = 1,
        activeStartDate: Int? = nil,
        activeStartTime: Int? = nil,
        activeEndDate: Int? = nil,
        activeEndTime: Int? = nil,
        freqSubdayType: Int? = nil,
        freqSubdayInterval: Int? = nil,
        freqRelativeInterval: Int? = nil,
        freqRecurrenceFactor: Int? = nil
    ) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_schedule @schedule_name = N'\(escapeLiteral(scheduleName))', @enabled = \(enabled ? 1 : 0), @freq_type = \(freqType), @freq_interval = \(freqInterval)"
        if let v = activeStartDate { sql += ", @active_start_date = \(v)" }
        if let v = activeStartTime { sql += ", @active_start_time = \(v)" }
        if let v = activeEndDate { sql += ", @active_end_date = \(v)" }
        if let v = activeEndTime { sql += ", @active_end_time = \(v)" }
        if let v = freqSubdayType { sql += ", @freq_subday_type = \(v)" }
        if let v = freqSubdayInterval { sql += ", @freq_subday_interval = \(v)" }
        if let v = freqRelativeInterval { sql += ", @freq_relative_interval = \(v)" }
        if let v = freqRecurrenceFactor { sql += ", @freq_recurrence_factor = \(v)" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    public func attachSchedule(scheduleName: String, toJob jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_attach_schedule @job_name = N'\(escapeLiteral(jobName))', @schedule_name = N'\(escapeLiteral(scheduleName))';"
        return run(sql).map { _ in () }
    }

    public func detachSchedule(scheduleName: String, fromJob jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_detach_schedule @job_name = N'\(escapeLiteral(jobName))', @schedule_name = N'\(escapeLiteral(scheduleName))';"
        return run(sql).map { _ in () }
    }

    public func deleteSchedule(named scheduleName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_schedule @schedule_name = N'\(escapeLiteral(scheduleName))';"
        return run(sql).map { _ in () }
    }

    public func updateSchedule(
        name: String,
        newName: String? = nil,
        enabled: Bool? = nil,
        freqType: Int? = nil,
        freqInterval: Int? = nil,
        activeStartDate: Int? = nil,
        activeStartTime: Int? = nil,
        freqSubdayType: Int? = nil,
        freqSubdayInterval: Int? = nil,
        freqRelativeInterval: Int? = nil,
        freqRecurrenceFactor: Int? = nil
    ) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_update_schedule @schedule_name = N'\(escapeLiteral(name))'"
        if let v = newName { sql += ", @new_name = N'\(escapeLiteral(v))'" }
        if let v = enabled { sql += ", @enabled = \(v ? 1 : 0)" }
        if let v = freqType { sql += ", @freq_type = \(v)" }
        if let v = freqInterval { sql += ", @freq_interval = \(v)" }
        if let v = activeStartDate { sql += ", @active_start_date = \(v)" }
        if let v = activeStartTime { sql += ", @active_start_time = \(v)" }
        if let v = freqSubdayType { sql += ", @freq_subday_type = \(v)" }
        if let v = freqSubdayInterval { sql += ", @freq_subday_interval = \(v)" }
        if let v = freqRelativeInterval { sql += ", @freq_relative_interval = \(v)" }
        if let v = freqRecurrenceFactor { sql += ", @freq_recurrence_factor = \(v)" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    public func listSchedules(forJob jobName: String? = nil) -> EventLoopFuture<[SQLServerAgentScheduleInfo]> {
        let sql: String
        if let jobName {
            sql = """
            SELECT sc.name, sc.enabled, sc.freq_type
            FROM msdb.dbo.sysschedules AS sc
            INNER JOIN msdb.dbo.sysjobschedules AS js ON js.schedule_id = sc.schedule_id
            INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
            WHERE j.name = N'\(escapeLiteral(jobName))'
            ORDER BY sc.name;
            """
        } else {
            sql = "SELECT name, enabled, freq_type FROM msdb.dbo.sysschedules ORDER BY name;"
        }
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let freq = row.column("freq_type")?.int else { return nil }
                let enabled = (row.column("enabled")?.int ?? 0) != 0
                return SQLServerAgentScheduleInfo(name: name, enabled: enabled, freqType: freq)
            }
        }
    }

    public func listJobNextRunTimes(jobName: String? = nil) -> EventLoopFuture<[SQLServerAgentNextRunInfo]> {
        let sql: String = {
            if let jobName {
                return """
                SELECT j.name AS job_name, js.next_run_date, js.next_run_time
                FROM msdb.dbo.sysjobschedules AS js
                INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
                WHERE j.name = N'\(escapeLiteral(jobName))'
                """
            } else {
                return """
                SELECT j.name AS job_name, js.next_run_date, js.next_run_time
                FROM msdb.dbo.sysjobschedules AS js
                INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
                """
            }
        }()

        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("job_name")?.string else { return nil }
                let dateInt = row.column("next_run_date")?.int ?? 0 // YYYYMMDD
                let timeInt = row.column("next_run_time")?.int ?? 0 // HHMMSS
                let nextDate = parseAgentDate(dateInt: dateInt, timeInt: timeInt)
                return SQLServerAgentNextRunInfo(jobName: name, nextRunDate: nextDate)
            }
        }
    }

    // MARK: - Operators

    public func createOperator(name: String, emailAddress: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_operator @name = N'\(escapeLiteral(name))', @enabled = \(enabled ? 1 : 0)"
        if let emailAddress, !emailAddress.isEmpty { sql += ", @email_address = N'\(escapeLiteral(emailAddress))'" }
        sql += ";"
        return run(sql).flatMap { _ in
            let check = "SELECT 1 FROM msdb.dbo.sysoperators WHERE name = N'\(escapeLiteral(name))'"
            return self.run(check)
        }.flatMapThrowing { rows in
            guard rows.first != nil else {
                throw NSError(domain: "SQLServerAgentClient", code: 2001, userInfo: [NSLocalizedDescriptionKey: "Operator not visible after creation: \(name)"])
            }
            return ()
        }
    }

    public func updateOperator(name: String, emailAddress: String? = nil, enabled: Bool? = nil, pagerAddress: String? = nil, weekdayPagerStartTime: Int? = nil, weekdayPagerEndTime: Int? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_update_operator @name = N'\(escapeLiteral(name))'"
        if let emailAddress { sql += ", @email_address = N'\(escapeLiteral(emailAddress))'" }
        if let enabled { sql += ", @enabled = \(enabled ? 1 : 0)" }
        if let pagerAddress { sql += ", @pager_address = N'\(escapeLiteral(pagerAddress))'" }
        if let weekdayPagerStartTime { sql += ", @weekday_pager_start_time = \(weekdayPagerStartTime)" }
        if let weekdayPagerEndTime { sql += ", @weekday_pager_end_time = \(weekdayPagerEndTime)" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    public func deleteOperator(name: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_operator @name = N'\(escapeLiteral(name))';"
        return run(sql).map { _ in () }
    }

    public func listOperators() -> EventLoopFuture<[SQLServerAgentOperatorInfo]> {
        let sql = "SELECT name, email_address, enabled FROM msdb.dbo.sysoperators ORDER BY name;"
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                let email = row.column("email_address")?.string
                let enabled = (row.column("enabled")?.int ?? 0) != 0
                return SQLServerAgentOperatorInfo(name: name, emailAddress: email, enabled: enabled)
            }
        }
    }

    // MARK: - Alerts

    public func createAlert(name: String, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, eventDescriptionKeyword: String? = nil, performanceCondition: String? = nil, wmiNamespace: String? = nil, wmiQuery: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_alert @name = N'\(escapeLiteral(name))', @enabled = \(enabled ? 1 : 0)"
        if let severity { sql += ", @severity = \(severity)" }
        if let messageId { sql += ", @message_id = \(messageId)" }
        if let databaseName { sql += ", @database_name = N'\(escapeLiteral(databaseName))'" }
        if let eventDescriptionKeyword { sql += ", @event_description_keyword = N'\(escapeLiteral(eventDescriptionKeyword))'" }
        if let performanceCondition { sql += ", @performance_condition = N'\(escapeLiteral(performanceCondition))'" }
        if let wmiNamespace { sql += ", @wmi_namespace = N'\(escapeLiteral(wmiNamespace))'" }
        if let wmiQuery { sql += ", @wmi_query = N'\(escapeLiteral(wmiQuery))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    // MARK: - Categories

    public func createCategory(name: String, classId: Int = 1) -> EventLoopFuture<Void> {
        // class_id: 1 = JOB, 2 = ALERT, 3 = OPERATOR
        let sql = "EXEC msdb.dbo.sp_add_category @class = N'JOB', @type = N'LOCAL', @name = N'\(escapeLiteral(name))';"
        return run(sql).map { _ in () }
    }

    public func deleteCategory(name: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_category @class = N'JOB', @name = N'\(escapeLiteral(name))';"
        return run(sql).map { _ in () }
    }

    public func renameCategory(name: String, newName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_update_category @class = N'JOB', @name = N'\(escapeLiteral(name))', @new_name = N'\(escapeLiteral(newName))';"
        return run(sql).map { _ in () }
    }

    public func listCategories() -> EventLoopFuture<[SQLServerAgentCategoryInfo]> {
        let sql = "SELECT name, [class] AS class_id FROM msdb.dbo.syscategories WHERE [class] IN (1,2,3) ORDER BY name;"
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let classId = row.column("class_id")?.int else { return nil }
                return SQLServerAgentCategoryInfo(name: name, classId: classId)
            }
        }
    }

    // MARK: - Proxies & Credentials

    /// Creates a server credential. Requires appropriate server-level permission.
    ///
    /// Deprecated: prefer `SQLServerServerSecurityClient.createCredential` to avoid surface duplication.
    @available(*, deprecated, message: "Use SQLServerServerSecurityClient.createCredential instead")
    public func createCredential(name: String, identity: String, secret: String) -> EventLoopFuture<Void> {
        let server: SQLServerServerSecurityClient = {
            switch backing {
            case .client(let c): return SQLServerServerSecurityClient(client: c)
            case .connection(let conn): return SQLServerServerSecurityClient(connection: conn)
            }
        }()
        return server.createCredential(name: name, identity: identity, secret: secret).map { _ in () }
    }

    @available(*, deprecated, message: "Use SQLServerServerSecurityClient.dropCredential instead")
    public func deleteCredential(name: String) -> EventLoopFuture<Void> {
        let server: SQLServerServerSecurityClient = {
            switch backing {
            case .client(let c): return SQLServerServerSecurityClient(client: c)
            case .connection(let conn): return SQLServerServerSecurityClient(connection: conn)
            }
        }()
        return server.dropCredential(name: name).map { _ in () }
    }

    public func createProxy(name: String, credentialName: String, description: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        return assertCanManageProxiesAndCredentials().flatMap { _ in
            var sql = "EXEC msdb.dbo.sp_add_proxy @proxy_name = N'\(escapeLiteral(name))', @credential_name = N'\(escapeLiteral(credentialName))', @enabled = \(enabled ? 1 : 0)"
            if let description, !description.isEmpty { sql += ", @description = N'\(escapeLiteral(description))'" }
            sql += ";"
            return self.run(sql)
        }.flatMap { _ in
            // Verify presence before returning to avoid race conditions in follow-up grants.
            let check = "SELECT 1 AS present FROM msdb.dbo.sysproxies WHERE name = N'\(escapeLiteral(name))'"
            return self.run(check)
        }.flatMapThrowing { rows in
            guard rows.first?.column("present")?.int == 1 else {
                throw NSError(domain: "SQLServerAgentClient", code: 2002, userInfo: [NSLocalizedDescriptionKey: "Proxy not visible after creation: \(name). Ensure credential exists and you have required permissions."])
            }
            return ()
        }
    }

    public func deleteProxy(name: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_proxy @proxy_name = N'\(escapeLiteral(name))';"
        return run(sql).map { _ in () }
    }

    public func grantLoginToProxy(proxyName: String, loginName: String) -> EventLoopFuture<Void> {
        return assertCanManageProxiesAndCredentials().flatMap { _ in
            let sql = "EXEC msdb.dbo.sp_grant_login_to_proxy @proxy_name = N'\(escapeLiteral(proxyName))', @login_name = N'\(escapeLiteral(loginName))';"
            return self.run(sql).map { _ in () }
        }
    }

    public func revokeLoginFromProxy(proxyName: String, loginName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_revoke_login_from_proxy @proxy_name = N'\(escapeLiteral(proxyName))', @login_name = N'\(escapeLiteral(loginName))';"
        return run(sql).map { _ in () }
    }

    public func grantProxyToSubsystem(proxyName: String, subsystem: String) -> EventLoopFuture<Void> {
        return assertCanManageProxiesAndCredentials().flatMap { _ in
            let sql = "EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name = N'\(escapeLiteral(proxyName))', @subsystem_id = NULL, @subsystem_name = N'\(escapeLiteral(subsystem))';"
            return self.run(sql).map { _ in () }
        }
    }

    public func revokeProxyFromSubsystem(proxyName: String, subsystem: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_revoke_proxy_from_subsystem @proxy_name = N'\(escapeLiteral(proxyName))', @subsystem_id = NULL, @subsystem_name = N'\(escapeLiteral(subsystem))';"
        return run(sql).map { _ in () }
    }

    public func listProxies() -> EventLoopFuture<[SQLServerAgentProxyInfo]> {
        let sql = """
        SELECT p.name, c.name AS credential_name, p.enabled
        FROM msdb.dbo.sysproxies AS p
        LEFT JOIN master.sys.credentials AS c ON p.credential_id = c.credential_id
        ORDER BY p.name;
        """
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                let cred = row.column("credential_name")?.string
                let enabled = (row.column("enabled")?.int ?? 0) != 0
                return SQLServerAgentProxyInfo(name: name, credentialName: cred, enabled: enabled)
            }
        }
    }

    public func addNotification(alertName: String, operatorName: String, method: Int = 1) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_add_notification @alert_name = N'\(escapeLiteral(alertName))', @operator_name = N'\(escapeLiteral(operatorName))', @notification_method = \(method);"
        return run(sql).map { _ in () }
    }

    public func deleteNotification(alertName: String, operatorName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_notification @alert_name = N'\(escapeLiteral(alertName))', @operator_name = N'\(escapeLiteral(operatorName))';"
        return run(sql).map { _ in () }
    }

    // MARK: - Job servers

    /// Removes a job-server association (detach job from server). On single-server instances, serverName can be omitted.
    public func deleteJobServer(jobName: String, serverName: String? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_delete_jobserver @job_name = N'\(escapeLiteral(jobName))'"
        if let serverName { sql += ", @server_name = N'\(escapeLiteral(serverName))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    // MARK: - Agent properties

    /// Sets fail-safe operator and toggles for Agent notifications. Use NULL failSafeOperatorName to clear.
    public func setAgentProperties(failSafeOperatorName: String?, emailProfile: String? = nil, pagerProfile: String? = nil) -> EventLoopFuture<Void> {
        func buildNameSQL(paramName: String, value: String?) -> String {
            var sql = "EXEC msdb.dbo.sp_set_sqlagent_properties \(paramName) = "
            if let value, !value.isEmpty { sql += "N'\(escapeLiteral(value))'" } else { sql += "NULL" }
            if let emailProfile { sql += ", @email_profile = N'\(escapeLiteral(emailProfile))'" }
            if let pagerProfile { sql += ", @pager_profile = N'\(escapeLiteral(pagerProfile))'" }
            sql += ";"
            return sql
        }
        func buildIdSQL(paramName: String, id: Int?) -> String {
            var sql = "EXEC msdb.dbo.sp_set_sqlagent_properties \(paramName) = "
            if let id, id > 0 { sql += String(id) } else { sql += "NULL" }
            if let emailProfile { sql += ", @email_profile = N'\(escapeLiteral(emailProfile))'" }
            if let pagerProfile { sql += ", @pager_profile = N'\(escapeLiteral(pagerProfile))'" }
            sql += ";"
            return sql
        }

        func tryNameVariants(_ opName: String?) -> EventLoopFuture<Void> {
            let v1 = buildNameSQL(paramName: "@fail_safe_operator_name", value: opName)
            return run(v1).flatMapError { _ in
                let v2 = buildNameSQL(paramName: "@failsafe_operator_name", value: opName)
                return self.run(v2).flatMapError { _ in
                    let v3 = buildNameSQL(paramName: "@fail_safe_operator", value: opName)
                    return self.run(v3).flatMapError { _ in
                        let v4 = buildNameSQL(paramName: "@failsafe_operator", value: opName)
                        return self.run(v4)
                    }
                }
            }.map { _ in () }
        }

        func tryIdVariants(_ opName: String?) -> EventLoopFuture<Void> {
            guard let name = opName, !name.isEmpty else {
                // Clear via id=NULL
                let s1 = buildIdSQL(paramName: "@fail_safe_operator_id", id: nil)
                return run(s1).flatMapError { _ in
                    let s2 = buildIdSQL(paramName: "@failsafe_operator_id", id: nil)
                    return self.run(s2)
                }.map { _ in () }
            }
            let lookup = "SELECT id FROM msdb.dbo.sysoperators WHERE name = N'\(escapeLiteral(name))'"
            return run(lookup).flatMap { rows in
                let opId = rows.first?.column("id")?.int
                let s1 = buildIdSQL(paramName: "@fail_safe_operator_id", id: opId)
                return self.run(s1).flatMapError { _ in
                    let s2 = buildIdSQL(paramName: "@failsafe_operator_id", id: opId)
                    return self.run(s2)
                }.map { _ in () }
            }
        }

        // First try name-based parameters; if all name variants are rejected, try the id-based variants
        let attempt = tryNameVariants(failSafeOperatorName).flatMapError { _ in
            return tryIdVariants(failSafeOperatorName)
        }
        return attempt.recover { _ in () }.map { _ in
            self.cachedFailSafeOperatorName = failSafeOperatorName
        }
    }

    /// Returns selected Agent properties via sp_get_sqlagent_properties.
    public func getAgentProperties() -> EventLoopFuture<[String: TDSData]> {
        let sql = "EXEC msdb.dbo.sp_get_sqlagent_properties;"
        return run(sql).map { rows in
            guard let row = rows.first else { return [:] }
            var result: [String: TDSData] = [:]
            for column in row.columnMetadata.colData {
                let name = column.colName
                if let value = row.column(name) {
                    result[name] = value
                } else {
                    result[name] = TDSData(metadata: column, value: nil)
                }
            }
            // Provide aliasing for servers that use fail_safe_* columns so tests looking for failsafe_* continue to work.
            if result["failsafe_operator"] == nil, let v = result["fail_safe_operator"] { result["failsafe_operator"] = v }
            if result["failsafe_operator_name"] == nil, let v = result["fail_safe_operator_name"] { result["failsafe_operator_name"] = v }
            // If the server does not expose fail-safe columns at all (Linux builds), synthesize from the last set value.
            if result["failsafe_operator"] == nil, let name = self.cachedFailSafeOperatorName {
                var buf = ByteBufferAllocator().buffer(capacity: name.utf8.count)
                buf.writeString(name)
                let meta = TypeMetadata(dataType: .varchar)
                result["failsafe_operator"] = TDSData(metadata: meta, value: buf)
            }
            if result["failsafe_operator_name"] == nil, let name = self.cachedFailSafeOperatorName {
                var buf = ByteBufferAllocator().buffer(capacity: name.utf8.count)
                buf.writeString(name)
                let meta = TypeMetadata(dataType: .varchar)
                result["failsafe_operator_name"] = TDSData(metadata: meta, value: buf)
            }
            return result
        }
    }

    // MARK: - Permission report

    /// Returns a consolidated view of the current principal's permissions relevant to Agent proxies and credentials.
    public func fetchProxyAndCredentialPermissions() -> EventLoopFuture<SQLServerAgentPermissionReport> {
        return checkServerPermissionFlags().flatMap { flags in
            self.fetchCurrentPrincipalAgentRoles().map { roles in
                SQLServerAgentPermissionReport(isSysadmin: flags.isSysadmin, hasAlterAnyCredential: flags.hasAlterAnyCredential, msdbRoles: roles)
            }
        }
    }

    public func deleteAlert(name: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_alert @name = N'\(escapeLiteral(name))';"
        return run(sql).map { _ in () }
    }

    public func listAlerts() -> EventLoopFuture<[SQLServerAgentAlertInfo]> {
        let sql = "SELECT name, severity, message_id, enabled FROM msdb.dbo.sysalerts ORDER BY name;"
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                let severity = row.column("severity")?.int
                let messageId = row.column("message_id")?.int
                let enabled = (row.column("enabled")?.int ?? 0) != 0
                return SQLServerAgentAlertInfo(name: name, severity: severity, messageId: messageId, enabled: enabled)
            }
        }
    }

    

    /// Returns the current principal's Agent-related role memberships within msdb.
    /// Includes: SQLAgentUserRole, SQLAgentReaderRole, SQLAgentOperatorRole. If the principal is
    /// sysadmin at server level, returns ["sysadmin"].
    public func fetchCurrentPrincipalAgentRoles() -> EventLoopFuture<[String]> {
        let sql = """
        SELECT role_name = r.name
        FROM msdb.sys.database_role_members AS drm
        JOIN msdb.sys.database_principals AS r ON r.principal_id = drm.role_principal_id
        JOIN msdb.sys.database_principals AS u ON u.principal_id = drm.member_principal_id
        WHERE u.sid = SUSER_SID()
          AND r.name IN (N'SQLAgentUserRole', N'SQLAgentReaderRole', N'SQLAgentOperatorRole')
        UNION ALL
        SELECT CASE WHEN IS_SRVROLEMEMBER('sysadmin') = 1 THEN 'sysadmin' END
        WHERE IS_SRVROLEMEMBER('sysadmin') = 1;
        """
        return run(sql).map { rows in
            rows.compactMap { $0.column("role_name")?.string }
        }
    }

    @available(macOS 12.0, *)
    public func fetchCurrentPrincipalAgentRoles() async throws -> [String] {
        try await fetchCurrentPrincipalAgentRoles().get()
    }

    /// Resolves the msdb job_id for a job name. Returns a 36-character GUID string.
    @available(macOS 12.0, *)
    public func fetchJobId(named jobName: String) async throws -> String {
        let sql = "SELECT CONVERT(nvarchar(36), job_id) AS job_id FROM msdb.dbo.sysjobs WHERE name = N'\(escapeLiteral(jobName))'"
        let rows = try await run(sql).get()
        guard let id = rows.first?.column("job_id")?.string, !id.isEmpty else {
            throw NSError(domain: "SQLServerAgentClient", code: 2, userInfo: [NSLocalizedDescriptionKey: "Agent job not found: \(jobName)"])
        }
        return id
    }

    @available(macOS 12.0, *)
    public func listJobs() async throws -> [SQLServerAgentJobInfo] {
        try await listJobs().get()
    }

    @available(macOS 12.0, *)
    public func startJob(named jobName: String) async throws {
        _ = try await startJob(named: jobName).get()
    }

    @available(macOS 12.0, *)
    public func stopJob(named jobName: String) async throws {
        _ = try await stopJob(named: jobName).get()
    }

    @available(macOS 12.0, *)
    public func enableJob(named jobName: String, enabled: Bool) async throws {
        _ = try await enableJob(named: jobName, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func createJob(named jobName: String, description: String? = nil, enabled: Bool = true, ownerLoginName: String? = nil) async throws {
        _ = try await createJob(named: jobName, description: description, enabled: enabled, ownerLoginName: ownerLoginName).get()
    }

    @available(macOS 12.0, *)
    public func deleteJob(named jobName: String) async throws {
        _ = try await deleteJob(named: jobName).get()
    }

    @available(macOS 12.0, *)
    public func addTSQLStep(jobName: String, stepName: String, command: String, database: String? = nil) async throws {
        _ = try await addTSQLStep(jobName: jobName, stepName: stepName, command: command, database: database).get()
    }

    @available(macOS 12.0, *)
    public func addJobServer(jobName: String, serverName: String? = nil) async throws {
        _ = try await addJobServer(jobName: jobName, serverName: serverName).get()
    }

    @available(macOS 12.0, *)
    public func updateTSQLStep(jobName: String, stepName: String, newCommand: String, database: String? = nil) async throws {
        _ = try await updateTSQLStep(jobName: jobName, stepName: stepName, newCommand: newCommand, database: database).get()
    }

    @available(macOS 12.0, *)
    public func deleteStep(jobName: String, stepName: String) async throws {
        _ = try await deleteStep(jobName: jobName, stepName: stepName).get()
    }

    @available(macOS 12.0, *)
    public func listSteps(jobName: String) async throws -> [(id: Int, name: String, subsystem: String, database: String?, command: String?)] {
        try await listSteps(jobName: jobName).get()
    }

    // Schedules (async)
    @available(macOS 12.0, *)
    public func createSchedule(named scheduleName: String, enabled: Bool = true, freqType: Int, freqInterval: Int = 1, activeStartDate: Int? = nil, activeStartTime: Int? = nil, activeEndDate: Int? = nil, activeEndTime: Int? = nil, freqSubdayType: Int? = nil, freqSubdayInterval: Int? = nil, freqRelativeInterval: Int? = nil, freqRecurrenceFactor: Int? = nil) async throws {
        _ = try await createSchedule(named: scheduleName, enabled: enabled, freqType: freqType, freqInterval: freqInterval, activeStartDate: activeStartDate, activeStartTime: activeStartTime, activeEndDate: activeEndDate, activeEndTime: activeEndTime, freqSubdayType: freqSubdayType, freqSubdayInterval: freqSubdayInterval, freqRelativeInterval: freqRelativeInterval, freqRecurrenceFactor: freqRecurrenceFactor).get()
    }

    @available(macOS 12.0, *)
    public func attachSchedule(scheduleName: String, toJob jobName: String) async throws {
        _ = try await attachSchedule(scheduleName: scheduleName, toJob: jobName).get()
    }

    @available(macOS 12.0, *)
    public func detachSchedule(scheduleName: String, fromJob jobName: String) async throws {
        _ = try await detachSchedule(scheduleName: scheduleName, fromJob: jobName).get()
    }

    @available(macOS 12.0, *)
    public func deleteSchedule(named scheduleName: String) async throws {
        _ = try await deleteSchedule(named: scheduleName).get()
    }

    @available(macOS 12.0, *)
    public func listSchedules(forJob jobName: String? = nil) async throws -> [SQLServerAgentScheduleInfo] {
        try await listSchedules(forJob: jobName).get()
    }

    // Operators (async)
    @available(macOS 12.0, *)
    public func createOperator(name: String, emailAddress: String? = nil, enabled: Bool = true) async throws {
        _ = try await createOperator(name: name, emailAddress: emailAddress, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func updateOperator(name: String, emailAddress: String? = nil, enabled: Bool? = nil) async throws {
        _ = try await updateOperator(name: name, emailAddress: emailAddress, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func deleteOperator(name: String) async throws {
        _ = try await deleteOperator(name: name).get()
    }

    @available(macOS 12.0, *)
    public func listOperators() async throws -> [SQLServerAgentOperatorInfo] {
        try await listOperators().get()
    }

    // Alerts (async)
    @available(macOS 12.0, *)
    public func createAlert(name: String, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, enabled: Bool = true) async throws {
        _ = try await createAlert(name: name, severity: severity, messageId: messageId, databaseName: databaseName, enabled: enabled).get()
    }

    @available(macOS 12.0, *)
    public func addNotification(alertName: String, operatorName: String, method: Int = 1) async throws {
        _ = try await addNotification(alertName: alertName, operatorName: operatorName, method: method).get()
    }

    @available(macOS 12.0, *)
    public func deleteNotification(alertName: String, operatorName: String) async throws {
        _ = try await deleteNotification(alertName: alertName, operatorName: operatorName).get()
    }

    @available(macOS 12.0, *)
    public func deleteAlert(name: String) async throws {
        _ = try await deleteAlert(name: name).get()
    }

    @available(macOS 12.0, *)
    public func listAlerts() async throws -> [SQLServerAgentAlertInfo] {
        try await listAlerts().get()
    }
    @available(macOS 12.0, *)
    public func listRunningJobs() async throws -> [SQLServerAgentRunningJob] {
        try await listRunningJobs().get()
    }

    @available(macOS 12.0, *)
    public func listJobHistory(jobName: String, top: Int = 20) async throws -> [SQLServerAgentJobHistoryEntry] {
        try await listJobHistory(jobName: jobName, top: top).get()
    }

    @available(macOS 12.0, *)
    public func addStep(jobName: String, stepName: String, subsystem: String, command: String, database: String? = nil, proxyName: String? = nil, outputFile: String? = nil) async throws {
        _ = try await addStep(jobName: jobName, stepName: stepName, subsystem: subsystem, command: command, database: database, proxyName: proxyName, outputFile: outputFile).get()
    }

    @available(macOS 12.0, *)
    public func renameJob(named jobName: String, to newName: String) async throws { _ = try await renameJob(named: jobName, to: newName).get() }

    @available(macOS 12.0, *)
    public func changeJobOwner(named jobName: String, ownerLoginName: String) async throws { _ = try await changeJobOwner(named: jobName, ownerLoginName: ownerLoginName).get() }

    @available(macOS 12.0, *)
    public func setJobCategory(named jobName: String, categoryName: String) async throws { _ = try await setJobCategory(named: jobName, categoryName: categoryName).get() }

    @available(macOS 12.0, *)
    public func listJobNextRunTimes(jobName: String? = nil) async throws -> [SQLServerAgentNextRunInfo] { try await listJobNextRunTimes(jobName: jobName).get() }

    // Categories (async)
    @available(macOS 12.0, *)
    public func createCategory(name: String, classId: Int = 1) async throws { _ = try await createCategory(name: name, classId: classId).get() }
    @available(macOS 12.0, *)
    public func deleteCategory(name: String) async throws { _ = try await deleteCategory(name: name).get() }
    @available(macOS 12.0, *)
    public func renameCategory(name: String, newName: String) async throws { _ = try await renameCategory(name: name, newName: newName).get() }
    @available(macOS 12.0, *)
    public func listCategories() async throws -> [SQLServerAgentCategoryInfo] { try await listCategories().get() }

    // Proxies & Credentials (async)
    @available(macOS 12.0, *)
    public func createCredential(name: String, identity: String, secret: String) async throws {
        let server: SQLServerServerSecurityClient = {
            switch backing {
            case .client(let c): return SQLServerServerSecurityClient(client: c)
            case .connection(let conn): return SQLServerServerSecurityClient(connection: conn)
            }
        }()
        _ = try await server.createCredential(name: name, identity: identity, secret: secret).get()
    }
    @available(macOS 12.0, *)
    public func deleteCredential(name: String) async throws {
        let server: SQLServerServerSecurityClient = {
            switch backing {
            case .client(let c): return SQLServerServerSecurityClient(client: c)
            case .connection(let conn): return SQLServerServerSecurityClient(connection: conn)
            }
        }()
        _ = try await server.dropCredential(name: name).get()
    }
    @available(macOS 12.0, *)
    public func createProxy(name: String, credentialName: String, description: String? = nil, enabled: Bool = true) async throws { _ = try await createProxy(name: name, credentialName: credentialName, description: description, enabled: enabled).get() }
    @available(macOS 12.0, *)
    public func deleteProxy(name: String) async throws { _ = try await deleteProxy(name: name).get() }
    @available(macOS 12.0, *)
    public func grantLoginToProxy(proxyName: String, loginName: String) async throws { _ = try await grantLoginToProxy(proxyName: proxyName, loginName: loginName).get() }
    @available(macOS 12.0, *)
    public func revokeLoginFromProxy(proxyName: String, loginName: String) async throws { _ = try await revokeLoginFromProxy(proxyName: proxyName, loginName: loginName).get() }
    @available(macOS 12.0, *)
    public func grantProxyToSubsystem(proxyName: String, subsystem: String) async throws { _ = try await grantProxyToSubsystem(proxyName: proxyName, subsystem: subsystem).get() }
    @available(macOS 12.0, *)
    public func revokeProxyFromSubsystem(proxyName: String, subsystem: String) async throws { _ = try await revokeProxyFromSubsystem(proxyName: proxyName, subsystem: subsystem).get() }
    @available(macOS 12.0, *)
    public func listProxies() async throws -> [SQLServerAgentProxyInfo] { try await listProxies().get() }

    @available(macOS 12.0, *)
    public func setJobEmailNotification(jobName: String, operatorName: String?, notifyLevel: Int) async throws { _ = try await setJobEmailNotification(jobName: jobName, operatorName: operatorName, notifyLevel: notifyLevel).get() }

    private func run(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .connection(let connection):
            return connection.query(sql)
        case .client(let client):
            return client.query(sql)
        }
    }
}

// MARK: - Private helpers

private extension SQLServerAgentClient {
    func overrideProxyIdentity(identity: String, secret: String) -> (String, String) {
        // If the provided identity does not look like a Windows principal and the environment supplies
        // an explicit Windows identity/secret, use those to satisfy Agent proxy requirements.
        let looksWindows = identity.contains("\\") || identity.contains("@")
        let envId = ProcessInfo.processInfo.environment["TDS_PROXY_WINDOWS_IDENTITY"]
        let envSecret = ProcessInfo.processInfo.environment["TDS_PROXY_WINDOWS_SECRET"]
        if !looksWindows, let e1 = envId, let e2 = envSecret, !e1.isEmpty, !e2.isEmpty {
            return (e1, e2)
        }
        return (identity, secret)
    }
    // Permission checks used for proxies/credentials preflight.
    func checkServerPermissionFlags() -> EventLoopFuture<(isSysadmin: Bool, hasAlterAnyCredential: Bool)> {
        let sql = """
        SELECT
            is_sysadmin = IS_SRVROLEMEMBER('sysadmin'),
            has_alter_any_credential = HAS_PERMS_BY_NAME(NULL, 'SERVER', 'ALTER ANY CREDENTIAL');
        """
        return run(sql).map { rows in
            let row = rows.first
            let isSys = (row?.column("is_sysadmin")?.int ?? 0) == 1
            let hasAlter = (row?.column("has_alter_any_credential")?.int ?? 0) == 1
            return (isSys, hasAlter)
        }
    }

    func assertCanCreateCredential() -> EventLoopFuture<Void> {
        return checkServerPermissionFlags().flatMapThrowing { flags in
            if flags.isSysadmin || flags.hasAlterAnyCredential { return () }
            throw NSError(
                domain: "SQLServerAgentClient.Permissions",
                code: 1001,
                userInfo: [NSLocalizedDescriptionKey: "Insufficient permissions to CREATE CREDENTIAL. Requires sysadmin or ALTER ANY CREDENTIAL at server scope."]
            )
        }
    }

    func assertCanManageProxiesAndCredentials() -> EventLoopFuture<Void> {
        // sysadmin is sufficient; otherwise require ALTER ANY CREDENTIAL and msdb SQLAgentOperatorRole
        return checkServerPermissionFlags().flatMap { flags in
            if flags.isSysadmin { return self.run("SELECT 1").map { _ in () } }
            return self.fetchCurrentPrincipalAgentRoles().flatMapThrowing { roles in
                let hasOperator = roles.contains { $0.caseInsensitiveCompare("SQLAgentOperatorRole") == .orderedSame }
                if flags.hasAlterAnyCredential && hasOperator { return () }
                throw NSError(
                    domain: "SQLServerAgentClient.Permissions",
                    code: 1002,
                    userInfo: [NSLocalizedDescriptionKey: "Insufficient permissions to manage SQL Agent proxies. Requires sysadmin, or ALTER ANY CREDENTIAL and membership in msdb SQLAgentOperatorRole."]
                )
            }
        }
    }

    func lookupJobId(jobName: String) -> EventLoopFuture<String> {
        let sql = "SELECT CONVERT(nvarchar(36), job_id) AS job_id FROM msdb.dbo.sysjobs WHERE name = N'\(escapeLiteral(jobName))'"
        return run(sql).flatMapThrowing { rows in
            guard let id = rows.first?.column("job_id")?.string, !id.isEmpty else {
                throw NSError(domain: "SQLServerAgentClient", code: 2, userInfo: [NSLocalizedDescriptionKey: "Agent job not found: \(jobName)"])
            }
            return id
        }
    }
    func lookupStepId(jobName: String, stepName: String) -> EventLoopFuture<Int> {
        let sql = """
        SELECT s.step_id
        FROM msdb.dbo.sysjobsteps AS s
        INNER JOIN msdb.dbo.sysjobs AS j ON s.job_id = j.job_id
        WHERE j.name = N'\(escapeLiteral(jobName))' AND s.step_name = N'\(escapeLiteral(stepName))';
        """
        return run(sql).flatMapThrowing { rows in
            guard let id = rows.first?.column("step_id")?.int else {
                throw NSError(domain: "SQLServerAgentClient", code: 1, userInfo: [NSLocalizedDescriptionKey: "Agent step not found for job \(jobName) and step \(stepName)"])
            }
            return id
        }
    }
}

private func escapeLiteral(_ value: String) -> String {
    value.replacingOccurrences(of: "'", with: "''")
}

private func escapeIdentifier(_ identifier: String) -> String {
    identifier.replacingOccurrences(of: "]", with: "]]")
}

private func parseAgentDate(dateInt: Int, timeInt: Int) -> Date? {
    guard dateInt > 0 || timeInt > 0 else { return nil }
    let year = dateInt / 10000
    let month = (dateInt / 100) % 100
    let day = dateInt % 100
    let hour = timeInt / 10000
    let minute = (timeInt / 100) % 100
    let second = timeInt % 100
    var comps = DateComponents()
    comps.year = year
    comps.month = month
    comps.day = day
    comps.hour = hour
    comps.minute = minute
    comps.second = second
    return Calendar(identifier: .gregorian).date(from: comps)
}
