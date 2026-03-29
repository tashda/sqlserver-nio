import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Job Listing

    internal func listJobs() -> EventLoopFuture<[SQLServerAgentJobInfo]> {
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

    internal func listJobsDetailed() -> EventLoopFuture<[SQLServerAgentJobDetail]> {
        let jobSql = "EXEC msdb.dbo.sp_help_job;"
        let lastRunSql = """
        SELECT CONVERT(nvarchar(36), job_id) AS job_id, last_run_date, last_run_time
        FROM msdb.dbo.sysjobservers;
        """
        let nextRunSql = """
        SELECT CONVERT(nvarchar(36), job_id) AS job_id, next_run_date, next_run_time
        FROM msdb.dbo.sysjobschedules;
        """

        return run(lastRunSql).flatMap { lastRunRows in
            self.run(nextRunSql).flatMap { nextRunRows in
            var lastRunByJob: [String: Date] = [:]
            for row in lastRunRows {
                guard let jobId = row.column("job_id")?.string else { continue }
                if let dateValue = self.convertSqlDateTime(date: row.column("last_run_date")?.int, time: row.column("last_run_time")?.int) {
                    if let existing = lastRunByJob[jobId] {
                        if dateValue > existing { lastRunByJob[jobId] = dateValue }
                    } else {
                        lastRunByJob[jobId] = dateValue
                    }
                }
            }

            var nextRunByJob: [String: Date] = [:]
            for row in nextRunRows {
                guard let jobId = row.column("job_id")?.string else { continue }
                if let dateValue = self.convertSqlDateTime(date: row.column("next_run_date")?.int, time: row.column("next_run_time")?.int) {
                    if let existing = nextRunByJob[jobId] {
                        if dateValue < existing { nextRunByJob[jobId] = dateValue }
                    } else {
                        nextRunByJob[jobId] = dateValue
                    }
                }
            }

            let frozenLastRunByJob = lastRunByJob
            let frozenNextRunByJob = nextRunByJob

                return self.run(jobSql).map { rows in
                    rows.compactMap { row in
                        guard let name = row.column("name")?.string, let jobId = row.column("job_id")?.string else { return nil }
                        let enabled = (row.column("enabled")?.int ?? 0) != 0
                        let description = row.column("description")?.string ?? row.column("job_description")?.string
                        let ownerLoginName = row.column("owner_login_name")?.string ?? row.column("owner")?.string
                        let categoryName = row.column("category_name")?.string ?? row.column("category")?.string
                        let startStepId = row.column("start_step_id")?.int
                        let lastRunOutcome = row.column("last_run_outcome")?.string
                        let lastRunDateInt = row.column("last_run_date")?.int
                        let lastRunTimeInt = row.column("last_run_time")?.int
                        let nextRunDateInt = row.column("next_run_date")?.int
                        let nextRunTimeInt = row.column("next_run_time")?.int
                        let hasSchedule = (row.column("has_schedule")?.int ?? 0) != 0
                        let notifyLevelEmail = row.column("notify_level_email")?.int ?? 0
                        let notifyEmailOperator = row.column("notify_email_operator_name")?.string
                        let notifyLevelEventlog = row.column("notify_level_eventlog")?.int ?? 0

                        let lastRunDate = self.convertSqlDateTime(date: lastRunDateInt, time: lastRunTimeInt) ?? frozenLastRunByJob[jobId]
                        let nextRunDate = self.convertSqlDateTime(date: nextRunDateInt, time: nextRunTimeInt) ?? frozenNextRunByJob[jobId]

                        return SQLServerAgentJobDetail(jobId: jobId, name: name, description: description, enabled: enabled, ownerLoginName: ownerLoginName, categoryName: categoryName, startStepId: startStepId, lastRunOutcome: lastRunOutcome, lastRunDate: lastRunDate, nextRunDate: nextRunDate, hasSchedule: hasSchedule, notifyLevelEmail: notifyLevelEmail, notifyEmailOperatorName: notifyEmailOperator, notifyLevelEventlog: notifyLevelEventlog)
                    }
                }
            }
        }
    }

    // MARK: - Job Control

    internal func startJob(named jobName: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_start_job @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))';").map { _ in () }
    }

    internal func stopJob(named jobName: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_stop_job @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))';").map { _ in () }
    }

    internal func deleteJob(named jobName: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_job @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))';").map { _ in () }
    }

    internal func enableJob(named jobName: String, enabled: Bool) -> EventLoopFuture<Void> {
        let flag = enabled ? 1 : 0
        return run("EXEC msdb.dbo.sp_update_job @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))', @enabled = \(flag);").map { _ in () }
    }

    internal func createJob(named jobName: String, description: String? = nil, enabled: Bool = true, ownerLoginName: String? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_job @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))', @enabled = \(enabled ? 1 : 0)"
        if let description, !description.isEmpty { sql += ", @description = N'\(SQLServerSQL.escapeLiteral(description))'" }
        if let ownerLoginName, !ownerLoginName.isEmpty { sql += ", @owner_login_name = N'\(SQLServerSQL.escapeLiteral(ownerLoginName))'" }
        sql += ";"
        return run(sql).flatMap { (rows: [TDSRow]) -> EventLoopFuture<Void> in
            self.run("SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'\(SQLServerSQL.escapeLiteral(jobName))'").flatMap { (rows: [TDSRow]) -> EventLoopFuture<Void> in
                self.addJobServer(jobName: jobName).recover { _ in () }
            }
        }.map { _ in () }
    }

    internal func updateJob(named jobName: String, newName: String? = nil, description: String? = nil, ownerLoginName: String? = nil, categoryName: String? = nil, enabled: Bool? = nil, startStepId: Int? = nil, notifyLevelEventlog: Int? = nil) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { (jobId: String) -> EventLoopFuture<Void> in
            var parts: [String] = ["@job_id = N'\(jobId)'"]
            if let newName, !newName.isEmpty { parts.append("@new_name = N'\(SQLServerSQL.escapeLiteral(newName))'") }
            if let desc = description { parts.append("@description = N'\(SQLServerSQL.escapeLiteral(desc))'") }
            if let owner = ownerLoginName { parts.append("@owner_login_name = N'\(SQLServerSQL.escapeLiteral(owner))'") }
            if let category = categoryName { parts.append("@category_name = N'\(SQLServerSQL.escapeLiteral(category))'") }
            if let enabled { parts.append("@enabled = \(enabled ? 1 : 0)") }
            if let startStepId { parts.append("@start_step_id = \(startStepId)") }
            if let notifyLevelEventlog { parts.append("@notify_level_eventlog = \(notifyLevelEventlog)") }

            return self.run("EXEC msdb.dbo.sp_update_job \(parts.joined(separator: ", "));").map { _ in () }
        }
    }

    internal func renameJob(named jobName: String, to newName: String) -> EventLoopFuture<Void> {
        updateJob(named: jobName, newName: newName)
    }

    /// Sets or clears e-mail notification for a job. notifyLevel: 0=Never, 1=On Success, 2=On Failure, 3=On Completion
    internal func setJobEmailNotification(jobName: String, operatorName: String?, notifyLevel: Int) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { (jobId: String) -> EventLoopFuture<Void> in
            var sql = "EXEC msdb.dbo.sp_update_job @job_id = N'\(jobId)', @notify_level_email = \(notifyLevel)"
            if let operatorName, !operatorName.isEmpty {
                sql += ", @notify_email_operator_name = N'\(SQLServerSQL.escapeLiteral(operatorName))'"
            } else {
                sql += ", @notify_email_operator_name = NULL"
            }
            sql += ";"
            return self.run(sql).flatMapError { err -> EventLoopFuture<[TDSRow]> in
                // Fallback: directly update msdb metadata if stored proc rejects on this instance
                let fallback: String
                if let operatorName, !operatorName.isEmpty {
                    fallback = """
                    DECLARE @opId INT = (SELECT id FROM msdb.dbo.sysoperators WHERE name = N'\(SQLServerSQL.escapeLiteral(operatorName))');
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

    // MARK: - Steps

    internal func addTSQLStep(jobName: String, stepName: String, command: String, database: String? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_jobstep @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))', @step_name = N'\(SQLServerSQL.escapeLiteral(stepName))', @subsystem = N'TSQL', @command = N'\(SQLServerSQL.escapeLiteral(command))'"
        if let database, !database.isEmpty { sql += ", @database_name = N'\(SQLServerSQL.escapeLiteral(database))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func updateTSQLStep(jobName: String, stepName: String, newCommand: String, database: String? = nil) -> EventLoopFuture<Void> {
        return lookupStepId(jobName: jobName, stepName: stepName).flatMap { (stepId: Int) -> EventLoopFuture<Void> in
            var sql = "EXEC msdb.dbo.sp_update_jobstep @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))', @step_id = \(stepId), @command = N'\(SQLServerSQL.escapeLiteral(newCommand))'"
            if let database, !database.isEmpty { sql += ", @database_name = N'\(SQLServerSQL.escapeLiteral(database))'" }
            sql += ";"
            return self.run(sql).map { _ in () }
        }
    }

    internal func addStep(jobName: String, stepName: String, subsystem: String, command: String, database: String? = nil, proxyName: String? = nil, outputFile: String? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_jobstep @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))', @step_name = N'\(SQLServerSQL.escapeLiteral(stepName))', @subsystem = N'\(SQLServerSQL.escapeLiteral(subsystem))', @command = N'\(SQLServerSQL.escapeLiteral(command))'"
        if let database, !database.isEmpty { sql += ", @database_name = N'\(SQLServerSQL.escapeLiteral(database))'" }
        if let proxyName, !proxyName.isEmpty { sql += ", @proxy_name = N'\(SQLServerSQL.escapeLiteral(proxyName))'" }
        if let outputFile, !outputFile.isEmpty { sql += ", @output_file_name = N'\(SQLServerSQL.escapeLiteral(outputFile))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func deleteStep(jobName: String, stepName: String) -> EventLoopFuture<Void> {
        return lookupStepId(jobName: jobName, stepName: stepName).flatMap { (stepId: Int) -> EventLoopFuture<Void> in
            self.run("EXEC msdb.dbo.sp_delete_jobstep @job_name = N'\(SQLServerSQL.escapeLiteral(jobName))', @step_id = \(stepId);").map { _ in () }
        }
    }

    internal func listSteps(jobName: String) -> EventLoopFuture<[SQLServerAgentJobStepDetail]> {
        let sql = """
        SELECT s.step_id, s.step_name, s.subsystem, s.database_name, s.command,
               s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id,
               s.retry_attempts, s.retry_interval
        FROM msdb.dbo.sysjobsteps AS s
        INNER JOIN msdb.dbo.sysjobs AS j ON s.job_id = j.job_id
        WHERE j.name = N'\(SQLServerSQL.escapeLiteral(jobName))'
        ORDER BY s.step_id;
        """
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let stepId = row.column("step_id")?.int,
                      let stepName = row.column("step_name")?.string,
                      let subsystem = row.column("subsystem")?.string else { return nil }
                return SQLServerAgentJobStepDetail(
                    stepId: stepId,
                    name: stepName,
                    subsystem: subsystem,
                    command: row.column("command")?.string,
                    databaseName: row.column("database_name")?.string,
                    onSuccessAction: row.column("on_success_action")?.int,
                    onSuccessStepId: row.column("on_success_step_id")?.int,
                    onFailureAction: row.column("on_fail_action")?.int,
                    onFailureStepId: row.column("on_fail_step_id")?.int,
                    retryAttempts: row.column("retry_attempts")?.int,
                    retryIntervalMinutes: row.column("retry_interval")?.int
                )
            }
        }
    }

    // MARK: - History

    internal func getJobHistory(jobName: String? = nil, top: Int = 100) -> EventLoopFuture<[SQLServerAgentJobHistoryDetail]> {
        var params: [String] = ["@mode = 'FULL'"]
        if let jobName { params.append("@job_name = N'\(SQLServerSQL.escapeLiteral(jobName))'") }
        let sql = "EXEC msdb.dbo.sp_help_jobhistory \(params.joined(separator: ", "));"
        return run(sql).map { rows in
            let entries = rows.compactMap { row -> SQLServerAgentJobHistoryDetail? in
                guard let instanceId = row.column("instance_id")?.int, let jobName = row.column("job_name")?.string, let stepId = row.column("step_id")?.int, let runStatus = row.column("run_status")?.int else { return nil }
                let runDateTime = self.convertSqlDateTime(date: row.column("run_date")?.int, time: row.column("run_time")?.int)
                let runDurationSeconds = self.convertSqlDurationToSeconds(duration: row.column("run_duration")?.int)
                return SQLServerAgentJobHistoryDetail(instanceId: instanceId, jobName: jobName, stepId: stepId, stepName: row.column("step_name")?.string, runStatus: runStatus, runStatusDescription: self.getRunStatusDescription(runStatus), message: row.column("message")?.string ?? "", runDateTime: runDateTime, runDurationSeconds: runDurationSeconds)
            }
            return (top > 0 && entries.count > top) ? Array(entries.prefix(top)) : entries
        }
    }

    internal func reorderJobSteps(jobName: String, stepMapping: [(oldID: Int, newID: Int)]) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { (jobId: String) -> EventLoopFuture<Void> in
            let caseParts = stepMapping.map { "WHEN \($0.oldID) THEN \($0.newID)" }.joined(separator: " ")
            let successCase = "CASE on_success_step_id \(caseParts) ELSE on_success_step_id END"
            let failCase = "CASE on_fail_step_id \(caseParts) ELSE on_fail_step_id END"

            let individualUpdates = stepMapping.map {
                "UPDATE msdb.dbo.sysjobsteps SET step_id = \($0.newID) WHERE job_id = CONVERT(uniqueidentifier, N'\(jobId)') AND step_id = \($0.oldID + 10000);"
            }.joined(separator: "\n")

            let startStepCase = "CASE start_step_id \(caseParts) ELSE start_step_id END"

            let sql = """
            BEGIN TRANSACTION;

            UPDATE msdb.dbo.sysjobsteps
            SET on_success_step_id = \(successCase),
                on_fail_step_id = \(failCase)
            WHERE job_id = CONVERT(uniqueidentifier, N'\(jobId)');

            UPDATE msdb.dbo.sysjobsteps
            SET step_id = step_id + 10000
            WHERE job_id = CONVERT(uniqueidentifier, N'\(jobId)');

            \(individualUpdates)

            UPDATE msdb.dbo.sysjobs
            SET start_step_id = \(startStepCase)
            WHERE job_id = CONVERT(uniqueidentifier, N'\(jobId)');

            COMMIT;
            """
            
            return self.run(sql).map { _ in () }
        }
    }
}
