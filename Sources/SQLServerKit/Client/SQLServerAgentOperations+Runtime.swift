import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Runtime Activity

    internal func listRunningJobs() -> EventLoopFuture<[SQLServerAgentRunningJob]> {
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
                return SQLServerAgentRunningJob(name: name, sessionId: row.column("session_id")?.int, startExecutionDate: row.column("start_execution_date")?.date)
            }
        }
    }

    internal func listJobHistory(jobName: String, top: Int = 20) -> EventLoopFuture<[SQLServerAgentJobHistoryEntry]> {
        @Sendable
        func fetchRows() -> EventLoopFuture<[TDSRow]> {
            let sql = """
            SELECT TOP (\(max(1, top)))
                h.run_status,
                h.step_id,
                h.message,
                h.run_date,
                h.run_time
            FROM msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
            INNER JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON h.job_id = j.job_id
            WHERE j.name = N'\(Self.escapeLiteral(jobName))'
            ORDER BY h.instance_id DESC;
            """
            return self.run(sql)
        }

        @Sendable
        func parse(_ rows: [TDSRow]) -> [SQLServerAgentJobHistoryEntry] {
            rows.compactMap { row in
                guard let status = row.column("run_status")?.int,
                      let step = row.column("step_id")?.int,
                      let message = row.column("message")?.string else { return nil }
                return SQLServerAgentJobHistoryEntry(runStatus: status, stepId: step, message: message, runDate: row.column("run_date")?.int, runTime: row.column("run_time")?.int)
            }
        }

        @Sendable
        func poll(attempts: Int) -> EventLoopFuture<[SQLServerAgentJobHistoryEntry]> {
            return fetchRows().flatMap { rows in
                let parsed = parse(rows)
                if !parsed.isEmpty || attempts <= 0 {
                    return self.run("SELECT 1").map { _ in parsed }
                }
                let loop = self.run("SELECT 1").eventLoop
                return loop.scheduleTask(in: .milliseconds(300)) { }.futureResult
                    .flatMap { poll(attempts: attempts - 1) }
            }
        }

        return poll(attempts: 40)
    }

    /// Returns the currently executing step for a specific running job, or `nil` if the job is not running.
    internal func getActiveJobStep(jobName: String) -> EventLoopFuture<SQLServerAgentActiveStep?> {
        let sql = """
        SELECT
            j.name AS job_name,
            a.last_executed_step_id,
            s.step_name,
            a.start_execution_date
        FROM msdb.dbo.sysjobactivity AS a
        INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = a.job_id
        LEFT JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = a.job_id AND s.step_id = a.last_executed_step_id
        WHERE j.name = N'\(Self.escapeLiteral(jobName))'
          AND a.start_execution_date IS NOT NULL
          AND a.stop_execution_date IS NULL
          AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)
        ORDER BY a.start_execution_date DESC;
        """
        return run(sql).map { rows in
            guard let row = rows.first else { return nil }
            let name = row.column("job_name")?.string ?? jobName
            let stepId = row.column("last_executed_step_id")?.int ?? 0
            let stepName = row.column("step_name")?.string
            let startDate = row.column("start_execution_date")?.date
            return SQLServerAgentActiveStep(jobName: name, lastExecutedStepId: stepId, stepName: stepName, startExecutionDate: startDate)
        }
    }

    internal func listErrorLogs() -> EventLoopFuture<[SQLServerAgentErrorLog]> {
        run("EXEC xp_enumerrorlogs 2;").map { rows in
            rows.compactMap { row in
                let columns = row.data
                // xp_enumerrorlogs columns can vary, typically: Archive #, Date, Log File Size (Byte)
                guard columns.count >= 2 else { return nil }
                
                let archiveStr = columns[0].string ?? "0"
                let digitsOnly = archiveStr.unicodeScalars.filter { CharacterSet.decimalDigits.contains($0) }
                let archive = Int(String(String.UnicodeScalarView(digitsOnly))) ?? 0
                
                let date = columns[1].string ?? ""
                let size = columns.count >= 3 ? columns[2].string : nil
                
                return SQLServerAgentErrorLog(archiveNumber: archive, date: date, size: size)
            }
        }
    }
}
