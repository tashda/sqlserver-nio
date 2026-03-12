import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Runtime Activity

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
                return SQLServerAgentRunningJob(name: name, sessionId: row.column("session_id")?.int, startExecutionDate: row.column("start_execution_date")?.date)
            }
        }
    }

    public func listJobHistory(jobName: String, top: Int = 20) -> EventLoopFuture<[SQLServerAgentJobHistoryEntry]> {
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
}
