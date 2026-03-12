import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    internal func run(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .connection(let connection):
            return connection.execute(sql).map(\.rawRows)
        case .client(let client):
            return client.withConnection { $0.execute(sql).map(\.rawRows) }
        }
    }

    internal func lookupJobId(jobName: String) -> EventLoopFuture<String> {
        let sql = "SELECT CONVERT(nvarchar(36), job_id) AS job_id FROM msdb.dbo.sysjobs WHERE name = N'\(Self.escapeLiteral(jobName))'"
        return run(sql).flatMapThrowing { rows in
            guard let id = rows.first?.column("job_id")?.string, !id.isEmpty else {
                throw NSError(domain: "SQLServerAgentOperations", code: 2, userInfo: [NSLocalizedDescriptionKey: "Agent job not found: \(jobName)"])
            }
            return id
        }
    }

    internal func lookupStepId(jobName: String, stepName: String) -> EventLoopFuture<Int> {
        let sql = """
        SELECT s.step_id
        FROM msdb.dbo.sysjobsteps AS s
        INNER JOIN msdb.dbo.sysjobs AS j ON s.job_id = j.job_id
        WHERE j.name = N'\(Self.escapeLiteral(jobName))' AND s.step_name = N'\(Self.escapeLiteral(stepName))';
        """
        return run(sql).flatMapThrowing { rows in
            guard let id = rows.first?.column("step_id")?.int else {
                throw NSError(domain: "SQLServerAgentOperations", code: 3, userInfo: [NSLocalizedDescriptionKey: "Agent job step not found: \(jobName).\(stepName)"])
            }
            return id
        }
    }

    internal func convertSqlDateTime(date: Int?, time: Int?) -> Date? {
        guard let date = date, date > 0 else { return nil }
        let time = time ?? 0
        let dateStr = String(format: "%08d", date)
        let timeStr = String(format: "%06d", time)
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMddHHmmss"
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter.date(from: dateStr + timeStr)
    }

    internal func parseAgentDate(dateInt: Int, timeInt: Int) -> Date? {
        guard dateInt > 0 else { return nil }
        return convertSqlDateTime(date: dateInt, time: timeInt)
    }

    internal func convertSqlDurationToSeconds(duration: Int?) -> Int? {
        guard let d = duration else { return nil }
        let hours = d / 10000
        let minutes = (d % 10000) / 100
        let seconds = d % 100
        return (hours * 3600) + (minutes * 60) + seconds
    }

    internal func getRunStatusDescription(_ status: Int) -> String {
        switch status {
        case 0: return "Failed"
        case 1: return "Succeeded"
        case 2: return "Retry"
        case 3: return "Canceled"
        case 4: return "In Progress"
        default: return "Unknown (\(status))"
        }
    }
}
