import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

@available(*, deprecated, renamed: "SQLServerAgentOperations")
public typealias SQLServerAgentClient = SQLServerAgentOperations

public final class SQLServerAgentOperations: Sendable {
    internal enum Backing: Sendable {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }

    internal let backing: Backing
    private let cachedFailSafeOperatorNameBox = NIOLockedValueBox<String?>(nil)

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }

    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }

    internal init(backing: Backing) {
        self.backing = backing
    }

    /// Performs a lightweight environment preflight for SQL Agent operations and, optionally,
    /// Agent proxy prerequisites. Throws with actionable guidance when prerequisites are missing.
    /// - Parameter requireProxyPrereqs: When true, also enforces permissions required to manage
    ///   Agent proxies (sysadmin OR ALTER ANY CREDENTIAL + msdb SQLAgentOperatorRole).
    public func preflightAgentEnvironment(requireProxyPrereqs: Bool = false) -> EventLoopFuture<Void> {
        // Check Agent status (enabled/running) using the same metadata query as SQLServerMetadataOperations.
        let agentStatusSQL = """
        SELECT
            is_enabled = CAST(ISNULL((
                SELECT CAST(value_in_use AS INT)
                FROM sys.configurations
                WHERE name = 'Agent XPs'
            ), 0) AS INT),
            is_running = CAST(CASE WHEN EXISTS (
                SELECT 1
                FROM sys.dm_server_services
                WHERE servicename LIKE 'SQL Server Agent%'
                  AND status_desc = 'Running'
            ) THEN 1 ELSE 0 END AS INT)
        """

        @Sendable
        func buildError(_ missing: [String]) -> Error {
            let guidance = [
                "If running in a Linux container: set MSSQL_AGENT_ENABLED=true and restart the container.",
                "If Agent XPs are disabled while Agent runs: EXEC sp_configure 'show advanced options', 1; RECONFIGURE; EXEC sp_configure 'Agent XPs', 1; RECONFIGURE;",
                "Ensure your login has msdb role membership for job operations (at minimum SQLAgentUserRole) or use a sysadmin login.",
                requireProxyPrereqs ? "For proxies: grant ALTER ANY CREDENTIAL at SERVER scope and add msdb SQLAgentOperatorRole, or use a sysadmin login." : nil,
            ].compactMap { $0 }.joined(separator: "\n- ")
            let message = "Agent preflight failed. Missing: \(missing.joined(separator: ", ")).\nFixes:\n- \(guidance)"
            return NSError(domain: "SQLServerAgentOperations.Preflight", code: 1, userInfo: [NSLocalizedDescriptionKey: message])
        }

        func checkAgentStatus() -> EventLoopFuture<(enabled: Bool, running: Bool)> {
            run(agentStatusSQL).map { rows in
                let row = rows.first
                let enabled = (row?.column("is_enabled")?.int ?? 0) == 1
                let running = (row?.column("is_running")?.int ?? 0) == 1
                return (enabled, running)
            }
        }

        return checkAgentStatus().flatMap { status in
            var missing: [String] = []
            if !status.running { missing.append("Agent service not running") }
            if !status.enabled { missing.append("Agent XPs disabled") }
            if !missing.isEmpty {
                let required = missing
                return self.run("SELECT 1").flatMapThrowing { _ in throw buildError(required) }
            }
            
            if !requireProxyPrereqs {
                return self.run("SELECT 1").map { _ in () }
            }
            
            return self.fetchProxyAndCredentialPermissions().flatMapThrowing { perms in
                let hasOperator = perms.msdbRoles.contains { $0.caseInsensitiveCompare("SQLAgentOperatorRole") == .orderedSame }
                let ok = perms.isSysadmin || (perms.hasAlterAnyCredential && hasOperator)
                if !ok {
                    let missing = ["sysadmin or (ALTER ANY CREDENTIAL + msdb SQLAgentOperatorRole)"]
                    throw buildError(missing)
                }
                return ()
            }
        }
    }

    public func addJobServer(jobName: String, serverName: String? = nil) -> EventLoopFuture<Void> {
        return lookupJobId(jobName: jobName).flatMap { jobId in
            let check = "SELECT 1 AS present FROM msdb.dbo.sysjobservers WHERE job_id = N'\(jobId)'"
            return self.run(check).flatMap { (rows: [TDSRow]) -> EventLoopFuture<Void> in
                if rows.first?.column("present")?.int == 1 { return self.run("SELECT 1").map { _ in () } }
                var sql = "EXEC msdb.dbo.sp_add_jobserver @job_name = N'\(Self.escapeLiteral(jobName))'"
                if let serverName, !serverName.isEmpty { sql += ", @server_name = N'\(Self.escapeLiteral(serverName))'" }
                sql += ";"
                return self.run(sql).flatMapError { error -> EventLoopFuture<[TDSRow]> in
                    let msg = String(describing: error)
                    if msg.localizedCaseInsensitiveContains("already has a target server") || msg.localizedCaseInsensitiveContains("already targeted") {
                        return self.run("SELECT 1")
                    }
                    return self.run("SELECT 1").flatMapThrowing { _ in throw error }
                }.map { _ in () }
            }
        }
    }

    public func getJobDetail(jobName: String) -> EventLoopFuture<SQLServerAgentJobDetail?> {
        let sql = "EXEC msdb.dbo.sp_help_job @job_name = N'\(Self.escapeLiteral(jobName))';"
        return run(sql).flatMapErrorThrowing { error in
            let message = String(describing: error)
            if message.localizedCaseInsensitiveContains("does not exist") {
                return []
            }
            throw error
        }.map { rows in
            guard let row = rows.first, let jobId = row.column("job_id")?.string, let name = row.column("name")?.string else { return nil }
            let lastRunDate = self.convertSqlDateTime(date: row.column("last_run_date")?.int, time: row.column("last_run_time")?.int)
            let nextRunDate = self.convertSqlDateTime(date: row.column("next_run_date")?.int, time: row.column("next_run_time")?.int)
            return SQLServerAgentJobDetail(jobId: jobId, name: name, description: row.column("description")?.string ?? row.column("job_description")?.string, enabled: (row.column("enabled")?.int ?? 0) != 0, ownerLoginName: row.column("owner_login_name")?.string ?? row.column("owner")?.string, categoryName: row.column("category_name")?.string ?? row.column("category")?.string, startStepId: row.column("start_step_id")?.int, lastRunOutcome: row.column("last_run_outcome")?.string, lastRunDate: lastRunDate, nextRunDate: nextRunDate, hasSchedule: (row.column("has_schedule")?.int ?? 0) != 0)
        }
    }

    public func getJobSchedules(jobName: String) -> EventLoopFuture<[SQLServerAgentJobScheduleDetail]> {
        let sql = "EXEC msdb.dbo.sp_help_jobschedule @job_name = N'\(Self.escapeLiteral(jobName))';"
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let scheduleId = row.column("schedule_id")?.int, let enabled = row.column("enabled")?.int, let freqType = row.column("freq_type")?.int else { return nil }
                let name = row.column("name")?.string ?? row.column("schedule_name")?.string
                guard let name, !name.isEmpty else { return nil }
                return SQLServerAgentJobScheduleDetail(scheduleId: String(scheduleId), name: name, enabled: enabled != 0, freqType: freqType, freqInterval: row.column("freq_interval")?.int, freqSubdayType: row.column("freq_subday_type")?.int, freqSubdayInterval: row.column("freq_subday_interval")?.int, activeStartDate: row.column("active_start_date")?.int, activeStartTime: row.column("active_start_time")?.int, activeEndDate: row.column("active_end_date")?.int, activeEndTime: row.column("active_end_time")?.int, nextRunDate: self.convertSqlDateTime(date: row.column("next_run_date")?.int, time: row.column("next_run_time")?.int))
            }
        }
    }

    public func configureStep(jobName: String, stepName: String, onSuccessAction: Int? = nil, onSuccessStepId: Int? = nil, onFailAction: Int? = nil, onFailStepId: Int? = nil, retryAttempts: Int? = nil, retryIntervalMinutes: Int? = nil, outputFileName: String? = nil, appendOutputFile: Bool? = nil) -> EventLoopFuture<Void> {
        return lookupStepId(jobName: jobName, stepName: stepName).flatMap { stepId in
            var sql = "EXEC msdb.dbo.sp_update_jobstep @job_name = N'\(Self.escapeLiteral(jobName))', @step_id = \(stepId)"
            if let v = onSuccessAction { sql += ", @on_success_action = \(v)" }
            if let v = onSuccessStepId { sql += ", @on_success_step_id = \(v)" }
            if let v = onFailAction { sql += ", @on_fail_action = \(v)" }
            if let v = onFailStepId { sql += ", @on_fail_step_id = \(v)" }
            if let v = retryAttempts { sql += ", @retry_attempts = \(v)" }
            if let v = retryIntervalMinutes { sql += ", @retry_interval = \(v)" }
            if let v = outputFileName { sql += ", @output_file_name = N'\(Self.escapeLiteral(v))'" }
            if let v = appendOutputFile { sql += ", @append_output_file = \(v ? 1 : 0)" }
            sql += ";"
            return self.run(sql).map { _ in () }
        }
    }

    internal static func escapeLiteral(_ literal: String) -> String {
        return literal.replacingOccurrences(of: "'", with: "''")
    }

    internal static func escapeIdentifier(_ identifier: String) -> String {
        return identifier.replacingOccurrences(of: "]", with: "]]")
    }
}
