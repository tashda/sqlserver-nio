import NIO

public struct SQLServerAgentJobInfo: Sendable {
    public let name: String
    public let enabled: Bool
    public let lastRunOutcome: String?
}

public final class SQLServerAgentClient {
    private enum Backing {
        case connection(SQLServerConnection)
        case client(SQLServerClient)
    }

    private let backing: Backing

    public convenience init(connection: SQLServerConnection) {
        self.init(backing: .connection(connection))
    }

    public convenience init(client: SQLServerClient) {
        self.init(backing: .client(client))
    }

    private init(backing: Backing) {
        self.backing = backing
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

    public func startJob(named jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_start_job @job_name = N'\(escapeLiteral(jobName))';"
        return run(sql).map { _ in () }
    }

    @available(macOS 12.0, *)
    public func listJobs() async throws -> [SQLServerAgentJobInfo] {
        try await listJobs().get()
    }

    @available(macOS 12.0, *)
    public func startJob(named jobName: String) async throws {
        _ = try await startJob(named: jobName).get()
    }

    private func run(_ sql: String) -> EventLoopFuture<[TDSRow]> {
        switch backing {
        case .connection(let connection):
            return connection.query(sql)
        case .client(let client):
            return client.query(sql)
        }
    }
}

private func escapeLiteral(_ value: String) -> String {
    value.replacingOccurrences(of: "'", with: "''")
}
