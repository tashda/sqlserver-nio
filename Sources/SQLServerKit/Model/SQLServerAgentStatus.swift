import Foundation

/// Represents the SQL Server Agent status as observed from server metadata.
///
/// - isSqlAgentEnabled reflects the value of SERVERPROPERTY('IsSqlAgentEnabled') which
///   typically mirrors the Agent XPs configuration. When the Agent service starts, SQL Server
///   enables Agent XPs automatically; when it stops, SQL Server disables them. This is a strong
///   indicator that Agent capabilities are available to clients (sp_start_job, etc.).
/// - isSqlAgentRunning attempts to read from sys.dm_server_services to determine if the Agent
///   service is currently running. On platforms or configurations where that DMV is unavailable
///   or does not surface Agent, this value falls back to 0. Callers should primarily rely on
///   `isSqlAgentEnabled` to decide whether Agent features are usable, and treat
///   `isSqlAgentRunning` as best-effort runtime state.
public struct SQLServerAgentStatus: Sendable {
    public let isSqlAgentEnabled: Bool
    public let isSqlAgentRunning: Bool

    public init(isSqlAgentEnabled: Bool, isSqlAgentRunning: Bool) {
        self.isSqlAgentEnabled = isSqlAgentEnabled
        self.isSqlAgentRunning = isSqlAgentRunning
    }
}
