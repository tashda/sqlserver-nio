import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Schedule Management

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
        var sql = "EXEC msdb.dbo.sp_add_schedule @schedule_name = N'\(Self.escapeLiteral(scheduleName))', @enabled = \(enabled ? 1 : 0), @freq_type = \(freqType), @freq_interval = \(freqInterval)"
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

    internal func attachSchedule(scheduleName: String, toJob jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_attach_schedule @job_name = N'\(Self.escapeLiteral(jobName))', @schedule_name = N'\(Self.escapeLiteral(scheduleName))';"
        return run(sql).map { _ in () }
    }

    internal func detachSchedule(scheduleName: String, fromJob jobName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_detach_schedule @job_name = N'\(Self.escapeLiteral(jobName))', @schedule_name = N'\(Self.escapeLiteral(scheduleName))';"
        return run(sql).map { _ in () }
    }

    internal func deleteSchedule(named scheduleName: String) -> EventLoopFuture<Void> {
        let sql = "EXEC msdb.dbo.sp_delete_schedule @schedule_name = N'\(Self.escapeLiteral(scheduleName))';"
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
        var sql = "EXEC msdb.dbo.sp_update_schedule @schedule_name = N'\(Self.escapeLiteral(name))'"
        if let v = newName { sql += ", @new_name = N'\(Self.escapeLiteral(v))'" }
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

    internal func listSchedules(forJob jobName: String? = nil) -> EventLoopFuture<[SQLServerAgentScheduleInfo]> {
        let sql: String
        if let jobName {
            sql = """
            SELECT sc.name, sc.enabled, sc.freq_type
            FROM msdb.dbo.sysschedules AS sc
            INNER JOIN msdb.dbo.sysjobschedules AS js ON js.schedule_id = sc.schedule_id
            INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
            WHERE j.name = N'\(Self.escapeLiteral(jobName))'
            ORDER BY sc.name;
            """
        } else {
            sql = "SELECT name, enabled, freq_type FROM msdb.dbo.sysschedules ORDER BY name;"
        }
        return run(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let freq = row.column("freq_type")?.int else { return nil }
                return SQLServerAgentScheduleInfo(name: name, enabled: (row.column("enabled")?.int ?? 0) != 0, freqType: freq)
            }
        }
    }
}
