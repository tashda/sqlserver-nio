import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Operator Management

    internal func createOperator(name: String, emailAddress: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_operator @name = N'\(SQLServerSQL.escapeLiteral(name))', @enabled = \(enabled ? 1 : 0)"
        if let emailAddress, !emailAddress.isEmpty { sql += ", @email_address = N'\(SQLServerSQL.escapeLiteral(emailAddress))'" }
        sql += ";"
        return run(sql).flatMap { _ in
            self.run("SELECT 1 FROM msdb.dbo.sysoperators WHERE name = N'\(SQLServerSQL.escapeLiteral(name))'")
        }.flatMapThrowing { rows in
            guard rows.first != nil else {
                throw SQLServerError.invalidArgument("Operator not visible after creation: \(name)")
            }
            return ()
        }
    }

    internal func updateOperator(name: String, emailAddress: String? = nil, enabled: Bool? = nil, pagerAddress: String? = nil, weekdayPagerStartTime: Int? = nil, weekdayPagerEndTime: Int? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_update_operator @name = N'\(SQLServerSQL.escapeLiteral(name))'"
        if let emailAddress { sql += ", @email_address = N'\(SQLServerSQL.escapeLiteral(emailAddress))'" }
        if let enabled { sql += ", @enabled = \(enabled ? 1 : 0)" }
        if let pagerAddress { sql += ", @pager_address = N'\(SQLServerSQL.escapeLiteral(pagerAddress))'" }
        if let weekdayPagerStartTime { sql += ", @weekday_pager_start_time = \(weekdayPagerStartTime)" }
        if let weekdayPagerEndTime { sql += ", @weekday_pager_end_time = \(weekdayPagerEndTime)" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func deleteOperator(name: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_operator @name = N'\(SQLServerSQL.escapeLiteral(name))';").map { _ in () }
    }

    internal func listOperators() -> EventLoopFuture<[SQLServerAgentOperatorInfo]> {
        run("SELECT name, email_address, enabled FROM msdb.dbo.sysoperators ORDER BY name;").map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SQLServerAgentOperatorInfo(name: name, emailAddress: row.column("email_address")?.string, enabled: (row.column("enabled")?.int ?? 0) != 0)
            }
        }
    }

    // MARK: - Notifications

    internal func addNotification(alertName: String, operatorName: String, method: Int = 1) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_add_notification @alert_name = N'\(SQLServerSQL.escapeLiteral(alertName))', @operator_name = N'\(SQLServerSQL.escapeLiteral(operatorName))', @notification_method = \(method);").map { _ in () }
    }

    internal func deleteNotification(alertName: String, operatorName: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_notification @alert_name = N'\(SQLServerSQL.escapeLiteral(alertName))', @operator_name = N'\(SQLServerSQL.escapeLiteral(operatorName))';").map { _ in () }
    }
}
