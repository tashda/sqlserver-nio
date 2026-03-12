import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Alerts

    internal func createAlert(name: String, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, eventDescriptionKeyword: String? = nil, performanceCondition: String? = nil, wmiNamespace: String? = nil, wmiQuery: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_alert @name = N'\(Self.escapeLiteral(name))', @enabled = \(enabled ? 1 : 0)"
        if let severity { sql += ", @severity = \(severity)" }
        if let messageId { sql += ", @message_id = \(messageId)" }
        if let databaseName { sql += ", @database_name = N'\(Self.escapeLiteral(databaseName))'" }
        if let eventDescriptionKeyword { sql += ", @event_description_keyword = N'\(Self.escapeLiteral(eventDescriptionKeyword))'" }
        if let performanceCondition { sql += ", @performance_condition = N'\(Self.escapeLiteral(performanceCondition))'" }
        if let wmiNamespace { sql += ", @wmi_namespace = N'\(Self.escapeLiteral(wmiNamespace))'" }
        if let wmiQuery { sql += ", @wmi_query = N'\(Self.escapeLiteral(wmiQuery))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func deleteAlert(name: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_alert @name = N'\(Self.escapeLiteral(name))';").map { _ in () }
    }

    internal func listAlerts() -> EventLoopFuture<[SQLServerAgentAlertInfo]> {
        run("SELECT name, severity, message_id, enabled FROM msdb.dbo.sysalerts ORDER BY name;").map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SQLServerAgentAlertInfo(name: name, severity: row.column("severity")?.int, messageId: row.column("message_id")?.int, enabled: (row.column("enabled")?.int ?? 0) != 0)
            }
        }
    }

    // MARK: - Categories

    internal func createCategory(name: String, classId: Int = 1) -> EventLoopFuture<Void> {
        // class_id: 1 = JOB, 2 = ALERT, 3 = OPERATOR
        run("EXEC msdb.dbo.sp_add_category @class = N'JOB', @type = N'LOCAL', @name = N'\(Self.escapeLiteral(name))';").map { _ in () }
    }

    internal func deleteCategory(name: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_category @class = N'JOB', @name = N'\(Self.escapeLiteral(name))';").map { _ in () }
    }

    internal func renameCategory(name: String, newName: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_update_category @class = N'JOB', @name = N'\(Self.escapeLiteral(name))', @new_name = N'\(Self.escapeLiteral(newName))';").map { _ in () }
    }

    internal func listCategories() -> EventLoopFuture<[SQLServerAgentCategoryInfo]> {
        run("SELECT name, [class] AS class_id FROM msdb.dbo.syscategories WHERE [class] IN (1,2,3) ORDER BY name;").map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let classId = row.column("class_id")?.int else { return nil }
                return SQLServerAgentCategoryInfo(name: name, classId: classId)
            }
        }
    }
}
