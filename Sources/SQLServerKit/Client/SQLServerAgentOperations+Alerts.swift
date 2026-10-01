import Foundation
import NIO
import SQLServerTDS

extension SQLServerAgentOperations {
    // MARK: - Alerts

    internal func createAlert(name: String, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, eventDescriptionKeyword: String? = nil, performanceCondition: String? = nil, wmiNamespace: String? = nil, wmiQuery: String? = nil, enabled: Bool = true) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_add_alert @name = N'\(SQLServerSQL.escapeLiteral(name))', @enabled = \(enabled ? 1 : 0)"
        if let severity { sql += ", @severity = \(severity)" }
        if let messageId { sql += ", @message_id = \(messageId)" }
        if let databaseName { sql += ", @database_name = N'\(SQLServerSQL.escapeLiteral(databaseName))'" }
        if let eventDescriptionKeyword { sql += ", @event_description_keyword = N'\(SQLServerSQL.escapeLiteral(eventDescriptionKeyword))'" }
        if let performanceCondition { sql += ", @performance_condition = N'\(SQLServerSQL.escapeLiteral(performanceCondition))'" }
        if let wmiNamespace { sql += ", @wmi_namespace = N'\(SQLServerSQL.escapeLiteral(wmiNamespace))'" }
        if let wmiQuery { sql += ", @wmi_query = N'\(SQLServerSQL.escapeLiteral(wmiQuery))'" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func deleteAlert(name: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_alert @name = N'\(SQLServerSQL.escapeLiteral(name))';").map { _ in () }
    }

    internal func updateAlert(name: String, newName: String? = nil, severity: Int? = nil, messageId: Int? = nil, databaseName: String? = nil, eventDescriptionKeyword: String? = nil, enabled: Bool? = nil) -> EventLoopFuture<Void> {
        var sql = "EXEC msdb.dbo.sp_update_alert @name = N'\(SQLServerSQL.escapeLiteral(name))'"
        if let newName { sql += ", @new_name = N'\(SQLServerSQL.escapeLiteral(newName))'" }
        if let severity { sql += ", @severity = \(severity)" }
        if let messageId { sql += ", @message_id = \(messageId)" }
        if let databaseName { sql += ", @database_name = N'\(SQLServerSQL.escapeLiteral(databaseName))'" }
        if let eventDescriptionKeyword { sql += ", @event_description_keyword = N'\(SQLServerSQL.escapeLiteral(eventDescriptionKeyword))'" }
        if let enabled { sql += ", @enabled = \(enabled ? 1 : 0)" }
        sql += ";"
        return run(sql).map { _ in () }
    }

    internal func enableAlert(name: String, enabled: Bool) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_update_alert @name = N'\(SQLServerSQL.escapeLiteral(name))', @enabled = \(enabled ? 1 : 0);").map { _ in () }
    }

    @available(macOS 12.0, *)
    public func enableAlert(name: String, enabled: Bool) async throws {
        try await enableAlert(name: name, enabled: enabled).get()
    }

    internal func listAlerts() -> EventLoopFuture<[SQLServerAgentAlertInfo]> {
        run("SELECT name, severity, message_id, database_name, event_description_keyword, enabled FROM msdb.dbo.sysalerts ORDER BY name;").map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SQLServerAgentAlertInfo(
                    name: name,
                    severity: row.column("severity")?.int,
                    messageId: row.column("message_id")?.int,
                    databaseName: row.column("database_name")?.string,
                    eventDescriptionKeyword: row.column("event_description_keyword")?.string,
                    enabled: (row.column("enabled")?.int ?? 0) != 0
                )
            }
        }
    }

    // MARK: - Categories

    /// The `@class` argument of msdb's category procedures for a `syscategories.category_class`:
    /// 1 = JOB, 2 = ALERT, 3 = OPERATOR.
    static func categoryClassName(_ classId: Int) -> String? {
        switch classId {
        case 1: return "JOB"
        case 2: return "ALERT"
        case 3: return "OPERATOR"
        default: return nil
        }
    }

    static func categoryClass(_ classId: Int) throws -> String {
        guard let className = categoryClassName(classId) else {
            throw SQLServerError.invalidArgument("Category class \(classId) is not 1 (job), 2 (alert) or 3 (operator)")
        }
        return className
    }

    internal func createCategory(name: String, className: String) -> EventLoopFuture<Void> {
        // Job categories are local (or multi-server); alert and operator categories have no type.
        let type = className == "JOB" ? "LOCAL" : "NONE"
        return run("EXEC msdb.dbo.sp_add_category @class = N'\(className)', @type = N'\(type)', @name = N'\(SQLServerSQL.escapeLiteral(name))';").map { _ in () }
    }

    internal func deleteCategory(name: String, className: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_delete_category @class = N'\(className)', @name = N'\(SQLServerSQL.escapeLiteral(name))';").map { _ in () }
    }

    internal func renameCategory(name: String, newName: String, className: String) -> EventLoopFuture<Void> {
        run("EXEC msdb.dbo.sp_update_category @class = N'\(className)', @name = N'\(SQLServerSQL.escapeLiteral(name))', @new_name = N'\(SQLServerSQL.escapeLiteral(newName))';").map { _ in () }
    }

    internal func listCategories() -> EventLoopFuture<[SQLServerAgentCategoryInfo]> {
        run("SELECT name, category_class AS class_id FROM msdb.dbo.syscategories WHERE category_class IN (1,2,3) ORDER BY category_class, name;").map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let classId = row.column("class_id")?.int else { return nil }
                return SQLServerAgentCategoryInfo(name: name, classId: classId)
            }
        }
    }
}
