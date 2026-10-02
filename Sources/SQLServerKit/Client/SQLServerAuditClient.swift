import NIO
import SQLServerTDS

/// Client for SQL Server Audit operations.
///
/// Provides access to server audits, server/database audit specifications,
/// audit actions, and audit log reading.
///
/// Usage:
/// ```swift
/// let audits = try await client.audit.listServerAudits()
/// let specs = try await client.audit.listServerAuditSpecifications()
/// ```
public final class SQLServerAuditClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - Server Audits

    /// Lists all server audits.
    @available(macOS 12.0, *)
    public func listServerAudits() async throws -> [ServerAuditInfo] {
        let sql = """
        SELECT a.audit_id, a.name,
               ISNULL(s.status_desc, 'STOPPED') AS status_desc,
               a.type_desc AS destination,
               f.log_file_path AS file_path,
               f.max_file_size, f.max_rollover_files,
               a.queue_delay, a.on_failure_desc,
               CONVERT(varchar(30), a.create_date, 126) AS create_date
        FROM sys.server_audits AS a
        LEFT JOIN sys.dm_server_audit_status AS s ON s.audit_id = a.audit_id
        LEFT JOIN sys.server_file_audits AS f ON f.audit_id = a.audit_id
        ORDER BY a.name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            let destStr = row.column("destination")?.string ?? "FILE"
            let failStr = row.column("on_failure_desc")?.string ?? "CONTINUE"
            let statusDesc = row.column("status_desc")?.string ?? "STOPPED"
            return ServerAuditInfo(
                auditID: row.column("audit_id")?.int ?? 0,
                name: row.column("name")?.string ?? "",
                isEnabled: statusDesc == "STARTED",
                destination: AuditDestination(rawValue: destStr) ?? .file,
                filePath: row.column("file_path")?.string,
                maxFileSize: row.column("max_file_size")?.int,
                maxRolloverFiles: row.column("max_rollover_files")?.int,
                queueDelay: row.column("queue_delay")?.int,
                onFailure: AuditOnFailure(rawValue: failStr) ?? .continueOperation,
                createDate: row.column("create_date")?.string
            )
        }
    }

    /// Creates a new server audit.
    @available(macOS 12.0, *)
    public func createServerAudit(name: String, destination: AuditDestination, options: ServerAuditOptions? = nil) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        var sql = "CREATE SERVER AUDIT \(escapedName) TO \(destination.rawValue)"
        if destination == .file, let path = options?.filePath {
            let escapedPath = path.replacingOccurrences(of: "'", with: "''")
            sql += " (FILEPATH = N'\(escapedPath)'"
            if let maxSize = options?.maxFileSize { sql += ", MAXSIZE = \(maxSize) MB" }
            if let maxFiles = options?.maxRolloverFiles { sql += ", MAX_ROLLOVER_FILES = \(maxFiles)" }
            if let reserve = options?.reserveDiskSpace { sql += ", RESERVE_DISK_SPACE = \(reserve ? "ON" : "OFF")" }
            sql += ")"
        }
        var withClauses: [String] = []
        if let delay = options?.queueDelay { withClauses.append("QUEUE_DELAY = \(delay)") }
        if let fail = options?.onFailure { withClauses.append("ON_FAILURE = \(fail.rawValue)") }
        if !withClauses.isEmpty {
            sql += " WITH (\(withClauses.joined(separator: ", ")))"
        }
        _ = try await client.execute(sql)
    }

    /// Enables or disables a server audit.
    @available(macOS 12.0, *)
    public func setAuditState(name: String, enabled: Bool) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let state = enabled ? "ON" : "OFF"
        _ = try await client.execute("ALTER SERVER AUDIT \(escapedName) WITH (STATE = \(state))")
    }

    /// Drops a server audit.
    @available(macOS 12.0, *)
    public func dropServerAudit(name: String) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("DROP SERVER AUDIT \(escapedName)")
    }

    // MARK: - Server Audit Specifications

    /// Lists all server audit specifications.
    @available(macOS 12.0, *)
    public func listServerAuditSpecifications() async throws -> [AuditSpecificationInfo] {
        let sql = """
        SELECT sas.name, a.name AS audit_name, sas.is_state_enabled AS is_enabled,
               CONVERT(varchar(30), sas.create_date, 126) AS create_date
        FROM sys.server_audit_specifications AS sas
        INNER JOIN sys.server_audits AS a ON a.audit_guid = sas.audit_guid
        ORDER BY sas.name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            AuditSpecificationInfo(
                name: row.column("name")?.string ?? "",
                auditName: row.column("audit_name")?.string ?? "",
                isEnabled: row.column("is_enabled")?.bool ?? false,
                createDate: row.column("create_date")?.string
            )
        }
    }

    /// Lists the detail actions for a server audit specification.
    @available(macOS 12.0, *)
    public func listServerAuditSpecificationDetails(name: String) async throws -> [AuditSpecificationDetail] {
        let sql = """
        SELECT d.audit_action_name AS action_name, d.class_desc,
               d.audited_result
        FROM sys.server_audit_specification_details AS d
        INNER JOIN sys.server_audit_specifications AS sas ON sas.server_specification_id = d.server_specification_id
        WHERE sas.name = N'\(name.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY d.audit_action_name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            AuditSpecificationDetail(
                actionName: row.column("action_name")?.string ?? "",
                classDesc: row.column("class_desc")?.string ?? ""
            )
        }
    }

    /// Creates a server audit specification.
    @available(macOS 12.0, *)
    public func createServerAuditSpecification(name: String, auditName: String, actions: [String]) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let escapedAudit = SQLServerSQL.escapeIdentifier(auditName)
        var sql = "CREATE SERVER AUDIT SPECIFICATION \(escapedName) FOR SERVER AUDIT \(escapedAudit)"
        for action in actions {
            sql += " ADD (\(action))"
        }
        _ = try await client.execute(sql)
    }

    /// Enables or disables a server audit specification.
    @available(macOS 12.0, *)
    public func setServerAuditSpecificationState(name: String, enabled: Bool) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let state = enabled ? "ON" : "OFF"
        _ = try await client.execute("ALTER SERVER AUDIT SPECIFICATION \(escapedName) WITH (STATE = \(state))")
    }

    /// Drops a server audit specification.
    @available(macOS 12.0, *)
    public func dropServerAuditSpecification(name: String) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("DROP SERVER AUDIT SPECIFICATION \(escapedName)")
    }

    // MARK: - Database Audit Specifications

    /// Lists all database audit specifications in the current database.
    @available(macOS 12.0, *)
    public func listDatabaseAuditSpecifications() async throws -> [AuditSpecificationInfo] {
        let sql = """
        SELECT das.name, a.name AS audit_name, das.is_state_enabled AS is_enabled,
               CONVERT(varchar(30), das.create_date, 126) AS create_date
        FROM sys.database_audit_specifications AS das
        INNER JOIN sys.server_audits AS a ON a.audit_guid = das.audit_guid
        ORDER BY das.name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            AuditSpecificationInfo(
                name: row.column("name")?.string ?? "",
                auditName: row.column("audit_name")?.string ?? "",
                isEnabled: row.column("is_enabled")?.bool ?? false,
                createDate: row.column("create_date")?.string
            )
        }
    }

    /// Lists the detail actions for a database audit specification.
    @available(macOS 12.0, *)
    public func listDatabaseAuditSpecificationDetails(name: String) async throws -> [AuditSpecificationDetail] {
        let sql = """
        SELECT d.audit_action_name AS action_name, d.class_desc,
               SCHEMA_NAME(d.major_id) AS securable_schema_name,
               OBJECT_NAME(d.major_id) AS securable_object_name,
               dp.name AS principal_name
        FROM sys.database_audit_specification_details AS d
        INNER JOIN sys.database_audit_specifications AS das ON das.database_specification_id = d.database_specification_id
        LEFT JOIN sys.database_principals AS dp ON dp.principal_id = d.audited_principal_id
        WHERE das.name = N'\(name.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY d.audit_action_name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            AuditSpecificationDetail(
                actionName: row.column("action_name")?.string ?? "",
                classDesc: row.column("class_desc")?.string ?? "",
                securableSchemaName: row.column("securable_schema_name")?.string,
                securableObjectName: row.column("securable_object_name")?.string,
                principalName: row.column("principal_name")?.string
            )
        }
    }

    /// Creates a database audit specification.
    @available(macOS 12.0, *)
    public func createDatabaseAuditSpecification(name: String, auditName: String, actions: [String]) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let escapedAudit = SQLServerSQL.escapeIdentifier(auditName)
        var sql = "CREATE DATABASE AUDIT SPECIFICATION \(escapedName) FOR SERVER AUDIT \(escapedAudit)"
        for action in actions {
            sql += " ADD (\(action))"
        }
        _ = try await client.execute(sql)
    }

    /// Enables or disables a database audit specification.
    @available(macOS 12.0, *)
    public func setDatabaseAuditSpecificationState(name: String, enabled: Bool) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let state = enabled ? "ON" : "OFF"
        _ = try await client.execute("ALTER DATABASE AUDIT SPECIFICATION \(escapedName) WITH (STATE = \(state))")
    }

    /// Drops a database audit specification.
    @available(macOS 12.0, *)
    public func dropDatabaseAuditSpecification(name: String) async throws {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        _ = try await client.execute("DROP DATABASE AUDIT SPECIFICATION \(escapedName)")
    }

    // MARK: - Audit Actions & Log

    /// Lists all available audit actions from `sys.dm_audit_actions`.
    @available(macOS 12.0, *)
    public func listAuditActions() async throws -> [AuditActionInfo] {
        let sql = """
        SELECT action_id, name, class_desc, covering_action_name
        FROM sys.dm_audit_actions
        ORDER BY class_desc, name
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            AuditActionInfo(
                actionID: row.column("action_id")?.string ?? "",
                name: row.column("name")?.string ?? "",
                classDesc: row.column("class_desc")?.string ?? "",
                coveringActionName: row.column("covering_action_name")?.string
            )
        }
    }

    /// Reads entries from an audit log file using `sys.fn_get_audit_file`.
    @available(macOS 12.0, *)
    public func readAuditLog(filePath: String, limit: Int = 1000) async throws -> [AuditLogEntry] {
        let escapedPath = filePath.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT TOP(\(limit))
            CONVERT(varchar(30), event_time, 126) AS event_time,
            action_id, ISNULL(action_id, '') AS action_name,
            server_principal_name, database_name,
            schema_name, object_name, statement, succeeded
        FROM sys.fn_get_audit_file(N'\(escapedPath)', DEFAULT, DEFAULT)
        ORDER BY event_time DESC
        """
        let rows = try await client.query(sql)
        return rows.map { row in
            AuditLogEntry(
                eventTime: row.column("event_time")?.string,
                actionID: row.column("action_id")?.string ?? "",
                actionName: row.column("action_name")?.string,
                serverPrincipalName: row.column("server_principal_name")?.string,
                databaseName: row.column("database_name")?.string,
                schemaName: row.column("schema_name")?.string,
                objectName: row.column("object_name")?.string,
                statement: row.column("statement")?.string,
                succeeded: row.column("succeeded")?.bool ?? false
            )
        }
    }
}
