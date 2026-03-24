import NIO
import SQLServerTDS

extension SQLServerSecurityClient {

    // MARK: - Row-Level Security

    /// Lists all security policies in the current database.
    @available(macOS 12.0, *)
    public func listSecurityPolicies() async throws -> [SecurityPolicyInfo] {
        let sql = """
        SELECT sp.name, s.name AS schema_name,
               sp.is_enabled, sp.is_schema_bound,
               CONVERT(varchar(30), sp.create_date, 126) AS create_date,
               CONVERT(varchar(30), sp.modify_date, 126) AS modify_date
        FROM sys.security_policies AS sp
        INNER JOIN sys.schemas AS s ON s.schema_id = sp.schema_id
        ORDER BY s.name, sp.name
        """
        let rows = try await query(sql)
        return rows.map { row in
            SecurityPolicyInfo(
                name: row.column("name")?.string ?? "",
                schema: row.column("schema_name")?.string ?? "",
                isEnabled: row.column("is_enabled")?.bool ?? false,
                isSchemaBound: row.column("is_schema_bound")?.bool ?? false,
                createDate: row.column("create_date")?.string,
                modifyDate: row.column("modify_date")?.string
            )
        }
    }

    /// Lists security predicates for a given security policy.
    @available(macOS 12.0, *)
    public func listSecurityPredicates(policyName: String, schema: String) async throws -> [SecurityPredicateInfo] {
        let sql = """
        SELECT
            CASE pred.predicate_type WHEN 0 THEN 'FILTER' ELSE 'BLOCK' END AS predicate_type,
            pred.predicate_definition,
            ts.name AS target_schema,
            OBJECT_NAME(pred.target_object_id) AS target_table,
            CASE pred.operation
                WHEN 1 THEN 'AFTER INSERT'
                WHEN 2 THEN 'AFTER UPDATE'
                WHEN 3 THEN 'BEFORE UPDATE'
                WHEN 4 THEN 'BEFORE DELETE'
                ELSE NULL
            END AS operation
        FROM sys.security_predicates AS pred
        INNER JOIN sys.security_policies AS sp ON sp.object_id = pred.object_id
        INNER JOIN sys.schemas AS ps ON ps.schema_id = sp.schema_id
        LEFT JOIN sys.objects AS tobj ON tobj.object_id = pred.target_object_id
        LEFT JOIN sys.schemas AS ts ON ts.schema_id = tobj.schema_id
        WHERE sp.name = N'\(policyName.replacingOccurrences(of: "'", with: "''"))'
          AND ps.name = N'\(schema.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY pred.security_predicate_id
        """
        let rows = try await query(sql)
        return rows.map { row in
            let typeStr = row.column("predicate_type")?.string ?? "FILTER"
            let opStr = row.column("operation")?.string
            return SecurityPredicateInfo(
                predicateType: PredicateType(rawValue: typeStr) ?? .filter,
                predicateDefinition: row.column("predicate_definition")?.string ?? "",
                targetSchema: row.column("target_schema")?.string ?? "",
                targetTable: row.column("target_table")?.string ?? "",
                operation: opStr.flatMap { BlockOperation(rawValue: $0) }
            )
        }
    }

    /// Enables or disables a security policy.
    @available(macOS 12.0, *)
    public func alterSecurityPolicyState(name: String, schema: String, enabled: Bool) async throws {
        let escapedSchema = Self.escapeIdentifier(schema)
        let escapedName = Self.escapeIdentifier(name)
        let state = enabled ? "ON" : "OFF"
        _ = try await exec("ALTER SECURITY POLICY \(escapedSchema).\(escapedName) WITH (STATE = \(state))")
    }

    /// Drops a security policy.
    @available(macOS 12.0, *)
    public func dropSecurityPolicy(name: String, schema: String) async throws {
        let escapedSchema = Self.escapeIdentifier(schema)
        let escapedName = Self.escapeIdentifier(name)
        _ = try await exec("DROP SECURITY POLICY \(escapedSchema).\(escapedName)")
    }
}
