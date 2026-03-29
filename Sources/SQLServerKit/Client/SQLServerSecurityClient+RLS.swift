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

    /// Creates a security policy with a single predicate.
    @available(macOS 12.0, *)
    public func createSecurityPolicy(
        name: String,
        schema: String,
        filterFunction: String,
        filterFunctionSchema: String,
        targetTable: String,
        targetSchema: String,
        predicateType: PredicateType = .filter,
        enabled: Bool = true,
        schemaBound: Bool = true
    ) async throws {
        let predicate = SecurityPredicateDefinition(
            predicateType: predicateType,
            functionName: filterFunction,
            functionSchema: filterFunctionSchema,
            targetTable: targetTable,
            targetSchema: targetSchema
        )
        try await createSecurityPolicy(
            name: name,
            schema: schema,
            predicates: [predicate],
            enabled: enabled,
            schemaBound: schemaBound
        )
    }

    /// Creates a security policy with multiple predicates.
    @available(macOS 12.0, *)
    public func createSecurityPolicy(
        name: String,
        schema: String,
        predicates: [SecurityPredicateDefinition],
        enabled: Bool = true,
        schemaBound: Bool = true
    ) async throws {
        guard !predicates.isEmpty else {
            throw SQLServerError.invalidArgument("At least one predicate is required to create a security policy")
        }

        let policyName = "\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(name))"

        var predicateClauses: [String] = []
        for pred in predicates {
            let funcName = "\(Self.escapeIdentifier(pred.functionSchema)).\(Self.escapeIdentifier(pred.functionName))"
            let tableName = "\(Self.escapeIdentifier(pred.targetSchema)).\(Self.escapeIdentifier(pred.targetTable))"
            let predicateArguments = try await predicateArgumentList(
                functionName: pred.functionName,
                functionSchema: pred.functionSchema
            )
            let keyword = pred.predicateType == .filter ? "FILTER" : "BLOCK"
            var clause = "ADD \(keyword) PREDICATE \(funcName)(\(predicateArguments)) ON \(tableName)"
            if pred.predicateType == .block, let op = pred.blockOperation {
                clause += " \(op.rawValue)"
            }
            predicateClauses.append(clause)
        }

        let sql = """
        CREATE SECURITY POLICY \(policyName)
        \(predicateClauses.joined(separator: ",\n"))
        WITH (STATE = \(enabled ? "ON" : "OFF"), SCHEMABINDING = \(schemaBound ? "ON" : "OFF"));
        """
        _ = try await exec(sql)
    }

    /// Adds a predicate to an existing security policy.
    @available(macOS 12.0, *)
    public func addSecurityPredicate(
        policyName: String,
        policySchema: String,
        predicate: SecurityPredicateDefinition
    ) async throws {
        let policy = "\(Self.escapeIdentifier(policySchema)).\(Self.escapeIdentifier(policyName))"
        let funcName = "\(Self.escapeIdentifier(predicate.functionSchema)).\(Self.escapeIdentifier(predicate.functionName))"
        let tableName = "\(Self.escapeIdentifier(predicate.targetSchema)).\(Self.escapeIdentifier(predicate.targetTable))"
        let predicateArguments = try await predicateArgumentList(
            functionName: predicate.functionName,
            functionSchema: predicate.functionSchema
        )
        let keyword = predicate.predicateType == .filter ? "FILTER" : "BLOCK"

        var clause = "ADD \(keyword) PREDICATE \(funcName)(\(predicateArguments)) ON \(tableName)"
        if predicate.predicateType == .block, let op = predicate.blockOperation {
            clause += " \(op.rawValue)"
        }

        let sql = "ALTER SECURITY POLICY \(policy) \(clause);"
        _ = try await exec(sql)
    }

    /// Drops a predicate from an existing security policy.
    @available(macOS 12.0, *)
    public func dropSecurityPredicate(
        policyName: String,
        policySchema: String,
        predicateType: PredicateType,
        targetTable: String,
        targetSchema: String,
        blockOperation: BlockOperation? = nil
    ) async throws {
        let policy = "\(Self.escapeIdentifier(policySchema)).\(Self.escapeIdentifier(policyName))"
        let tableName = "\(Self.escapeIdentifier(targetSchema)).\(Self.escapeIdentifier(targetTable))"
        let keyword = predicateType == .filter ? "FILTER" : "BLOCK"

        var clause = "DROP \(keyword) PREDICATE ON \(tableName)"
        if predicateType == .block, let op = blockOperation {
            clause += " \(op.rawValue)"
        }

        let sql = "ALTER SECURITY POLICY \(policy) \(clause);"
        _ = try await exec(sql)
    }

    @available(macOS 12.0, *)
    private func predicateArgumentList(functionName: String, functionSchema: String) async throws -> String {
        let rows = try await query("""
        SELECT p.name
        FROM sys.parameters AS p
        INNER JOIN sys.objects AS o ON o.object_id = p.object_id
        INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
        WHERE o.name = N'\(functionName.replacingOccurrences(of: "'", with: "''"))'
          AND s.name = N'\(functionSchema.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY p.parameter_id
        """)

        let arguments = rows.compactMap { row -> String? in
            guard let parameterName = row.column("name")?.string else {
                return nil
            }
            let columnName = parameterName.hasPrefix("@") ? String(parameterName.dropFirst()) : parameterName
            return Self.escapeIdentifier(columnName)
        }

        return arguments.joined(separator: ", ")
    }
}
