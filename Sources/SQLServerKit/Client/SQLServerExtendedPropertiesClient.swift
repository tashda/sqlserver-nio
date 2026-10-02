import Foundation

// MARK: - Extended Property Types

/// A name-value pair of extended property metadata attached to a SQL Server object.
public struct SQLServerExtendedProperty: Sendable, Hashable {
    public let name: String
    public let value: String

    public init(name: String, value: String) {
        self.name = name
        self.value = value
    }
}

/// Identifies the level-type hierarchy for an extended property target.
public struct ExtendedPropertyTarget: Sendable {
    public let schema: String
    public let level1Type: String
    public let level1Name: String
    public let level2Type: String?
    public let level2Name: String?

    /// Target a table-level object (or view).
    public static func table(schema: String, name: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "TABLE", level1Name: name, level2Type: nil, level2Name: nil)
    }

    /// Target a column on a table.
    public static func column(schema: String, table: String, column: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "TABLE", level1Name: table, level2Type: "COLUMN", level2Name: column)
    }

    /// Target a view-level object.
    public static func view(schema: String, name: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "VIEW", level1Name: name, level2Type: nil, level2Name: nil)
    }

    /// Target an index on a table.
    public static func index(schema: String, table: String, index: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "TABLE", level1Name: table, level2Type: "INDEX", level2Name: index)
    }

    /// Target a constraint on a table.
    public static func constraint(schema: String, table: String, constraint: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "TABLE", level1Name: table, level2Type: "CONSTRAINT", level2Name: constraint)
    }

    /// Target a stored procedure.
    public static func procedure(schema: String, name: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "PROCEDURE", level1Name: name, level2Type: nil, level2Name: nil)
    }

    /// Target a parameter on a stored procedure.
    public static func parameter(schema: String, procedure: String, parameter: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "PROCEDURE", level1Name: procedure, level2Type: "PARAMETER", level2Name: parameter)
    }

    /// Target a function.
    public static func function(schema: String, name: String) -> ExtendedPropertyTarget {
        ExtendedPropertyTarget(schema: schema, level1Type: "FUNCTION", level1Name: name, level2Type: nil, level2Name: nil)
    }

    /// Custom target with explicit level types.
    public init(schema: String, level1Type: String, level1Name: String, level2Type: String?, level2Name: String?) {
        self.schema = schema
        self.level1Type = level1Type
        self.level1Name = level1Name
        self.level2Type = level2Type
        self.level2Name = level2Name
    }
}

// MARK: - Client

/// Provides CRUD operations for SQL Server extended properties.
public final class SQLServerExtendedPropertiesClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List

    /// Lists all extended properties for the given target object.
    @available(macOS 12.0, *)
    public func list(target: ExtendedPropertyTarget) async throws -> [SQLServerExtendedProperty] {
        let level2TypeArg = target.level2Type.map { "'\(SQLServerSQL.escapeLiteral($0))'" } ?? "NULL"
        let level2NameArg = target.level2Name.map { "'\(SQLServerSQL.escapeLiteral($0))'" } ?? "NULL"

        let sql = """
        SELECT objname, name, CAST(value AS NVARCHAR(MAX)) AS value
        FROM fn_listextendedproperty(
            NULL,
            'SCHEMA', '\(SQLServerSQL.escapeLiteral(target.schema))',
            '\(SQLServerSQL.escapeLiteral(target.level1Type))', '\(SQLServerSQL.escapeLiteral(target.level1Name))',
            \(level2TypeArg), \(level2NameArg)
        )
        ORDER BY name
        """

        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let name = row.column("name")?.string else { return nil }
            let value = row.column("value")?.string ?? ""
            return SQLServerExtendedProperty(name: name, value: value)
        }
    }

    /// Lists extended properties for all columns of a table.
    @available(macOS 12.0, *)
    public func listForAllColumns(schema: String, table: String) async throws -> [String: [SQLServerExtendedProperty]] {
        let sql = """
        SELECT objname, name, CAST(value AS NVARCHAR(MAX)) AS value
        FROM fn_listextendedproperty(
            NULL,
            'SCHEMA', '\(SQLServerSQL.escapeLiteral(schema))',
            'TABLE', '\(SQLServerSQL.escapeLiteral(table))',
            'COLUMN', NULL
        )
        ORDER BY objname, name
        """

        let rows = try await client.query(sql)
        var result: [String: [SQLServerExtendedProperty]] = [:]
        for row in rows {
            guard let columnName = row.column("objname")?.string,
                  let name = row.column("name")?.string else { continue }
            let value = row.column("value")?.string ?? ""
            result[columnName, default: []].append(SQLServerExtendedProperty(name: name, value: value))
        }
        return result
    }

    // MARK: - Add

    /// Adds a new extended property to the given target.
    @available(macOS 12.0, *)
    public func add(name: String, value: String, target: ExtendedPropertyTarget) async throws {
        var sql = """
        EXEC sp_addextendedproperty
            @name = N'\(SQLServerSQL.escapeLiteral(name))',
            @value = N'\(SQLServerSQL.escapeLiteral(value))',
            @level0type = N'SCHEMA', @level0name = N'\(SQLServerSQL.escapeLiteral(target.schema))',
            @level1type = N'\(SQLServerSQL.escapeLiteral(target.level1Type))', @level1name = N'\(SQLServerSQL.escapeLiteral(target.level1Name))'
        """

        if let l2Type = target.level2Type, let l2Name = target.level2Name {
            sql += ",\n    @level2type = N'\(SQLServerSQL.escapeLiteral(l2Type))', @level2name = N'\(SQLServerSQL.escapeLiteral(l2Name))'"
        }

        _ = try await client.execute(sql)
    }

    // MARK: - Update

    /// Updates an existing extended property on the given target.
    @available(macOS 12.0, *)
    public func update(name: String, value: String, target: ExtendedPropertyTarget) async throws {
        var sql = """
        EXEC sp_updateextendedproperty
            @name = N'\(SQLServerSQL.escapeLiteral(name))',
            @value = N'\(SQLServerSQL.escapeLiteral(value))',
            @level0type = N'SCHEMA', @level0name = N'\(SQLServerSQL.escapeLiteral(target.schema))',
            @level1type = N'\(SQLServerSQL.escapeLiteral(target.level1Type))', @level1name = N'\(SQLServerSQL.escapeLiteral(target.level1Name))'
        """

        if let l2Type = target.level2Type, let l2Name = target.level2Name {
            sql += ",\n    @level2type = N'\(SQLServerSQL.escapeLiteral(l2Type))', @level2name = N'\(SQLServerSQL.escapeLiteral(l2Name))'"
        }

        _ = try await client.execute(sql)
    }

    // MARK: - Drop

    /// Removes an extended property from the given target.
    @available(macOS 12.0, *)
    public func drop(name: String, target: ExtendedPropertyTarget) async throws {
        var sql = """
        EXEC sp_dropextendedproperty
            @name = N'\(SQLServerSQL.escapeLiteral(name))',
            @level0type = N'SCHEMA', @level0name = N'\(SQLServerSQL.escapeLiteral(target.schema))',
            @level1type = N'\(SQLServerSQL.escapeLiteral(target.level1Type))', @level1name = N'\(SQLServerSQL.escapeLiteral(target.level1Name))'
        """

        if let l2Type = target.level2Type, let l2Name = target.level2Name {
            sql += ",\n    @level2type = N'\(SQLServerSQL.escapeLiteral(l2Type))', @level2name = N'\(SQLServerSQL.escapeLiteral(l2Name))'"
        }

        _ = try await client.execute(sql)
    }

    // MARK: - Upsert

    /// Adds or updates an extended property. Tries update first, falls back to add if not found.
    @available(macOS 12.0, *)
    public func upsert(name: String, value: String, target: ExtendedPropertyTarget) async throws {
        let existing = try await list(target: target)
        if existing.contains(where: { $0.name == name }) {
            try await update(name: name, value: value, target: target)
        } else {
            try await add(name: name, value: value, target: target)
        }
    }

    // MARK: - User-Level Extended Properties

    /// Lists extended properties for a database user (class = 4, database principal).
    @available(macOS 12.0, *)
    public func listForUser(name: String) async throws -> [SQLServerExtendedProperty] {
        let escaped = SQLServerSQL.escapeLiteral(name)
        let sql = """
        SELECT ep.name, CAST(ep.value AS NVARCHAR(MAX)) AS value
        FROM sys.extended_properties ep
        WHERE ep.class = 4
        AND ep.major_id = USER_ID(N'\(escaped)')
        ORDER BY ep.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard let propName = row.column("name")?.string else { return nil }
            let value = row.column("value")?.string ?? ""
            return SQLServerExtendedProperty(name: propName, value: value)
        }
    }

    /// Adds an extended property to a database user.
    @available(macOS 12.0, *)
    public func addForUser(userName: String, name: String, value: String) async throws {
        let sql = """
        EXEC sp_addextendedproperty
            @name = N'\(SQLServerSQL.escapeLiteral(name))',
            @value = N'\(SQLServerSQL.escapeLiteral(value))',
            @level0type = N'USER', @level0name = N'\(SQLServerSQL.escapeLiteral(userName))'
        """
        _ = try await client.execute(sql)
    }

    /// Updates an extended property on a database user.
    @available(macOS 12.0, *)
    public func updateForUser(userName: String, name: String, value: String) async throws {
        let sql = """
        EXEC sp_updateextendedproperty
            @name = N'\(SQLServerSQL.escapeLiteral(name))',
            @value = N'\(SQLServerSQL.escapeLiteral(value))',
            @level0type = N'USER', @level0name = N'\(SQLServerSQL.escapeLiteral(userName))'
        """
        _ = try await client.execute(sql)
    }

    /// Removes an extended property from a database user.
    @available(macOS 12.0, *)
    public func dropForUser(userName: String, name: String) async throws {
        let sql = """
        EXEC sp_dropextendedproperty
            @name = N'\(SQLServerSQL.escapeLiteral(name))',
            @level0type = N'USER', @level0name = N'\(SQLServerSQL.escapeLiteral(userName))'
        """
        _ = try await client.execute(sql)
    }

    /// Adds or updates an extended property on a database user.
    @available(macOS 12.0, *)
    public func upsertForUser(userName: String, name: String, value: String) async throws {
        let existing = try await listForUser(name: userName)
        if existing.contains(where: { $0.name == name }) {
            try await updateForUser(userName: userName, name: name, value: value)
        } else {
            try await addForUser(userName: userName, name: name, value: value)
        }
    }

    // MARK: - Helpers

}
