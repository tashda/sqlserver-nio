import Foundation

/// The object a synonym points to. `server` and `database` are optional parts of the name.
public struct SQLServerObjectName: Sendable, Hashable {
    public var server: String?
    public var database: String?
    public var schema: String
    public var object: String

    public init(server: String? = nil, database: String? = nil, schema: String = "dbo", object: String) {
        self.server = server
        self.database = database
        self.schema = schema
        self.object = object
    }

    internal var sql: String {
        [server, database, schema, object].compactMap { $0 }.map(SQLServerSQL.escapeIdentifier).joined(separator: ".")
    }
}

/// `CACHE` behaviour of a sequence.
public enum SQLServerSequenceCache: Sendable, Hashable {
    case `default`
    case size(Int)
    case none
}

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {
    // MARK: - Sequences

    /// Creates a sequence in the client's scoped database (or the connection's database).
    public func createSequence(
        name: String,
        schema: String = "dbo",
        type: SQLDataType = .bigint,
        start: Int64? = nil,
        increment: Int64 = 1,
        minValue: Int64? = nil,
        maxValue: Int64? = nil,
        cycle: Bool = false,
        cache: SQLServerSequenceCache = .default
    ) async throws {
        try await executeInScopedDatabase(Self.sequenceSQL(
            name: name, schema: schema, type: type, start: start, increment: increment,
            minValue: minValue, maxValue: maxValue, cycle: cycle, cache: cache
        ))
    }

    public func dropSequence(name: String, schema: String = "dbo") async throws {
        try await executeInScopedDatabase("DROP SEQUENCE \(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))")
    }

    // MARK: - Synonyms

    /// Creates a synonym in the client's scoped database (or the connection's database).
    public func createSynonym(name: String, schema: String = "dbo", target: SQLServerObjectName) async throws {
        try await executeInScopedDatabase(
            "CREATE SYNONYM \(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name)) FOR \(target.sql)"
        )
    }

    public func dropSynonym(name: String, schema: String = "dbo") async throws {
        try await executeInScopedDatabase("DROP SYNONYM \(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name))")
    }

    // MARK: - SQL

    internal static func sequenceSQL(
        name: String, schema: String, type: SQLDataType, start: Int64?, increment: Int64,
        minValue: Int64?, maxValue: Int64?, cycle: Bool, cache: SQLServerSequenceCache
    ) -> String {
        var sql = "CREATE SEQUENCE \(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(name)) AS \(type.toSqlString())"
        if let start { sql += " START WITH \(start)" }
        sql += " INCREMENT BY \(increment)"
        sql += minValue.map { " MINVALUE \($0)" } ?? " NO MINVALUE"
        sql += maxValue.map { " MAXVALUE \($0)" } ?? " NO MAXVALUE"
        sql += cycle ? " CYCLE" : " NO CYCLE"
        switch cache {
        case .default: sql += " CACHE"
        case .size(let size): sql += " CACHE \(size)"
        case .none: sql += " NO CACHE"
        }
        return sql
    }

    /// CREATE SEQUENCE and CREATE SYNONYM take no database name, so switch the connection to the
    /// scoped database for the statement and back afterwards.
    private func executeInScopedDatabase(_ sql: String) async throws {
        try await client.withConnection { connection in
            guard let database = self.database, connection.currentDatabase.caseInsensitiveCompare(database) != .orderedSame else {
                _ = try await connection.execute(sql)
                return
            }
            let original = connection.currentDatabase
            try await connection.changeDatabase(database)
            do {
                _ = try await connection.execute(sql)
            } catch {
                try? await connection.changeDatabase(original)
                throw error
            }
            try await connection.changeDatabase(original)
        }
    }
}
