import Foundation

/// Client for temporal table and in-memory OLTP operations.
///
/// Provides listing, enabling, and disabling of system-versioned (temporal)
/// tables, and listing of memory-optimized (In-Memory OLTP) tables.
///
/// Usage:
/// ```swift
/// let temporal = try await client.temporal.listSystemVersionedTables(database: "MyDB")
/// try await client.temporal.disableSystemVersioning(database: "MyDB", schema: "dbo", table: "Orders")
/// let inMem = try await client.temporal.listMemoryOptimizedTables(database: "MyDB")
/// ```
public final class SQLServerTemporalClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    private static func escapeLiteral(_ literal: String) -> String {
        literal.replacingOccurrences(of: "'", with: "''")
    }

    // MARK: - Temporal Tables

    /// Lists all system-versioned tables in a database.
    @available(macOS 12.0, *)
    public func listSystemVersionedTables(database: String) async throws -> [TemporalTableInfo] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            hs.name AS history_schema,
            ht.name AS history_table,
            pc_start.name AS period_start,
            pc_end.name AS period_end
        FROM \(db).sys.tables AS t
        INNER JOIN \(db).sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN \(db).sys.tables AS ht ON ht.object_id = t.history_table_id
        LEFT JOIN \(db).sys.schemas AS hs ON hs.schema_id = ht.schema_id
        LEFT JOIN \(db).sys.periods AS p ON p.object_id = t.object_id
        LEFT JOIN \(db).sys.columns AS pc_start
            ON pc_start.object_id = t.object_id AND pc_start.column_id = p.start_column_id
        LEFT JOIN \(db).sys.columns AS pc_end
            ON pc_end.object_id = t.object_id AND pc_end.column_id = p.end_column_id
        WHERE t.temporal_type = 2
        ORDER BY s.name, t.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard
                let schema = row.column("schema_name")?.string,
                let name = row.column("table_name")?.string,
                let historySchema = row.column("history_schema")?.string,
                let historyTable = row.column("history_table")?.string,
                let periodStart = row.column("period_start")?.string,
                let periodEnd = row.column("period_end")?.string
            else { return nil }
            return TemporalTableInfo(
                schema: schema,
                name: name,
                historySchema: historySchema,
                historyTable: historyTable,
                periodStartColumn: periodStart,
                periodEndColumn: periodEnd
            )
        }
    }

    /// Enables system versioning on a table.
    ///
    /// The table must already have a `PERIOD FOR SYSTEM_TIME` definition
    /// with two `datetime2` generated-always columns.
    @available(macOS 12.0, *)
    public func enableSystemVersioning(
        database: String,
        schema: String,
        table: String,
        historySchema: String? = nil,
        historyTable: String? = nil
    ) async throws {
        let db = Self.escapeIdentifier(database)
        let qualified = "\(db).\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(table))"
        var clause = "SYSTEM_VERSIONING = ON"
        if let historySchema, let historyTable {
            let histQualified = "\(Self.escapeIdentifier(historySchema)).\(Self.escapeIdentifier(historyTable))"
            clause += " (HISTORY_TABLE = \(histQualified))"
        }
        let sql = "ALTER TABLE \(qualified) SET (\(clause))"
        _ = try await client.execute(sql)
    }

    /// Disables system versioning on a table.
    ///
    /// The history table is preserved but unlinked.
    @available(macOS 12.0, *)
    public func disableSystemVersioning(
        database: String,
        schema: String,
        table: String
    ) async throws {
        let db = Self.escapeIdentifier(database)
        let qualified = "\(db).\(Self.escapeIdentifier(schema)).\(Self.escapeIdentifier(table))"
        let sql = "ALTER TABLE \(qualified) SET (SYSTEM_VERSIONING = OFF)"
        _ = try await client.execute(sql)
    }

    // MARK: - In-Memory OLTP

    /// Lists all memory-optimized tables in a database.
    @available(macOS 12.0, *)
    public func listMemoryOptimizedTables(database: String) async throws -> [MemoryOptimizedTableInfo] {
        let db = Self.escapeIdentifier(database)
        let sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            t.durability_desc
        FROM \(db).sys.tables AS t
        INNER JOIN \(db).sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE t.is_memory_optimized = 1
        ORDER BY s.name, t.name
        """
        let rows = try await client.query(sql)
        return rows.compactMap { row in
            guard
                let schema = row.column("schema_name")?.string,
                let name = row.column("table_name")?.string
            else { return nil }
            let durabilityStr = row.column("durability_desc")?.string ?? "SCHEMA_AND_DATA"
            let durability = MemoryOptimizedDurability(rawValue: durabilityStr) ?? .schemaAndData
            return MemoryOptimizedTableInfo(
                schema: schema,
                name: name,
                durability: durability
            )
        }
    }
}
