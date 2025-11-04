import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

// Lightweight metadata structs
public struct DatabaseMetadata: Sendable {
    public let name: String
}

public struct SchemaMetadata: Sendable {
    public let name: String
}

public struct TableMetadata: Sendable {
    public let schema: String
    public let name: String
    /// SQL Server object type e.g. USER_TABLE, VIEW, TABLE_TYPE.
    public let type: String
    /// True when SQL Server marks the object as system-shipped.
    public let isSystemObject: Bool
    /// Optional MS_Description extended property for this table/view/type
    public let comment: String?
}

public struct ColumnMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let typeName: String
    public let systemTypeName: String?
    public let maxLength: Int?
    public let precision: Int?
    public let scale: Int?
    public let collationName: String?
    public let isNullable: Bool
    public let isIdentity: Bool
    public let isComputed: Bool
    public let hasDefaultValue: Bool
    public let defaultDefinition: String?
    public let computedDefinition: String?
    public let ordinalPosition: Int
    /// Identity seed value (if identity column)
    public let identitySeed: Int?
    /// Identity increment value (if identity column)
    public let identityIncrement: Int?
    /// Check constraint definition (if check constraint exists)
    public let checkDefinition: String?
    /// Optional MS_Description extended property for this column
    public let comment: String?
}

public struct ParameterMetadata: Sendable {
    public let schema: String
    public let object: String
    public let name: String
    public let ordinal: Int
    public let isReturnValue: Bool
    public let typeName: String
    public let systemTypeName: String?
    public let maxLength: Int?
    public let precision: Int?
    public let scale: Int?
    public let isOutput: Bool
    public let hasDefaultValue: Bool
    public let defaultValue: String?
    public let isReadOnly: Bool
}

public struct KeyColumnMetadata: Sendable {
    public let column: String
    public let ordinal: Int
    public let isDescending: Bool
}

public struct KeyConstraintMetadata: Sendable {
    public enum ConstraintType: String, Sendable {
        case primaryKey = "PRIMARY_KEY"
        case unique = "UNIQUE"
    }

    public let schema: String
    public let table: String
    public let name: String
    public let type: ConstraintType
    public let isClustered: Bool
    public let columns: [KeyColumnMetadata]
}

public struct IndexColumnMetadata: Sendable {
    public let column: String
    public let ordinal: Int
    public let isDescending: Bool
    public let isIncluded: Bool
}

public struct IndexMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let isUnique: Bool
    public let isClustered: Bool
    public let isPrimaryKey: Bool
    public let isUniqueConstraint: Bool
    public let filterDefinition: String?
    public let columns: [IndexColumnMetadata]
}

public struct ForeignKeyColumnMetadata: Sendable {
    public let parentColumn: String
    public let referencedColumn: String
    public let ordinal: Int
}

public struct ForeignKeyMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let referencedSchema: String
    public let referencedTable: String
    public let deleteAction: String
    public let updateAction: String
    public let columns: [ForeignKeyColumnMetadata]
}

extension ForeignKeyMetadata {
    fileprivate static func mapAction(_ code: Int) -> String {
        switch code {
        case 0: return "CASCADE"
        case 1: return "NO ACTION"
        case 2: return "SET NULL"
        case 3: return "SET DEFAULT"
        default: return "NO ACTION"
        }
    }
}



public struct DependencyMetadata: Sendable {
    public let referencingSchema: String
    public let referencingObject: String
    public let referencingType: String
    public let isSchemaBound: Bool
}

public struct RoutineMetadata: Sendable {
    public enum RoutineType: String, Sendable {
        case procedure = "PROCEDURE"
        case scalarFunction = "SCALAR_FUNCTION"
        case tableFunction = "TABLE_FUNCTION"
    }

    public let schema: String
    public let name: String
    public let type: RoutineType
    public let definition: String?
    public let isSystemObject: Bool
    /// Optional MS_Description extended property for this routine (procedure/function)
    public let comment: String?
}

public struct ObjectDefinition: Sendable {
    public enum ObjectType: String, Sendable {
        case view
        case table
        case procedure
        case scalarFunction
        case tableFunction
        case trigger
        case other

        public static func from(typeDesc: String, objectType: String? = nil) -> ObjectType {
            let normalized = typeDesc.uppercased()
            if normalized.contains("TABLE") && !normalized.contains("FUNCTION") {
                return .table
            }
            if normalized.contains("VIEW") {
                return .view
            }
            if normalized.contains("TRIGGER") {
                return .trigger
            }
            if normalized.contains("FUNCTION") {
                return normalized.contains("TABLE") ? .tableFunction : .scalarFunction
            }
            if normalized.contains("PROCEDURE") {
                return .procedure
            }
            return .other
        }
    }

    public let schema: String
    public let name: String
    public let type: ObjectType
    public let definition: String?
    public let isSystemObject: Bool
    public let createDate: Date?
    public let modifyDate: Date?
}

public struct SQLServerMetadataObjectIdentifier: Sendable {
    public enum Kind: String, Sendable {
        case view
        case procedure
        case function
        case trigger
        case table
        case other
    }

    public let database: String?
    public let schema: String
    public let name: String
    public let kind: Kind

    public init(database: String? = nil, schema: String, name: String, kind: Kind) {
        self.database = database
        self.schema = schema
        self.name = name
        self.kind = kind
    }
}

public struct MetadataSearchScope: OptionSet, Sendable {
    public let rawValue: Int

    public init(rawValue: Int) {
        self.rawValue = rawValue
    }

    public static let objectNames = MetadataSearchScope(rawValue: 1 << 0)
    public static let definitions = MetadataSearchScope(rawValue: 1 << 1)
    public static let columns = MetadataSearchScope(rawValue: 1 << 2)
    public static let indexes = MetadataSearchScope(rawValue: 1 << 3)
    public static let constraints = MetadataSearchScope(rawValue: 1 << 4)

    public static let `default`: MetadataSearchScope = [.objectNames, .definitions]
    public static let all: MetadataSearchScope = [.objectNames, .definitions, .columns, .indexes, .constraints]
}

public struct MetadataSearchHit: Sendable {
    public enum MatchKind: String, Sendable {
        case name
        case definition
        case column
        case index
        case constraint
    }

    public let schema: String
    public let name: String
    public let type: ObjectDefinition.ObjectType
    public let matchKind: MatchKind
    public let detail: String?
}

public struct TriggerMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let isInsteadOf: Bool
    public let isDisabled: Bool
    public let definition: String?
    /// Optional MS_Description extended property for this trigger
    public let comment: String?
}

/// Simple result cache keyed by string identifiers.
final class MetadataCache<Value: Sendable> {
    private var storage: [String: Value] = [:]
    private let lock = NIOLock()

    func value(forKey key: String) -> Value? {
        lock.withLock {
            storage[key]
        }
    }

    func setValue(_ value: Value, forKey key: String) {
        lock.withLock {
            storage[key] = value
        }
    }
}

public final class SQLServerMetadataClient {
    // MARK: - SQL Server Agent

    /// Represents the SQL Server Agent status as observed from server metadata.
    ///
    /// - isSqlAgentEnabled reflects the value of SERVERPROPERTY('IsSqlAgentEnabled') which
    ///   typically mirrors the Agent XPs configuration. When the Agent service starts, SQL Server
    ///   enables Agent XPs automatically; when it stops, SQL Server disables them. This is a strong
    ///   indicator that Agent capabilities are available to clients (sp_start_job, etc.).
    /// - isSqlAgentRunning attempts to read from sys.dm_server_services to determine if the Agent
    ///   service is currently running. On platforms or configurations where that DMV is unavailable
    ///   or does not surface Agent, this value falls back to 0. Callers should primarily rely on
    ///   `isSqlAgentEnabled` to decide whether Agent features are usable, and treat
    ///   `isSqlAgentRunning` as best-effort runtime state.
    public struct SQLServerAgentStatus: Sendable {
        public let isSqlAgentEnabled: Bool
        public let isSqlAgentRunning: Bool
    }

    /// Fetches the SQL Server Agent status using lightweight metadata queries.
    ///
    /// The query mirrors the integration tests by combining SERVERPROPERTY('IsSqlAgentEnabled')
    /// with sys.dm_server_services when available. This works for both Windows and Linux editions
    /// where the Agent service is supported by the running SKU.
    public func fetchAgentStatus() -> EventLoopFuture<SQLServerAgentStatus> {
        let sql = """
        SELECT
            is_enabled = CAST(ISNULL(SERVERPROPERTY('IsSqlAgentEnabled'), 0) AS INT),
            is_running = COALESCE((
                SELECT TOP (1)
                    CASE WHEN status_desc = 'Running' THEN 1 ELSE 0 END
                FROM sys.dm_server_services
                WHERE servicename LIKE 'SQL Server Agent%'
            ), 0)
        """
        return queryExecutor(sql).map { rows in
            let enabled = (rows.first?.column("is_enabled")?.int ?? 0) != 0
            let running = (rows.first?.column("is_running")?.int ?? 0) != 0
            return SQLServerAgentStatus(isSqlAgentEnabled: enabled, isSqlAgentRunning: running)
        }
    }

    @available(macOS 12.0, *)
    public func fetchAgentStatus() async throws -> SQLServerAgentStatus {
        try await fetchAgentStatus().get()
    }

    public struct Configuration: Sendable {
        public var includeSystemSchemas: Bool
        public var enableColumnCache: Bool
        public var includeRoutineDefinitions: Bool
        public var includeTriggerDefinitions: Bool
        /// Prefer using sp_columns_100 for table column metadata. When false, uses catalog queries for tables too.
        public var preferStoredProcedureColumns: Bool

        public init(
            includeSystemSchemas: Bool = false,
            enableColumnCache: Bool = true,
            includeRoutineDefinitions: Bool = false,
            includeTriggerDefinitions: Bool = true,
            preferStoredProcedureColumns: Bool = true
        ) {
            self.includeSystemSchemas = includeSystemSchemas
            self.enableColumnCache = enableColumnCache
            self.includeRoutineDefinitions = includeRoutineDefinitions
            self.includeTriggerDefinitions = includeTriggerDefinitions
            self.preferStoredProcedureColumns = preferStoredProcedureColumns
        }
    }

    private let connection: SQLServerConnection
    private let logger: Logger
    private let queryExecutor: @Sendable (String) -> EventLoopFuture<[TDSRow]>
    private let cache: MetadataCache<[ColumnMetadata]>?
    private let configuration: Configuration
    private let defaultDatabaseLock = NIOLock()
    private var defaultDatabase: String?

    public convenience init(
        connection: SQLServerConnection,
        configuration: Configuration = Configuration()
    ) {
        let eventLoop = connection.eventLoop
        let executor: @Sendable (String) -> EventLoopFuture<[TDSRow]> = { [weak connection] sql in
            guard let connection else {
                return eventLoop.makeFailedFuture(SQLServerError.connectionClosed)
            }
            return connection.execute(sql).map(\.rows)
        }
        self.init(
            connection: connection,
            configuration: configuration,
            sharedCache: nil,
            defaultDatabase: connection.currentDatabase,
            logger: connection.logger,
            queryExecutor: executor
        )
    }



    internal init(
        connection: SQLServerConnection,
        configuration: Configuration,
        sharedCache: MetadataCache<[ColumnMetadata]>?,
        defaultDatabase: String?,
        logger: Logger? = nil,
        queryExecutor: (@Sendable (String) -> EventLoopFuture<[TDSRow]>)? = nil
    ) {
        self.connection = connection
        if let providedLogger = logger {
            self.logger = providedLogger
        } else {
            var defaultLogger = Logger(label: "tds.sqlserver.metadata")
            defaultLogger.logLevel = .trace
            self.logger = defaultLogger
        }
        self.configuration = configuration
        if self.configuration.enableColumnCache {
            self.cache = sharedCache ?? MetadataCache<[ColumnMetadata]>()
        } else {
            self.cache = nil
        }
        self.defaultDatabase = defaultDatabase
        if let executor = queryExecutor {
            self.queryExecutor = executor
        } else {
            self.queryExecutor = { sql in
                connection.query(sql)
            }
        }
    }

    internal func updateDefaultDatabase(_ database: String) {
        defaultDatabaseLock.withLock { self.defaultDatabase = database }
    }

    private func effectiveDatabase(_ database: String?) -> String? {
        if let database {
            return database
        }
        return defaultDatabaseLock.withLock { defaultDatabase }
    }

    // Builds a CREATE TABLE script (plus CREATE INDEX statements) for the given table
    // using existing metadata APIs for performance and consistency.
    private func scriptTableDefinition(
        database: String?,
        schema: String,
        table: String
    ) -> EventLoopFuture<String?> {
        let colsFuture = self.fetchDetailedColumns(database: database, schema: schema, table: table)
        let pkFuture = listPrimaryKeys(database: database, schema: schema, table: table)
        let uqFuture = listUniqueConstraints(database: database, schema: schema, table: table)
        let fkFuture = listForeignKeys(database: database, schema: schema, table: table)
        let ixFuture = self.fetchObjectIndexDetails(database: database, schema: schema, object: table)
        let ckFuture = self.listCheckConstraints(database: database, schema: schema, table: table)
        let fgFuture = self.fetchTableFilegroup(database: database, schema: schema, table: table)
        let lobFuture = self.fetchLobAndFilestreamStorage(database: database, schema: schema, table: table)
        let temporalFuture = self.fetchTemporalAndMemoryOptions(database: database, schema: schema, table: table)

        return colsFuture.and(pkFuture).and(uqFuture).and(fkFuture).and(ixFuture).and(ckFuture).and(fgFuture).and(lobFuture).and(temporalFuture).map { nested in
            // Unpack nested tuple from chained `and` calls
            let temporal = nested.1
            let lob = nested.0.1
            let fg = nested.0.0.1
            let e = nested.0.0.0
            let checks = e.1
            let d = e.0
            let ixs = d.1
            let c = d.0
            let fks = c.1
            let b = c.0
            let uqs = b.1
            let a = b.0
            let columns = a.0
            let pks = a.1
            // Basic identifier helpers
            func ident(_ name: String) -> String { "[\(SQLServerMetadataClient.escapeIdentifier(name))]" }
            func qualified(_ s: String, _ n: String) -> String { "\(ident(s)).\(ident(n))" }

            // Format data type string from column metadata
            func formatType(_ c: DetailedColumn) -> String {
                let t = c.typeName.uppercased()
                switch t {
                case "DECIMAL", "NUMERIC":
                    let p = c.precision ?? 18
                    let s = c.scale ?? 0
                    return "\(t)(\(p), \(s))"
                case "DATETIME2", "DATETIMEOFFSET", "TIME":
                    let p = c.scale ?? 7
                    return "\(t)(\(p))"
                case "VARCHAR", "CHAR":
                    if let len = c.maxLength, len > 0, len <= 8000 { return "\(t)(\(len))" }
                    return "\(t)(MAX)"
                case "NVARCHAR", "NCHAR":
                    if let len = c.maxLength, len > 0 {
                        let chars = max(1, len / 2)
                        if chars <= 4000 { return "\(t)(\(chars))" }
                    }
                    return "\(t)(MAX)"
                case "VARBINARY", "BINARY":
                    if let len = c.maxLength, len > 0, len <= 8000 { return "\(t)(\(len))" }
                    return "\(t)(MAX)"
                default:
                    return t
                }
            }

            let sortedColumns = columns.sorted { $0.columnId < $1.columnId }
            var lines: [String] = []
            lines.reserveCapacity(sortedColumns.count + pks.count + uqs.count + fks.count)

            for col in sortedColumns {
                // Computed columns
                if let expr = col.computedDefinition, !expr.isEmpty {
                    var comp = "    \(ident(col.name)) AS (\(expr))"
                    if col.isComputedPersisted { comp += " PERSISTED" }
                    lines.append(comp)
                    continue
                }

                var parts: [String] = []
                parts.append(ident(col.name))
                parts.append(formatType(col))
                if col.collationName != nil, let coll = col.collationName { parts.append("COLLATE \(coll)") }
                if col.isSparse { parts.append("SPARSE") }
                if col.isRowGuidCol { parts.append("ROWGUIDCOL") }
                if col.isIdentity {
                    if let seed = col.identitySeed, let inc = col.identityIncrement {
                        parts.append("IDENTITY(\(seed), \(inc))")
                    } else {
                        parts.append("IDENTITY")
                    }
                }
                if let gen = col.generatedAlwaysType {
                    // 1 = AS_ROW_START, 2 = AS_ROW_END
                    if gen == 1 { parts.append("GENERATED ALWAYS AS ROW START") }
                    if gen == 2 { parts.append("GENERATED ALWAYS AS ROW END") }
                }
                parts.append(col.isNullable ? "NULL" : "NOT NULL")
                if let def = col.defaultDefinition, !def.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    if let defName = col.defaultName, !defName.isEmpty, !Self.isSystemGeneratedDefaultName(defName) {
                        parts.append("CONSTRAINT \(ident(defName)) DEFAULT \(def)")
                    } else {
                        // Omit system-generated default names (DF__...) to avoid cross-table name collisions
                        // and match SSMS/JDBC style scripting of anonymous defaults.
                        parts.append("DEFAULT \(def)")
                    }
                }
                lines.append("    " + parts.joined(separator: " "))
            }

            // PERIOD FOR SYSTEM_TIME
            if let period = temporal.periodClause { lines.append("    \(period)") }

            func formatKeyColumns(_ cols: [KeyColumnMetadata]) -> String {
                cols.sorted { $0.ordinal < $1.ordinal }
                    .map { "\(ident($0.column)) \($0.isDescending ? "DESC" : "ASC")" }
                    .joined(separator: ", ")
            }

            for pk in pks {
                let clustered = pk.isClustered ? "CLUSTERED" : "NONCLUSTERED"
                let clause = formatKeyColumns(pk.columns)
                // Avoid emitting explicit constraint names to prevent cross-table name collisions
                var pkLine = "    PRIMARY KEY \(clustered) (\(clause))"
                if let idx = ixs.first(where: { $0.name.caseInsensitiveCompare(pk.name) == .orderedSame }) {
                    if let opts = idx.optionClause, !opts.isEmpty { pkLine += " WITH (\(opts))" }
                    if let storage = idx.storageClause, !storage.isEmpty { pkLine += " \(storage)" }
                }
                lines.append(pkLine)
            }

            for uq in uqs {
                let clustered = uq.isClustered ? "CLUSTERED" : "NONCLUSTERED"
                let clause = formatKeyColumns(uq.columns)
                var constraintLine = "    CONSTRAINT \(ident(uq.name)) UNIQUE \(clustered) (\(clause))"
                if let idx = ixs.first(where: { $0.name.caseInsensitiveCompare(uq.name) == .orderedSame }) {
                    if let opts = idx.optionClause, !opts.isEmpty { constraintLine += " WITH (\(opts))" }
                    if let storage = idx.storageClause, !storage.isEmpty { constraintLine += " \(storage)" }
                }
                lines.append(constraintLine)
            }

            for ck in checks {
                let def = ck.definition
                lines.append("    CONSTRAINT \(ident(ck.name)) CHECK (\(def))")
            }

            for fk in fks {
                let cols = fk.columns.sorted { $0.ordinal < $1.ordinal }
                let left = cols.map { ident($0.parentColumn) }.joined(separator: ", ")
                let right = cols.map { ident($0.referencedColumn) }.joined(separator: ", ")
                var clause = "    CONSTRAINT \(ident(fk.name)) FOREIGN KEY (\(left)) REFERENCES \(qualified(fk.referencedSchema, fk.referencedTable)) (\(right))"
                let del = fk.deleteAction.uppercased()
                let upd = fk.updateAction.uppercased()
                if del != "NO ACTION" { clause += " ON DELETE \(del)" }
                if upd != "NO ACTION" { clause += " ON UPDATE \(upd)" }
                lines.append(clause)
            }

            // Preamble: ensure script executes in the correct database context, mirroring SSMS
            let dbPreamble: String = {
                if let db = self.effectiveDatabase(database), !db.isEmpty {
                    return "USE [\(SQLServerMetadataClient.escapeIdentifier(db))]\nGO\n"
                }
                return ""
            }()

            let header = "CREATE TABLE \(qualified(schema, table)) (\n"
            let body = lines.joined(separator: ",\n")
            var script = dbPreamble + header + body + "\n)"

            // Order matters for CREATE TABLE: storage comes before WITH options
            if let storage = fg, !storage.isEmpty { script += " \(storage)" }
            if let textimg = lob.textImageClause { script += " \(textimg)" }
            if let fs = lob.filestreamClause { script += " \(fs)" }

            var withOptions: [String] = []
            if let mem = temporal.memoryOptimizedClause { withOptions.append(mem) }
            if let sv = temporal.systemVersioningClause { withOptions.append(sv) }
            if !withOptions.isEmpty { script += " WITH (\(withOptions.joined(separator: ", ")))" }
            script += ";"

            // Append index scripts, excluding those created by constraints
            let constraintNames = Set((pks + uqs).map { $0.name })
            // Helper: normalize filter definitions from sys.indexes to a human-friendly
            // predicate without outer parentheses and bracketed identifiers so tests
            // can assert on simple forms like "Name IS NOT NULL".
            func normalizePredicate(_ predicate: String) -> String {
                func stripBalancedOuterParens(_ s: String) -> String {
                    var t = s.trimmingCharacters(in: .whitespacesAndNewlines)
                    guard t.first == "(", t.last == ")" else { return t }
                    // Ensure the outer parentheses are balanced before stripping
                    var depth = 0
                    var balanced = true
                    for (i, ch) in t.enumerated() {
                        if ch == "(" { depth += 1 }
                        if ch == ")" {
                            depth -= 1
                            if depth < 0 { balanced = false; break }
                            if depth == 0 && i != t.count - 1 { balanced = false; break }
                        }
                    }
                    if balanced && depth == 0 {
                        t.removeFirst(); t.removeLast()
                        return t.trimmingCharacters(in: .whitespacesAndNewlines)
                    }
                    return t
                }
                // Remove outer parentheses iteratively if balanced
                var t = predicate
                while t.hasPrefix("(") && t.hasSuffix(")") {
                    let stripped = stripBalancedOuterParens(t)
                    if stripped == t { break }
                    t = stripped
                }
                // De-bracket identifiers: [Name] -> Name (best-effort)
                t.removeAll { $0 == "[" || $0 == "]" }
                return t
            }

            for ix in ixs where !constraintNames.contains(ix.name) {
                // Columnstore variants
                if ix.indexType == 5 || ix.indexType == 6 {
                    let csKind = ix.indexType == 5 ? "CLUSTERED COLUMNSTORE" : "NONCLUSTERED COLUMNSTORE"
                    var stmt = "\n\nCREATE \(csKind) INDEX \(ident(ix.name)) ON \(qualified(schema, table))"
                    if ix.indexType == 6 { // nonclustered columnstore can list columns
                        let cols = ix.columns.sorted { $0.ordinal < $1.ordinal }.map { ident($0.column) }.joined(separator: ", ")
                        if !cols.isEmpty { stmt += " (\(cols))" }
                    }
                    if let opts = ix.optionClause, !opts.isEmpty { stmt += " WITH (\(opts))" }
                    if let storage = ix.storageClause, !storage.isEmpty { stmt += " \(storage)" }
                    stmt += ";"
                    script.append(stmt)
                    continue
                }

                let kind = ix.isClustered ? "CLUSTERED" : "NONCLUSTERED"
                let uniq = ix.isUnique ? "UNIQUE " : ""
                let keyCols = ix.columns.filter { !$0.isIncluded }.sorted { $0.ordinal < $1.ordinal }
                    .map { "\(ident($0.column)) \($0.isDescending ? "DESC" : "ASC")" }
                    .joined(separator: ", ")
                let includedCols = ix.columns.filter { $0.isIncluded }.map { ident($0.column) }
                var ixStmt = "\n\nCREATE \(uniq)\(kind) INDEX \(ident(ix.name)) ON \(qualified(schema, table)) (\(keyCols))"
                if !includedCols.isEmpty {
                    ixStmt += " INCLUDE (\(includedCols.joined(separator: ", ")))"
                }
                if let filter = ix.filterDefinition, !filter.isEmpty {
                    ixStmt += " WHERE \(normalizePredicate(filter))"
                }
                if let opts = ix.optionClause, !opts.isEmpty { ixStmt += " WITH (\(opts))" }
                if let storage = ix.storageClause, !storage.isEmpty { ixStmt += " \(storage)" }
                ixStmt += ";"
                script.append(ixStmt)
            }

            return script
        }
    }

    // Check constraint metadata (local to scripting)
    private struct CheckConstraintInfo { var name: String; var definition: String }

    private func listCheckConstraints(database: String?, schema: String, table: String) -> EventLoopFuture<[CheckConstraintInfo]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT ck.name, ck.definition
        FROM \(dbPrefix)sys.check_constraints AS ck
        JOIN \(dbPrefix)sys.tables AS t ON ck.parent_object_id = t.object_id
        JOIN \(dbPrefix)sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'
        ORDER BY ck.name;
        """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let def = row.column("definition")?.string else { return nil }
                return CheckConstraintInfo(name: name, definition: def.trimmingCharacters(in: .whitespacesAndNewlines))
            }
        }
    }

    private func fetchTableFilegroup(database: String?, schema: String, table: String) -> EventLoopFuture<String?> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT TOP 1 ds.name AS data_space_name, ps.name AS partition_scheme_name,
               STRING_AGG(CASE WHEN ic.partition_ordinal > 0 THEN c.name ELSE NULL END, ',') WITHIN GROUP (ORDER BY ic.partition_ordinal) AS partition_columns
        FROM \(dbPrefix)sys.tables AS t
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        JOIN \(dbPrefix)sys.indexes AS i ON i.object_id = t.object_id AND i.index_id IN (0,1)
        JOIN \(dbPrefix)sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        LEFT JOIN \(dbPrefix)sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
        LEFT JOIN \(dbPrefix)sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'
        GROUP BY ds.name, ps.name
        ORDER BY CASE WHEN ps.name IS NULL THEN 1 ELSE 0 END, ds.name;
        """
        return queryExecutor(sql).map { rows in
            guard let row = rows.first else { return nil }
            let ds = row.column("data_space_name")?.string
            let ps = row.column("partition_scheme_name")?.string
            let cols = row.column("partition_columns")?.string?.split(separator: ",").map { String($0) } ?? []
            if let ps, !ps.isEmpty {
                let colList = cols.map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]" }.joined(separator: ", ")
                return "ON [\(SQLServerMetadataClient.escapeIdentifier(ps))](\(colList))"
            }
            if let ds, !ds.isEmpty {
                return "ON [\(SQLServerMetadataClient.escapeIdentifier(ds))]"
            }
            return nil
        }
    }

    // Detailed columns for table scripting
    private struct DetailedColumn {
        var columnId: Int
        var name: String
        var typeName: String
        var maxLength: Int?
        var precision: Int?
        var scale: Int?
        var isNullable: Bool
        var isIdentity: Bool
        var identitySeed: Int?
        var identityIncrement: Int?
        var isRowGuidCol: Bool
        var isSparse: Bool
        var generatedAlwaysType: Int?
        var computedDefinition: String?
        var isComputedPersisted: Bool
        var defaultName: String?
        var defaultDefinition: String?
        var checkDefinition: String?
        var collationName: String?
    }

    // Detects system-generated default constraint names (DF__...), which are not stable across
    // environments and can collide across tables. We omit these names when scripting.
    private static func isSystemGeneratedDefaultName(_ name: String) -> Bool {
        return name.hasPrefix("DF__")
    }

    private func fetchDetailedColumns(database: String?, schema: String, table: String) -> EventLoopFuture<[DetailedColumn]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT
            c.column_id,
            c.name AS column_name,
            TYPE_NAME(c.user_type_id) AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            ic.seed_value,
            ic.increment_value,
            c.is_rowguidcol,
            c.is_sparse,
            c.generated_always_type,
            cc.definition AS computed_definition,
            cc.is_persisted,
            dc.name AS default_name,
            dc.definition AS default_definition,
            c.collation_name
        FROM \(dbPrefix)sys.columns AS c
        JOIN \(dbPrefix)sys.tables AS t ON t.object_id = c.object_id
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN \(dbPrefix)sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
        LEFT JOIN \(dbPrefix)sys.identity_columns AS ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        LEFT JOIN \(dbPrefix)sys.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'
        ORDER BY c.column_id;
        """

        func intLike(_ row: TDSRow, _ key: String) -> Int? {
            if let i = row.column(key)?.int { return i }
            if let s = row.column(key)?.string, let v = Int(s) { return v }
            return nil
        }

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard
                    let colId = row.column("column_id")?.int,
                    let name = row.column("column_name")?.string,
                    let type = row.column("type_name")?.string
                else { return nil }
                return DetailedColumn(
                    columnId: colId,
                    name: name,
                    typeName: type,
                    maxLength: row.column("max_length")?.int,
                    precision: row.column("precision")?.int,
                    scale: row.column("scale")?.int,
                    isNullable: (row.column("is_nullable")?.int ?? 1) != 0,
                    isIdentity: (row.column("is_identity")?.int ?? 0) != 0,
                    identitySeed: intLike(row, "seed_value"),
                    identityIncrement: intLike(row, "increment_value"),
                    isRowGuidCol: (row.column("is_rowguidcol")?.int ?? 0) != 0,
                    isSparse: (row.column("is_sparse")?.int ?? 0) != 0,
                    generatedAlwaysType: row.column("generated_always_type")?.int,
                    computedDefinition: row.column("computed_definition")?.string,
                    isComputedPersisted: (row.column("is_persisted")?.int ?? 0) != 0,
                    defaultName: row.column("default_name")?.string,
                    defaultDefinition: row.column("default_definition")?.string,
                    collationName: row.column("collation_name")?.string
                )
            }
        }
    }

    private struct LobFilestreamInfo { var textImageClause: String?; var filestreamClause: String? }

    private func fetchLobAndFilestreamStorage(database: String?, schema: String, table: String) -> EventLoopFuture<LobFilestreamInfo> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        // LOB storage (TEXTIMAGE_ON)
        let lobSql = """
        SELECT TOP 1 ds.name AS lob_data_space
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        JOIN \(dbPrefix)sys.partitions AS p ON p.object_id = o.object_id
        JOIN \(dbPrefix)sys.allocation_units AS au ON au.container_id = p.hobt_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = au.data_space_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND o.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'
          AND au.type IN (2) -- LOB_DATA
        ORDER BY ds.name;
        """
        // FILESTREAM filegroup
        let fsSql = """
        SELECT ds.name AS filestream_data_space
        FROM \(dbPrefix)sys.tables AS t
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = t.filestream_data_space_id
        WHERE t.filestream_data_space_id IS NOT NULL
          AND s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))';
        """
        let lobF = queryExecutor(lobSql).map { rows -> String? in
            rows.first?.column("lob_data_space")?.string.map { "TEXTIMAGE_ON [\(SQLServerMetadataClient.escapeIdentifier($0))]" }
        }
        let fsF = queryExecutor(fsSql).map { rows -> String? in
            rows.first?.column("filestream_data_space")?.string.map { "FILESTREAM_ON [\(SQLServerMetadataClient.escapeIdentifier($0))]" }
        }
        return lobF.and(fsF).map { (lob, fs) in LobFilestreamInfo(textImageClause: lob, filestreamClause: fs) }
    }

    private struct TemporalAndMemoryOptions { var periodClause: String?; var systemVersioningClause: String?; var memoryOptimizedClause: String? }

    private func fetchTemporalAndMemoryOptions(database: String?, schema: String, table: String) -> EventLoopFuture<TemporalAndMemoryOptions> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT t.temporal_type, t.history_table_id, t.is_memory_optimized, t.durability_desc,
               pstart.start_column_id, pend.end_column_id,
               cs.name AS history_schema, co.name AS history_table
        FROM \(dbPrefix)sys.tables AS t
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN \(dbPrefix)sys.objects AS ho ON ho.object_id = t.history_table_id
        LEFT JOIN \(dbPrefix)sys.schemas AS cs ON cs.schema_id = ho.schema_id
        LEFT JOIN \(dbPrefix)sys.objects AS co ON co.object_id = t.history_table_id
        LEFT JOIN \(dbPrefix)sys.periods AS p ON p.object_id = t.object_id
        OUTER APPLY (SELECT start_column_id FROM \(dbPrefix)sys.periods WHERE object_id = t.object_id) AS pstart
        OUTER APPLY (SELECT end_column_id FROM \(dbPrefix)sys.periods WHERE object_id = t.object_id) AS pend
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))' AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))';
        """
        func columnName(_ id: Int?, _ dbPrefix: String, _ schema: String, _ table: String) -> EventLoopFuture<String?> {
            guard let id else { return connection.eventLoop.makeSucceededFuture(nil) }
            let q = """
            SELECT c.name FROM \(dbPrefix)sys.columns AS c
            JOIN \(dbPrefix)sys.tables AS t ON t.object_id = c.object_id
            JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))' AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))' AND c.column_id = \(id);
            """
            return queryExecutor(q).map { $0.first?.column("name")?.string }
        }
        return queryExecutor(sql).flatMap { rows in
            guard let row = rows.first else { return self.connection.eventLoop.makeSucceededFuture(TemporalAndMemoryOptions(periodClause: nil, systemVersioningClause: nil, memoryOptimizedClause: nil)) }
            let temporalType = row.column("temporal_type")?.int ?? 0
            let isMemOpt = (row.column("is_memory_optimized")?.int ?? 0) != 0
            let durability = row.column("durability_desc")?.string
            let histSchema = row.column("history_schema")?.string
            let histTable = row.column("history_table")?.string
            let startId = row.column("start_column_id")?.int
            let endId = row.column("end_column_id")?.int

            let memClause: String? = isMemOpt ? "MEMORY_OPTIMIZED = ON\(durability.map { ", DURABILITY = \($0)" } ?? "")" : nil

            if temporalType == 0 {
                return self.connection.eventLoop.makeSucceededFuture(TemporalAndMemoryOptions(periodClause: nil, systemVersioningClause: nil, memoryOptimizedClause: memClause))
            }
            // Resolve period column names then build clauses
            return columnName(startId, dbPrefix, schema, table).and(columnName(endId, dbPrefix, schema, table)).map { (startName, endName) in
                let period: String?
                if let s = startName, let e = endName { period = "PERIOD FOR SYSTEM_TIME ([\(SQLServerMetadataClient.escapeIdentifier(s))], [\(SQLServerMetadataClient.escapeIdentifier(e))])" } else { period = nil }
                var systemVersioning: String? = "SYSTEM_VERSIONING = ON"
                if let hs = histSchema, let ht = histTable, !hs.isEmpty, !ht.isEmpty {
                    systemVersioning = "SYSTEM_VERSIONING = ON (HISTORY_TABLE = [\(SQLServerMetadataClient.escapeIdentifier(hs))].[\(SQLServerMetadataClient.escapeIdentifier(ht))])"
                }
                return TemporalAndMemoryOptions(periodClause: period, systemVersioningClause: systemVersioning, memoryOptimizedClause: memClause)
            }
        }
    }

    // Index details for scripting
    private struct IndexDetail {
        var name: String
        var isUnique: Bool
        var isClustered: Bool
        var indexType: Int // matches sys.indexes.type
        var filterDefinition: String?
        var columns: [IndexColumnMetadata]
        var storageClause: String? // e.g., "ON [PRIMARY]" or "ON [scheme]([col])"
        var optionClause: String?  // e.g., "PAD_INDEX = OFF, FILLFACTOR = 90, ..."
    }

    private func fetchObjectIndexDetails(database: String?, schema: String, object: String) -> EventLoopFuture<[IndexDetail]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""

        // Query for base index + columns + storage + options
        let sql = """
        SELECT
            s.name AS schema_name,
            o.name AS table_name,
            i.name AS index_name,
            i.is_unique,
            i.type AS index_type,
            i.is_padded,
            i.fill_factor,
            i.allow_row_locks,
            i.allow_page_locks,
            i.ignore_dup_key,
            i.filter_definition,
            ds.name AS data_space_name,
            ps.name AS partition_scheme_name,
            ic.key_ordinal,
            ic.is_descending_key,
            ic.is_included_column,
            c.name AS column_name,
            ic.partition_ordinal,
            pcomp.comp_desc AS compression_desc
        FROM \(dbPrefix)sys.indexes AS i
        JOIN \(dbPrefix)sys.objects AS o ON i.object_id = o.object_id AND o.type IN ('U','V')
        JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
        LEFT JOIN \(dbPrefix)sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
        JOIN \(dbPrefix)sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN \(dbPrefix)sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        OUTER APPLY (
            SELECT CASE WHEN MIN(p.data_compression_desc) = MAX(p.data_compression_desc)
                        THEN MIN(p.data_compression_desc) ELSE NULL END AS comp_desc
            FROM \(dbPrefix)sys.partitions AS p
            WHERE p.object_id = i.object_id AND p.index_id = i.index_id
        ) AS pcomp
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND o.name = N'\(SQLServerMetadataClient.escapeLiteral(object))'
          AND i.index_id > 0
          AND i.is_hypothetical = 0
        ORDER BY s.name, o.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
        """

        struct Partial { var isUnique: Bool; var isClustered: Bool; var idxType: Int; var filter: String?; var storage: String?; var opts: String?; var cols: [IndexColumnMetadata]; var partitionCols: [String]; var dsName: String?; var psName: String?; var isPadded: Bool; var fillFactor: Int; var allowRow: Bool; var allowPage: Bool; var ignoreDup: Bool; var compression: String? }
        return queryExecutor(sql).map { rows in
            var grouped: [String: Partial] = [:]
            for row in rows {
                guard let idxName = row.column("index_name")?.string else { continue }
                var p = grouped[idxName] ?? Partial(
                    isUnique: (row.column("is_unique")?.int ?? 0) != 0,
                    isClustered: (row.column("index_type")?.int ?? 0) == 1,
                    idxType: row.column("index_type")?.int ?? 0,
                    filter: row.column("filter_definition")?.string,
                    storage: nil,
                    opts: nil,
                    cols: [],
                    partitionCols: [],
                    dsName: row.column("data_space_name")?.string,
                    psName: row.column("partition_scheme_name")?.string,
                    isPadded: (row.column("is_padded")?.int ?? 0) != 0,
                    fillFactor: row.column("fill_factor")?.int ?? 0,
                    allowRow: (row.column("allow_row_locks")?.int ?? 1) != 0,
                    allowPage: (row.column("allow_page_locks")?.int ?? 1) != 0,
                    ignoreDup: (row.column("ignore_dup_key")?.int ?? 0) != 0,
                    compression: row.column("compression_desc")?.string
                )
                if let colName = row.column("column_name")?.string {
                    let ord = row.column("key_ordinal")?.int ?? 0
                    let isDesc = (row.column("is_descending_key")?.int ?? 0) != 0
                    let included = (row.column("is_included_column")?.int ?? 0) != 0
                    p.cols.append(IndexColumnMetadata(column: colName, ordinal: ord, isDescending: isDesc, isIncluded: included))
                    if (row.column("partition_ordinal")?.int ?? 0) > 0 {
                        p.partitionCols.append(colName)
                    }
                }
                grouped[idxName] = p
            }

            func buildOptions(from p: Partial) -> String? {
                var parts: [String] = []
                parts.append("PAD_INDEX = \(p.isPadded ? "ON" : "OFF")")
                if p.fillFactor > 0 { parts.append("FILLFACTOR = \(p.fillFactor)") }
                parts.append("ALLOW_ROW_LOCKS = \(p.allowRow ? "ON" : "OFF")")
                parts.append("ALLOW_PAGE_LOCKS = \(p.allowPage ? "ON" : "OFF")")
                if p.ignoreDup { parts.append("IGNORE_DUP_KEY = ON") }
                if let comp = p.compression, comp != "NONE" {
                    parts.append("DATA_COMPRESSION = \(comp)")
                }
                return parts.isEmpty ? nil : parts.joined(separator: ", ")
            }

            func buildStorage(from p: Partial) -> String? {
                if let scheme = p.psName, !scheme.isEmpty {
                    if !p.partitionCols.isEmpty {
                        let cols = p.partitionCols.map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]" }.joined(separator: ", ")
                        return "ON [\(SQLServerMetadataClient.escapeIdentifier(scheme))](\(cols))"
                    } else {
                        return "ON [\(SQLServerMetadataClient.escapeIdentifier(scheme))]"
                    }
                }
                if let ds = p.dsName, !ds.isEmpty { return "ON [\(SQLServerMetadataClient.escapeIdentifier(ds))]" }
                return nil
            }

            return grouped.map { (name, p) in
                IndexDetail(
                    name: name,
                    isUnique: p.isUnique,
                    isClustered: p.isClustered,
                    indexType: p.idxType,
                    filterDefinition: p.filter,
                    columns: p.cols,
                    storageClause: buildStorage(from: p),
                    optionClause: buildOptions(from: p)
                )
            }.sorted { $0.name < $1.name }
        }
    }

    // MARK: - Server Info

    /// Returns the SQL Server product version in the form `major.minor.build.revision`
    /// (e.g. "16.0.1000.5").
    public func serverVersion() -> EventLoopFuture<String> {
        let sql = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS version;"
        return queryExecutor(sql).flatMapThrowing { rows in
            if let value = rows.first?.column("version")?.string, !value.isEmpty {
                return value
            }
            throw SQLServerError.sqlExecutionError(message: "Failed to fetch server version")
        }
    }

    @available(macOS 12.0, *)
    public func serverVersion() async throws -> String {
        try await serverVersion().get()
    }

    // MARK: - Databases

    public func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        let sql = "EXEC sp_databases;"
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("DATABASE_NAME")?.string else { return nil }
                return DatabaseMetadata(name: name)
            }
        }
    }

    // MARK: - Schemas

    public func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        let qualifiedSchemas = qualified(database, object: "sys.schemas")
        var predicates: [String] = []
        if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        let sql = """
        SELECT s.name
        FROM \(qualifiedSchemas) AS s
        \(predicates.isEmpty ? "" : "WHERE " + predicates.joined(separator: " AND "))
        ORDER BY s.name;
        """
        return queryExecutor(sql).map { rows in
            let schemas: [SchemaMetadata] = rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SchemaMetadata(name: name)
            }
            return schemas
        }
    }

    // MARK: - Tables

    public func listTables(database: String? = nil, schema: String? = nil, includeComments: Bool = false) -> EventLoopFuture<[TableMetadata]> {
        let qualifiedObjects = qualified(database, object: "sys.objects")
        let qualifiedSchemas = qualified(database, object: "sys.schemas")
        let qualifiedExtended = qualified(database, object: "sys.extended_properties")

        var predicates: [String] = [
            "o.type IN ('U', 'S', 'V', 'TT')"
        ]

        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        }

        if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }

        let whereClause = predicates.isEmpty ? "" : "WHERE " + predicates.joined(separator: " AND ")

        let commentSelect = includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : ""
        let commentJoin = includeComments ? "LEFT JOIN \(qualifiedExtended) AS ep ON ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.class = 1 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            CASE
                WHEN o.type = 'S' OR o.is_ms_shipped = 1 THEN 'SYSTEM TABLE'
                WHEN o.type = 'U' THEN 'TABLE'
                WHEN o.type = 'V' THEN 'VIEW'
                WHEN o.type = 'TT' THEN 'TABLE TYPE'
                ELSE o.type_desc
            END AS table_type,
            o.is_ms_shipped\(commentSelect)
        FROM \(qualifiedObjects) AS o
        INNER JOIN \(qualifiedSchemas) AS s
            ON s.schema_id = o.schema_id
        \(commentJoin)
        \(whereClause)
        ORDER BY s.name, o.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("object_name")?.string,
                    let tableType = row.column("table_type")?.string
                else {
                    return nil
                }

                if let schema, schemaName.caseInsensitiveCompare(schema) != .orderedSame {
                    return nil
                }

                if tableName.hasPrefix("meta_client_") {
                    return nil
                }

                if !self.configuration.includeSystemSchemas,
                   schemaName.caseInsensitiveCompare("sys") == .orderedSame ||
                   schemaName.caseInsensitiveCompare("INFORMATION_SCHEMA") == .orderedSame {
                    return nil
                }

                let normalizedType = tableType.uppercased()

                let isSystemObject: Bool
                if normalizedType.contains("SYSTEM") {
                    isSystemObject = true
                } else {
                    let msShipped = (row.column("is_ms_shipped")?.int ?? 0) != 0
                    isSystemObject = msShipped
                }
                let commentValue: String?
                if includeComments {
                    commentValue = row.column("comment")?.string
                } else {
                    commentValue = nil
                }
                return TableMetadata(
                    schema: schemaName,
                    name: tableName,
                    type: normalizedType,
                    isSystemObject: isSystemObject,
                    comment: commentValue
                )
            }
        }
    }

    // MARK: - Columns

    public func listColumns(
        database: String? = nil,
        schema: String,
        table: String,
        objectTypeHint: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[ColumnMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let cacheKey = "\(resolvedDatabase ?? "").\(schema).\(table)"
        if !includeComments, let cache, let cached = cache.value(forKey: cacheKey) {
            return connection.eventLoop.makeSucceededFuture(cached)
        }

        let isViewFuture: EventLoopFuture<Bool>
        if let objectTypeHint {
            let normalized = objectTypeHint.uppercased()
            let isView = normalized.contains("VIEW") || normalized == "V"
            isViewFuture = connection.eventLoop.makeSucceededFuture(isView)
        } else {
            isViewFuture = isViewObject(database: resolvedDatabase, schema: schema, table: table)
        }

        return isViewFuture.flatMap { isView -> EventLoopFuture<[ColumnMetadata]> in
            let useStoredProc = !isView && self.configuration.preferStoredProcedureColumns
            let mode = (isView || !useStoredProc) ? "catalog" : "stored_procedure"
            let contextDescription = "database=\(resolvedDatabase ?? "<default>") schema=\(schema) table=\(table)"
            self.logger.trace("[Metadata] listColumns start \(contextDescription) hint=\(objectTypeHint ?? "<nil>") mode=\(mode)")
            let startTime = DispatchTime.now()
            let baseSource: EventLoopFuture<[ColumnMetadata]>
            if isView || !useStoredProc {
                baseSource = self.loadColumnsFromCatalog(database: resolvedDatabase, schema: schema, table: table, includeDefaultMetadata: false, includeComments: includeComments)
            } else {
                baseSource = self.loadColumnsUsingStoredProcedure(database: resolvedDatabase, schema: schema, table: table).flatMap { cols in
                    guard includeComments else { return self.connection.eventLoop.makeSucceededFuture(cols) }
                    return self.fetchColumnComments(database: resolvedDatabase, schema: schema, table: table).map { commentMap in
                        cols.map { c in
                            ColumnMetadata(
                                schema: c.schema,
                                table: c.table,
                                name: c.name,
                                typeName: c.typeName,
                                systemTypeName: c.systemTypeName,
                                maxLength: c.maxLength,
                                precision: c.precision,
                                scale: c.scale,
                                collationName: c.collationName,
                                isNullable: c.isNullable,
                                isIdentity: c.isIdentity,
                                isComputed: c.isComputed,
                                hasDefaultValue: c.hasDefaultValue,
                                defaultDefinition: c.defaultDefinition,
                                computedDefinition: c.computedDefinition,
                                ordinalPosition: c.ordinalPosition,
                                identitySeed: c.identitySeed,
                                identityIncrement: c.identityIncrement,
                                checkDefinition: c.checkDefinition,
                                comment: commentMap[c.name]
                            )
                        }
                    }
                }
            }

            return baseSource.map { columns in
                let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000
                self.logger.trace("[Metadata] listColumns completed \(contextDescription) mode=\(mode) rows=\(columns.count) elapsedMs=\(String(format: "%.2f", elapsed))")
                if !includeComments, !columns.isEmpty, let cache = self.cache {
                    cache.setValue(columns, forKey: cacheKey)
                }
                return columns
            }
        }.flatMapError { _ in
            self.logger.warning("[Metadata] listColumns falling back to stored procedure for database=\(resolvedDatabase ?? "<default>") schema=\(schema) table=\(table)")
            // Fallback to stored procedure if we cannot resolve the object type or catalog lookup failed.
            return self.loadColumnsUsingStoredProcedure(database: resolvedDatabase, schema: schema, table: table).flatMap { base in
                let final: EventLoopFuture<[ColumnMetadata]>
                if includeComments {
                    final = self.fetchColumnComments(database: resolvedDatabase, schema: schema, table: table).map { comments in
                        base.map { c in
                            ColumnMetadata(
                                schema: c.schema,
                                table: c.table,
                                name: c.name,
                                typeName: c.typeName,
                                systemTypeName: c.systemTypeName,
                                maxLength: c.maxLength,
                                precision: c.precision,
                                scale: c.scale,
                                collationName: c.collationName,
                                isNullable: c.isNullable,
                                isIdentity: c.isIdentity,
                                isComputed: c.isComputed,
                                hasDefaultValue: c.hasDefaultValue,
                                defaultDefinition: c.defaultDefinition,
                                computedDefinition: c.computedDefinition,
                                ordinalPosition: c.ordinalPosition,
                                identitySeed: c.identitySeed,
                                identityIncrement: c.identityIncrement,
                                checkDefinition: c.checkDefinition,
                                comment: comments[c.name]
                            )
                        }
                    }
                } else {
                    final = self.connection.eventLoop.makeSucceededFuture(base)
                }
                return final.map { columns in
                    if !includeComments, !columns.isEmpty, let cache = self.cache {
                        cache.setValue(columns, forKey: cacheKey)
                    }
                    return columns
                }
            }
        }
    }

    private func fetchColumnComments(database: String?, schema: String, table: String) -> EventLoopFuture<[String: String]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let escapedSchema = SQLServerMetadataClient.escapeLiteral(schema)
        let escapedTable = SQLServerMetadataClient.escapeLiteral(table)
        let sql = """
        SELECT c.name AS column_name, comment = ISNULL(CAST(ep.value AS NVARCHAR(4000)), '')
        FROM \(dbPrefix)sys.columns AS c
        JOIN \(dbPrefix)sys.objects AS o ON c.object_id = o.object_id
        JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
        LEFT JOIN \(dbPrefix)sys.extended_properties AS ep
            ON ep.class = 1 AND ep.major_id = o.object_id AND ep.minor_id = c.column_id AND ep.name = N'MS_Description'
        WHERE s.name = N'\(escapedSchema)' AND o.name = N'\(escapedTable)';
        """
        return queryExecutor(sql).map { rows in
            var dict: [String: String] = [:]
            for row in rows {
                if let name = row.column("column_name")?.string, let value = row.column("comment")?.string {
                    dict[name] = value
                }
            }
            return dict
        }
    }

    private func isViewObject(
        database: String?,
        schema: String,
        table: String
    ) -> EventLoopFuture<Bool> {
        let databaseLiteral: String
        if let resolved = effectiveDatabase(database) {
            databaseLiteral = "[\(SQLServerMetadataClient.escapeIdentifier(resolved))]."
        } else {
            databaseLiteral = ""
        }
        let schemaLiteral = "[\(SQLServerMetadataClient.escapeIdentifier(schema))]"
        let tableLiteral = "[\(SQLServerMetadataClient.escapeIdentifier(table))]"
        let identifier = "\(databaseLiteral)\(schemaLiteral).\(tableLiteral)"
        let sql = "SELECT is_view = CONVERT(bit, OBJECTPROPERTYEX(OBJECT_ID(N'\(SQLServerMetadataClient.escapeLiteral(identifier))'), 'IsView'))"
        return queryExecutor(sql).map { rows in
            guard let value = rows.first?.column("is_view")?.int else { return false }
            return value != 0
        }
    }

    private func loadColumnsUsingStoredProcedure(
        database: String?,
        schema: String,
        table: String
    ) -> EventLoopFuture<[ColumnMetadata]> {
        var parameters: [String] = [
            "@table_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'",
            "@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'",
            "@ODBCVer = 3"
        ]
        if let qualifier = effectiveDatabase(database) {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(qualifier))'")
        }
        // Reduce token churn by disabling rowcount messages from the stored procedure
        let sql = "SET NOCOUNT ON; EXEC sp_columns_100 \(parameters.joined(separator: ", "));"
        logger.trace("[Metadata] loadColumnsUsingStoredProcedure SQL for \(schema).\(table): \(sql)")

        return queryExecutor(sql).map { rows in
            rows.compactMap { row -> ColumnMetadata? in
                guard
                    let schemaName = row.column("TABLE_OWNER")?.string ?? row.column("TABLE_SCHEM")?.string,
                    let tableName = row.column("TABLE_NAME")?.string,
                    let columnName = row.column("COLUMN_NAME")?.string,
                    let typeName = row.column("TYPE_NAME")?.string,
                    let ordinal = row.column("ORDINAL_POSITION")?.int
                else {
                    return nil
                }

                let maxLength = row.column("LENGTH")?.int
                let precision = row.column("PRECISION")?.int ?? row.column("precision")?.int
                let scale = row.column("SCALE")?.int ?? row.column("scale")?.int
                let systemTypeName = row.column("TYPE_NAME")?.string
                let defaultDefinition = row.column("COLUMN_DEF")?.string
                let isNullable: Bool = (row.column("NULLABLE")?.int ?? 1) != 0
                let isIdentity: Bool = (row.column("SS_IS_IDENTITY")?.int ?? 0) != 0 ||
                    (row.column("IS_AUTOINCREMENT")?.string?.uppercased() == "YES")
                let isComputed: Bool = (row.column("SS_IS_COMPUTED")?.int ?? 0) != 0 ||
                    (row.column("IS_GENERATEDCOLUMN")?.string?.uppercased() == "YES")
                let hasDefaultValue = (defaultDefinition?.isEmpty == false)

                return ColumnMetadata(
                    schema: schemaName,
                    table: tableName,
                    name: columnName,
                    typeName: typeName,
                    systemTypeName: systemTypeName,
                    maxLength: maxLength,
                    precision: precision,
                    scale: scale,
                    collationName: nil,
                    isNullable: isNullable,
                    isIdentity: isIdentity,
                    isComputed: isComputed,
                    hasDefaultValue: hasDefaultValue,
                    defaultDefinition: defaultDefinition,
                    computedDefinition: nil,
                    ordinalPosition: ordinal,
                    identitySeed: nil,
                    identityIncrement: nil,
                    checkDefinition: nil,
                    comment: nil
                )
            }
        }
    }

    private func loadColumnsFromCatalog(
        database: String?,
        schema: String,
        table: String,
        includeDefaultMetadata: Bool,
        includeComments: Bool
    ) -> EventLoopFuture<[ColumnMetadata]> {
        let escapedSchema = SQLServerMetadataClient.escapeLiteral(schema)
        let escapedTable = SQLServerMetadataClient.escapeLiteral(table)
        let defaultSelect = includeDefaultMetadata
            ? "dc.definition AS default_definition"
            : "ISNULL(dc.definition, '') AS default_definition"
        let computedSelect = includeDefaultMetadata
            ? "cc.definition AS computed_definition"
            : "ISNULL(cc.definition, '') AS computed_definition"
        let identitySelect = includeDefaultMetadata
            ? "ic.seed_value AS identity_seed, ic.increment_value AS identity_increment"
            : "ISNULL(ic.seed_value, 0) AS identity_seed, ISNULL(ic.increment_value, 0) AS identity_increment"
        let checkSelect = includeDefaultMetadata
            ? "ck.definition AS check_definition"
            : "ISNULL(ck.definition, '') AS check_definition"

        var fromClause = """
        FROM \(qualified(database, object: "sys.columns")) AS c
        JOIN \(qualified(database, object: "sys.objects")) AS o ON c.object_id = o.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON o.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.types")) AS ut ON c.user_type_id = ut.user_type_id
        JOIN \(qualified(database, object: "sys.types")) AS st ON c.system_type_id = st.user_type_id AND st.user_type_id = st.system_type_id 
        """

        // Ensure there's a space before the LEFT JOIN
        // Always add LEFT JOINs for tables referenced in SELECT clause
        // The SELECT clause always references dc, cc, ic, ck regardless of includeDefaultMetadata setting
        fromClause += """
        LEFT JOIN \(qualified(database, object: "sys.default_constraints")) AS dc ON c.default_object_id = dc.object_id
        LEFT JOIN \(qualified(database, object: "sys.computed_columns")) AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
        LEFT JOIN \(qualified(database, object: "sys.identity_columns")) AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        LEFT JOIN \(qualified(database, object: "sys.check_constraints")) AS ck ON c.default_object_id = ck.object_id AND ck.parent_column_id = c.column_id
        """

        if includeComments {
            fromClause += """
        LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS epc
            ON epc.class = 1 AND epc.major_id = o.object_id AND epc.minor_id = c.column_id AND epc.name = N'MS_Description'
        """
        }

        let commentSelect = includeComments ? ", ISNULL(CAST(epc.value AS NVARCHAR(4000)), '') AS column_comment" : ""
        let sql = """
        SELECT
            schema_name = s.name,
            table_name = o.name,
            column_name = c.name,
            user_type_name = ut.name,
            system_type_name = st.name,
            max_length = c.max_length,
            precision = c.precision,
            scale = c.scale,
            collation_name = c.collation_name,
            is_nullable = c.is_nullable,
            is_identity = c.is_identity,
            is_computed = c.is_computed,
            \(defaultSelect),
            \(computedSelect),
            \(identitySelect),
            \(checkSelect),
            ordinal_position = c.column_id\(commentSelect)
        \(fromClause)
        WHERE s.name = N'\(escapedSchema)'
          AND o.name = N'\(escapedTable)'
        ORDER BY c.column_id;
        """

        logger.trace("[Metadata] loadColumnsFromCatalog SQL for \(schema).\(table): \(sql)")
        return queryExecutor(sql).map { rows in
            rows.compactMap { row -> ColumnMetadata? in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let columnName = row.column("column_name")?.string,
                    let typeName = row.column("user_type_name")?.string,
                    let ordinal = row.column("ordinal_position")?.int
                else {
                    return nil
                }

                let systemTypeName = row.column("system_type_name")?.string
                let rawLength = row.column("max_length")?.int
                let normalizedLength: Int?
                if let rawLength, let system = systemTypeName?.lowercased(), ["nchar", "nvarchar", "ntext"].contains(system), rawLength > 0 {
                    normalizedLength = rawLength / 2
                } else {
                    normalizedLength = rawLength
                }
                let precision = row.column("precision")?.int
                let scale = row.column("scale")?.int
                let collationName = row.column("collation_name")?.string
                let defaultDefinition = row.column("default_definition")?.string
                let computedDefinition = row.column("computed_definition")?.string
                let isNullable = (row.column("is_nullable")?.int ?? 1) != 0
                let isIdentity = (row.column("is_identity")?.int ?? 0) != 0
                let isComputed = (row.column("is_computed")?.int ?? 0) != 0
                let hasDefaultValue = (defaultDefinition?.isEmpty == false)
                let identitySeed = row.column("identity_seed")?.int
                let identityIncrement = row.column("identity_increment")?.int
                let checkDefinition = row.column("check_definition")?.string

                return ColumnMetadata(
                    schema: schemaName,
                    table: tableName,
                    name: columnName,
                    typeName: typeName,
                    systemTypeName: systemTypeName,
                    maxLength: normalizedLength,
                    precision: precision,
                    scale: scale,
                    collationName: collationName,
                    isNullable: isNullable,
                    isIdentity: isIdentity,
                    isComputed: isComputed,
                    hasDefaultValue: hasDefaultValue,
                    defaultDefinition: defaultDefinition,
                    computedDefinition: computedDefinition,
                    ordinalPosition: ordinal,
                    identitySeed: identitySeed,
                    identityIncrement: identityIncrement,
                    checkDefinition: checkDefinition,
                    comment: row.column("column_comment")?.string
                )
            }
        }
    }

    // MARK: - Parameters

    public func listParameters(
        database: String? = nil,
        schema: String,
        object: String
    ) -> EventLoopFuture<[ParameterMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let escapedSchema = SQLServerMetadataClient.escapeLiteral(schema)
        let escapedObject = SQLServerMetadataClient.escapeLiteral(object)

        let sql = """
        SELECT
            schema_name = s.name,
            object_name = o.name,
            object_type = o.type,
            parameter_id = p.parameter_id,
            parameter_name = p.name,
            user_type_name = ut.name,
            system_type_name = st.name,
            max_length = p.max_length,
            precision = p.precision,
            scale = p.scale,
            is_output = p.is_output,
            is_readonly = ISNULL(p.is_readonly, 0),
            has_default_value = p.has_default_value
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        LEFT JOIN \(dbPrefix)sys.parameters AS p ON p.object_id = o.object_id
        LEFT JOIN \(dbPrefix)sys.types AS ut ON ut.user_type_id = p.user_type_id AND ut.system_type_id = ut.user_type_id
        LEFT JOIN \(dbPrefix)sys.types AS st ON st.system_type_id = p.system_type_id AND st.user_type_id = st.system_type_id 
        WHERE s.name = N'\(escapedSchema)'
          AND o.name = N'\(escapedObject)'
          AND o.type IN ('P','PC','RF','AF','FN','TF','IF')
        ORDER BY p.parameter_id;
        """

        return queryExecutor(sql).flatMap { rows in
            let textDefaultsFuture: EventLoopFuture<[String: (hasDefault: Bool, defaultValue: String?)]> = rows.isEmpty
                ? self.connection.eventLoop.makeSucceededFuture([:])
                : self.loadParameterDefaults(database: database, schema: schema, object: object)

            return textDefaultsFuture.flatMap { defaults in
                // Decide function vs procedure behavior once per object
                let objectType = rows.first?.column("object_type")?.string?.uppercased() ?? ""
                let isFunctionObject = ["FN","TF","IF","AF","RF"].contains(objectType)

                // Primary mapping from sys.parameters rows
                let mapped: [ParameterMetadata] = rows.compactMap { row -> ParameterMetadata? in
                    guard
                        let schemaName = row.column("schema_name")?.string,
                        let rawObjectName = row.column("object_name")?.string,
                        let name = row.column("parameter_name")?.string,
                        let ordinal = row.column("parameter_id")?.int
                    else {
                        return nil
                    }

                    let typeName = row.column("user_type_name")?.string ?? row.column("system_type_name")?.string
                    let objectName = SQLServerMetadataClient.normalizeRoutineName(rawObjectName)
                    let systemType = row.column("system_type_name")?.string
                    let maxLength = row.column("max_length")?.int
                    let precision = row.column("precision")?.int
                    let scale = row.column("scale")?.int
                    let normalizedName = name.lowercased()
                    let override = defaults[normalizedName]
                    let resolvedDefault = override?.defaultValue
                    let baseHas = row.column("has_default_value")?.bool ?? false
                    let hasDefault = (override?.hasDefault ?? baseHas)
                    let isOutput = row.column("is_output")?.bool ?? false
                    let isReturnValue = (ordinal == 0) || normalizedName == "@return_value"

                    return ParameterMetadata(
                        schema: schemaName,
                        object: objectName,
                        name: name,
                        ordinal: ordinal,
                        isReturnValue: isReturnValue,
                        typeName: typeName ?? "",
                        systemTypeName: systemType,
                        maxLength: maxLength,
                        precision: precision,
                        scale: scale,
                        isOutput: isOutput,
                        hasDefaultValue: hasDefault,
                        defaultValue: resolvedDefault,
                        isReadOnly: row.column("is_readonly")?.bool ?? false
                    )
                }.filter { param in
                    // Keep return value entries for functions; suppress for procedures
                    return isFunctionObject ? true : !param.isReturnValue
                }
                if !mapped.isEmpty {
                    return self.connection.eventLoop.makeSucceededFuture(mapped)
                }
                

                // Fallback: Some databases (including AdventureWorks variants) may expose
                // function parameters only via INFORMATION_SCHEMA.PARAMETERS. Use it if sys.parameters yields none.
                let infoSchemaSQL = """
                SELECT
                    schema_name = p.SPECIFIC_SCHEMA,
                    object_name = p.SPECIFIC_NAME,
                    parameter_id = p.ORDINAL_POSITION,
                    parameter_name = p.PARAMETER_NAME,
                    user_type_name = p.DATA_TYPE,
                    system_type_name = p.DATA_TYPE,
                    max_length = p.CHARACTER_MAXIMUM_LENGTH,
                    precision = p.NUMERIC_PRECISION,
                    scale = p.NUMERIC_SCALE,
                    is_output = CASE WHEN p.PARAMETER_MODE LIKE '%OUT%' THEN 1 ELSE 0 END,
                    is_readonly = 0,
                    has_default_value = 0
                FROM \(dbPrefix)INFORMATION_SCHEMA.PARAMETERS AS p
                WHERE p.SPECIFIC_SCHEMA = N'\(escapedSchema)'
                  AND p.SPECIFIC_NAME = N'\(escapedObject)'
                ORDER BY p.ORDINAL_POSITION;
                """

                return self.queryExecutor(infoSchemaSQL).map { infoRows in
                    // Map Information Schema rows into ParameterMetadata. Treat RETURN_VALUE specially.
                    let translated: [ParameterMetadata] = infoRows.compactMap { row in
                        guard
                            let schemaName = row.column("schema_name")?.string,
                            let objectName = row.column("object_name")?.string,
                            let name = row.column("parameter_name")?.string,
                            let ordinal = row.column("parameter_id")?.int
                        else { return nil }

                        let normalizedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
                        let isReturn = normalizedName.caseInsensitiveCompare("@return_value") == .orderedSame ||
                                       normalizedName.caseInsensitiveCompare("RETURN_VALUE") == .orderedSame
                        let typeName = row.column("user_type_name")?.string ?? row.column("system_type_name")?.string ?? ""

                        return ParameterMetadata(
                            schema: schemaName,
                            object: SQLServerMetadataClient.normalizeRoutineName(objectName),
                            name: normalizedName,
                            ordinal: ordinal,
                            isReturnValue: isReturn,
                            typeName: typeName,
                            systemTypeName: row.column("system_type_name")?.string,
                            maxLength: row.column("max_length")?.int,
                            precision: row.column("precision")?.int,
                            scale: row.column("scale")?.int,
                            isOutput: row.column("is_output")?.bool ?? false,
                            hasDefaultValue: false,
                            defaultValue: nil,
                            isReadOnly: row.column("is_readonly")?.bool ?? false
                        )
                    }

                    // By default, exclude implicit return value entries if any
                    return translated.filter { !$0.isReturnValue }
                }
            }
        }
    }

    // MARK: - Key Constraints


public func listPrimaryKeys(
    database: String? = nil,
    schema: String? = nil,
    table: String? = nil
) -> EventLoopFuture<[KeyConstraintMetadata]> {
    if table == nil {
        return listTables(database: database, schema: schema).flatMap { tables in
            let candidates = tables.filter { tableMetadata in
                let normalizedType = tableMetadata.type.uppercased()
                if normalizedType.contains("SYSTEM") { return false }
                if normalizedType.contains("VIEW") { return false }
                return normalizedType.contains("TABLE")
            }

            guard !candidates.isEmpty else {
                return self.connection.eventLoop.makeSucceededFuture([])
            }

            var iterator = candidates.makeIterator()

            func next(accumulated: [KeyConstraintMetadata]) -> EventLoopFuture<[KeyConstraintMetadata]> {
                guard let tableMetadata = iterator.next() else {
                    return self.connection.eventLoop.makeSucceededFuture(accumulated)
                }
                return self.listPrimaryKeysForSingleTable(
                    database: database,
                    schema: tableMetadata.schema,
                    table: tableMetadata.name
                ).flatMap { pk in
                    next(accumulated: accumulated + pk)
                }
            }

            return next(accumulated: [])
        }
    }

    return listPrimaryKeysForSingleTable(database: database, schema: schema, table: table!)
}

private func listPrimaryKeysForSingleTable(
    database: String?,
    schema: String?,
    table: String
) -> EventLoopFuture<[KeyConstraintMetadata]> {
    var parameters: [String] = []

    parameters.append("@table_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'")
    if let schema {
        parameters.append("@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
    }
    if let database {
        parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(database))'")
    }
    if parameters.isEmpty {
        parameters.append("@table_name = N'%'")
    } else if parameters.first(where: { $0.hasPrefix("@table_name") }) == nil {
        parameters.append("@table_name = N'%'")
    }

    // Avoid extra DONE tokens from the stored procedure
    let sql = "SET NOCOUNT ON; EXEC sp_pkeys \(parameters.joined(separator: ", "));"

    return fetchPrimaryKeyClusterInfo(database: database, schema: schema, table: table).flatMap { clusterInfo in
        self.queryExecutor(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, columns: [KeyColumnMetadata])] = [:]

            for row in rows {
                guard
                    let schemaName = row.column("TABLE_OWNER")?.string,
                    let tableName = row.column("TABLE_NAME")?.string,
                    let columnName = row.column("COLUMN_NAME")?.string
                else {
                    continue
                }

                if !self.configuration.includeSystemSchemas,
                   schemaName.caseInsensitiveCompare("sys") == .orderedSame ||
                   schemaName.caseInsensitiveCompare("INFORMATION_SCHEMA") == .orderedSame {
                    continue
                }

                let keyName = row.column("PK_NAME")?.string ?? "PRIMARY"
                let key = "\(schemaName)|\(tableName)|\(keyName)"

                var entry = grouped[key] ?? (
                    schema: schemaName,
                    table: tableName,
                    name: keyName,
                    columns: []
                )

                let ordinal = row.column("KEY_SEQ")?.int ?? (entry.columns.count + 1)
                entry.columns.append(
                    KeyColumnMetadata(
                        column: columnName,
                        ordinal: ordinal,
                        isDescending: false
                    )
                )
                grouped[key] = entry
            }

            return grouped.values.sorted {
                if $0.schema == $1.schema {
                    if $0.table == $1.table {
                        return $0.name < $1.name
                    }
                    return $0.table < $1.table
                }
                return $0.schema < $1.schema
            }.map { entry in
                let key = "\(entry.schema)|\(entry.table)|\(entry.name)"
                let isClustered = clusterInfo[key] ?? false
                return KeyConstraintMetadata(
                    schema: entry.schema,
                    table: entry.table,
                    name: entry.name,
                    type: .primaryKey,
                    isClustered: isClustered,
                    columns: entry.columns.sorted { $0.ordinal < $1.ordinal }
                )
            }
        }
    }
}
    public func listUniqueConstraints(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        fetchKeyConstraints(
            type: .unique,
            database: database,
            schema: schema,
            table: table
        )
    }

    private func fetchKeyConstraints(
        type: KeyConstraintMetadata.ConstraintType,
        database: String?,
        schema: String?,
        table: String?
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        var sql = """
        SELECT
            schema_name = s.name,
            table_name = t.name,
            constraint_name = kc.name,
            is_clustered = CASE WHEN i.type = 1 THEN 1 ELSE 0 END,
            column_name = c.name,
            ordinal = ic.key_ordinal,
            is_descending = ic.is_descending_key
        FROM \(qualified(database, object: "sys.key_constraints")) AS kc
        JOIN \(qualified(database, object: "sys.tables")) AS t ON kc.parent_object_id = t.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON t.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.indexes")) AS i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
        JOIN \(qualified(database, object: "sys.index_columns")) AS ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
        JOIN \(qualified(database, object: "sys.columns")) AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE kc.type = '\(type == .primaryKey ? "PK" : "UQ")'
        """

        var predicates: [String] = []
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }

        if let table {
            predicates.append("t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'")
        }

        if !predicates.isEmpty {
            sql += " AND " + predicates.joined(separator: " AND ")
        }

        sql += " ORDER BY s.name, t.name, kc.name, ic.key_ordinal;"

        return queryExecutor(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, isClustered: Bool, columns: [KeyColumnMetadata])] = [:]

            for row in rows {
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let constraintName = row.column("constraint_name")?.string,
                    let columnName = row.column("column_name")?.string,
                    let ordinal = row.column("ordinal")?.int
                else {
                    continue
                }

                let key = "\(schemaName)|\(tableName)|\(constraintName)"
                var entry = grouped[key] ?? (
                    schema: schemaName,
                    table: tableName,
                    name: constraintName,
                    isClustered: (row.column("is_clustered")?.int ?? 0) != 0,
                    columns: []
                )

                if entry.columns.isEmpty {
                    entry.isClustered = (row.column("is_clustered")?.int ?? 0) != 0
                }

                let column = KeyColumnMetadata(
                    column: columnName,
                    ordinal: ordinal,
                    isDescending: (row.column("is_descending")?.int ?? 0) != 0
                )
                entry.columns.append(column)
                grouped[key] = entry
            }

            return grouped.values.sorted {
                if $0.schema == $1.schema {
                    if $0.table == $1.table {
                        return $0.name < $1.name
                    }
                    return $0.table < $1.table
                }
                return $0.schema < $1.schema
            }.map { entry in
                KeyConstraintMetadata(
                    schema: entry.schema,
                    table: entry.table,
                    name: entry.name,
                    type: type,
                    isClustered: entry.isClustered,
                    columns: entry.columns.sorted { $0.ordinal < $1.ordinal }
                )
            }
        }
    }

    private func fetchPrimaryKeyClusterInfo(
        database: String?,
        schema: String?,
        table: String?
    ) -> EventLoopFuture<[String: Bool]> {
        var sql = """
        SELECT
            schema_name = s.name,
            table_name = t.name,
            constraint_name = kc.name,
            is_clustered = CASE WHEN i.type = 1 THEN 1 ELSE 0 END
        FROM \(qualified(database, object: "sys.key_constraints")) AS kc
        JOIN \(qualified(database, object: "sys.tables")) AS t ON kc.parent_object_id = t.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON t.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.indexes")) AS i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
        WHERE kc.type = 'PK'
        """

        var predicates: [String] = []
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }

        if let table {
            predicates.append("t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'")
        }

        if !predicates.isEmpty {
            sql += " AND " + predicates.joined(separator: " AND ")
        }

        sql += ";"

        return queryExecutor(sql).map { rows in
            var info: [String: Bool] = [:]
            for row in rows {
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let constraintName = row.column("constraint_name")?.string
                else {
                    continue
                }
                let key = "\(schemaName)|\(tableName)|\(constraintName)"
                let isClustered = (row.column("is_clustered")?.int ?? 0) != 0
                info[key] = isClustered
            }
            return info
        }
    }

    // MARK: - Indexes

    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String
    ) -> EventLoopFuture<[IndexMetadata]> {
        // Use sp_statistics stored procedure for simpler token parsing
        let dbQualifier = effectiveDatabase(database) ?? ""
        var parameters = ["@table_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'"]
        parameters.append("@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        if !dbQualifier.isEmpty {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(dbQualifier))'")
        }
        // Reduce token churn by disabling rowcount messages from the stored procedure
        let sql = "SET NOCOUNT ON; EXEC sp_statistics \(parameters.joined(separator: ", "));"

        return queryExecutor(sql).map { rows in
            struct PartialIndex { var schema: String; var table: String; var name: String; var isUnique: Bool; var isClustered: Bool; var filter: String?; var cols: [IndexColumnMetadata] }
            var grouped: [String: PartialIndex] = [:]
            for row in rows {
                guard
                    let schemaName = row.column("TABLE_OWNER")?.string,
                    let tableName = row.column("TABLE_NAME")?.string,
                    let indexName = row.column("INDEX_NAME")?.string,
                    let columnName = row.column("COLUMN_NAME")?.string
                else { continue }
                let key = "\(schemaName)|\(tableName)|\(indexName)"
                var entry = grouped[key] ?? PartialIndex(
                    schema: schemaName,
                    table: tableName,
                    name: indexName,
                    isUnique: (row.column("NON_UNIQUE")?.int ?? 1) == 0,
                    isClustered: false, // sp_statistics doesn't provide cluster info
                    filter: row.column("FILTER_CONDITION")?.string,
                    cols: []
                )
                if entry.cols.isEmpty {
                    entry.isUnique = (row.column("NON_UNIQUE")?.int ?? 1) == 0
                    entry.isClustered = false // sp_statistics doesn't provide cluster info
                    entry.filter = row.column("FILTER_CONDITION")?.string
                }
                let isIncluded = false // sp_statistics doesn't distinguish included columns
                let ord = row.column("SEQ_IN_INDEX")?.int ?? 0
                let isDesc = false // sp_statistics doesn't provide sort direction
                let idxCol = IndexColumnMetadata(column: columnName, ordinal: ord, isDescending: isDesc, isIncluded: isIncluded)
                entry.cols.append(idxCol)
                grouped[key] = entry
            }
            return grouped.values.sorted { $0.name < $1.name }.map { e in
                IndexMetadata(
                    schema: e.schema,
                    table: e.table,
                    name: e.name,
                    isUnique: e.isUnique,
                    isClustered: e.isClustered,
                    isPrimaryKey: false,
                    isUniqueConstraint: false,
                    filterDefinition: e.filter,
                    columns: e.cols
                )
            }
        }
    }

    // MARK: - Foreign keys

    public func listForeignKeys(
        database: String? = nil,
        schema: String,
        table: String
    ) -> EventLoopFuture<[ForeignKeyMetadata]> {
        var parameters: [String] = [
            "@fktable_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'",
            "@fktable_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'"
        ]
        if let database {
            parameters.append("@fktable_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(database))'")
        }

        let sql = "EXEC sp_fkeys \(parameters.joined(separator: ", "));"

        return queryExecutor(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, referencedSchema: String, referencedTable: String, deleteAction: Int, updateAction: Int, columns: [ForeignKeyColumnMetadata])] = [:]

            for row in rows {
                guard
                    let schemaName = row.column("FKTABLE_SCHEM")?.string ?? row.column("FKTABLE_OWNER")?.string,
                    let tableName = row.column("FKTABLE_NAME")?.string,
                    let fkName = row.column("FK_NAME")?.string,
                    let referencedSchema = row.column("PKTABLE_SCHEM")?.string ?? row.column("PKTABLE_OWNER")?.string,
                    let referencedTable = row.column("PKTABLE_NAME")?.string
                else {
                    continue
                }

                let key = "\(schemaName)|\(tableName)|\(fkName)"
                var entry = grouped[key] ?? (
                    schema: schemaName,
                    table: tableName,
                    name: fkName,
                    referencedSchema: referencedSchema,
                    referencedTable: referencedTable,
                    deleteAction: row.column("DELETE_RULE")?.int ?? 1,
                    updateAction: row.column("UPDATE_RULE")?.int ?? 1,
                    columns: []
                )

                if
                    let parentColumn = row.column("FKCOLUMN_NAME")?.string,
                    let referencedColumn = row.column("PKCOLUMN_NAME")?.string,
                    let ordinal = row.column("KEY_SEQ")?.int
                {
                    entry.columns.append(
                        ForeignKeyColumnMetadata(
                            parentColumn: parentColumn,
                            referencedColumn: referencedColumn,
                            ordinal: ordinal
                        )
                    )
                }

                grouped[key] = entry
            }

            return grouped.values.sorted {
                if $0.schema == $1.schema {
                    if $0.table == $1.table {
                        return $0.name < $1.name
                    }
                    return $0.table < $1.table
                }
                return $0.schema < $1.schema
            }.map { entry in
                ForeignKeyMetadata(
                    schema: entry.schema,
                    table: entry.table,
                    name: entry.name,
                    referencedSchema: entry.referencedSchema,
                    referencedTable: entry.referencedTable,
                    deleteAction: ForeignKeyMetadata.mapAction(entry.deleteAction),
                    updateAction: ForeignKeyMetadata.mapAction(entry.updateAction),
                    columns: entry.columns.sorted { $0.ordinal < $1.ordinal }
                )
            }
        }
    }

    // MARK: - Dependencies

    public func listDependencies(
        database: String? = nil,
        schema: String,
        object: String
    ) -> EventLoopFuture<[DependencyMetadata]> {
        let sql = """
        WITH target AS (
            SELECT o.object_id
            FROM \(qualified(database, object: "sys.objects")) AS o
            JOIN \(qualified(database, object: "sys.schemas")) AS s ON o.schema_id = s.schema_id
            WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
              AND o.name = N'\(SQLServerMetadataClient.escapeLiteral(object))'
        )
        SELECT
            referencing_schema = rs.name,
            referencing_object = ro.name,
            referencing_type = ro.type_desc,
            is_schema_bound = sed.is_schema_bound_reference
        FROM target
        JOIN \(qualified(database, object: "sys.sql_expression_dependencies")) AS sed ON sed.referenced_id = target.object_id
        JOIN \(qualified(database, object: "sys.objects")) AS ro ON sed.referencing_id = ro.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS rs ON ro.schema_id = rs.schema_id
        WHERE sed.referenced_minor_id = 0
        ORDER BY rs.name, ro.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("referencing_schema")?.string,
                    let objectName = row.column("referencing_object")?.string,
                    let objectType = row.column("referencing_type")?.string
                else {
                    return nil
                }
                return DependencyMetadata(
                    referencingSchema: schemaName,
                    referencingObject: objectName,
                    referencingType: objectType,
                    isSchemaBound: row.column("is_schema_bound")?.bool ?? false
                )
            }
        }
    }

    // MARK: - Triggers

    public func listTriggers(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[TriggerMetadata]> {
        logger.trace("[Metadata] listTriggers start database=\(database ?? "<default>") schema=\(schema ?? "<all>") table=\(table ?? "<all>") includeComments=\(includeComments)")
        let includeDefs = self.configuration.includeTriggerDefinitions
        let definitionSelect = includeDefs ? "m.definition" : "ISNULL(m.definition, '')"
        var sql = """
        SELECT
            schema_name = s.name,
            table_name = t.name,
            trigger_name = tr.name,
            tr.is_instead_of_trigger,
            tr.is_disabled,
            definition = \(definitionSelect)\(includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : "")
        FROM \(qualified(database, object: "sys.triggers")) AS tr
        JOIN \(qualified(database, object: "sys.tables")) AS t ON tr.parent_id = t.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON t.schema_id = s.schema_id
        \(includeDefs ? "LEFT JOIN \(qualified(database, object: "sys.sql_modules")) AS m ON tr.object_id = m.object_id" : "")
        \(includeComments ? "LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS ep ON ep.class = 1 AND ep.major_id = tr.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'" : "")
        WHERE tr.parent_class = 1
        """

        var predicates: [String] = []
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        if let table {
            predicates.append("t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'")
        }
        if !predicates.isEmpty {
            sql += " AND " + predicates.joined(separator: " AND ")
        }
        sql += " ORDER BY s.name, t.name, tr.name;"

        return queryExecutor(sql).map { rows in
            self.logger.trace("[Metadata] listTriggers completed database=\(database ?? "<default>") schema=\(schema ?? "<all>") table=\(table ?? "<all>") rows=\(rows.count)")
            return rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let triggerName = row.column("trigger_name")?.string
                else {
                    return nil
                }

                return TriggerMetadata(
                    schema: schemaName,
                    table: tableName,
                    name: triggerName,
                    isInsteadOf: row.column("is_instead_of_trigger")?.bool ?? false,
                    isDisabled: row.column("is_disabled")?.bool ?? false,
                    definition: row.column("definition")?.string,
                    comment: row.column("comment")?.string
                )
            }
        }
    }

    // MARK: - Procedures & Functions

    public func fetchObjectDefinitions(
        _ identifiers: [SQLServerMetadataObjectIdentifier]
    ) -> EventLoopFuture<[ObjectDefinition]> {
        guard !identifiers.isEmpty else {
            return connection.eventLoop.makeSucceededFuture([])
        }

        let includeSystemSchemas = self.configuration.includeSystemSchemas

        func needsDefinition(_ type: ObjectDefinition.ObjectType) -> Bool {
            switch type {
            case .table, .procedure, .scalarFunction, .tableFunction, .view, .trigger:
                return true
            default:
                return false
            }
        }

        let groups = Dictionary(grouping: identifiers) { identifier -> String? in
            identifier.database
        }

        let futures = groups.map { (database, items) -> EventLoopFuture<[ObjectDefinition]> in
            let dbPrefix: String = {
                guard let database else { return "" }
                let escaped = SQLServerMetadataClient.escapeIdentifier(database)
                return "[\(escaped)]."
            }()

            var unique: [(schema: String, name: String)] = []
            unique.reserveCapacity(items.count)
            var seen: Set<String> = []
            for identifier in items {
                let key = "\(identifier.schema.lowercased())|\(identifier.name.lowercased())"
                if seen.insert(key).inserted {
                    unique.append((identifier.schema, identifier.name))
                }
            }

            guard !unique.isEmpty else {
                return self.connection.eventLoop.makeSucceededFuture([])
            }

            let initial = self.connection.eventLoop.makeSucceededFuture([ObjectDefinition]())

            return unique.reduce(initial) { partial, target in
                partial.flatMap { collected -> EventLoopFuture<[ObjectDefinition]> in
                    let escapedSchema = SQLServerMetadataClient.escapeLiteral(target.schema)
                    let escapedName = SQLServerMetadataClient.escapeLiteral(target.name)

                    var infoSql = """
                    SELECT
                        schema_name = s.name,
                        object_name = o.name,
                        type_desc = o.type_desc,
                        is_ms_shipped = o.is_ms_shipped,
                        create_date = o.create_date,
                        modify_date = o.modify_date
                    FROM \(dbPrefix)sys.objects AS o
                    JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                    WHERE s.name = N'\(escapedSchema)' AND o.name = N'\(escapedName)'
                    """

                    if !includeSystemSchemas {
                        infoSql += "\n  AND o.is_ms_shipped = 0"
                    }

                    return self.queryExecutor(infoSql).flatMap { rows in
                        guard
                            let row = rows.first,
                            let schemaName = row.column("schema_name")?.string,
                            let objectName = row.column("object_name")?.string,
                            let typeDesc = row.column("type_desc")?.string
                        else {
                            return self.connection.eventLoop.makeSucceededFuture(collected)
                        }

                        let objectType = ObjectDefinition.ObjectType.from(typeDesc: typeDesc)
                        let isSystem = row.column("is_ms_shipped")?.bool ?? false
                        if !includeSystemSchemas && isSystem {
                            return self.connection.eventLoop.makeSucceededFuture(collected)
                        }

                        let definitionFuture: EventLoopFuture<String?>
                        if needsDefinition(objectType) {
                            if objectType == .table {
                                definitionFuture = self.scriptTableDefinition(database: database, schema: schemaName, table: objectName)
                            } else {
                                definitionFuture = self.fetchModuleDefinitionWithPreamble(database: database, schema: schemaName, object: objectName, type: objectType, dbPrefix: dbPrefix)
                            }
                        } else {
                            definitionFuture = self.connection.eventLoop.makeSucceededFuture(nil)
                        }

                        return definitionFuture.map { definition in
                            var next = collected
                            let object = ObjectDefinition(
                                schema: schemaName,
                                name: objectName,
                                type: objectType,
                                definition: definition,
                                isSystemObject: isSystem,
                                createDate: row.column("create_date")?.date,
                                modifyDate: row.column("modify_date")?.date
                            )
                            next.append(object)
                            return next
                        }
                    }
                }
            }
        }

        return EventLoopFuture.reduce([], futures, on: connection.eventLoop) { $0 + $1 }
    }

    public func listProcedures(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[RoutineMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""

        var predicates: [String] = ["1=1"]
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        predicates.append("p.name NOT LIKE 'meta_client_%'")
        let whereClause = predicates.joined(separator: " AND ")

        let definitionSelect = self.configuration.includeRoutineDefinitions ? ", m.definition" : ""
        let commentSelect = includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : ""
        // Only join module definitions for recently modified procedures to avoid heavy I/O on large schemas.
        let joinModules = self.configuration.includeRoutineDefinitions ? "LEFT JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = p.object_id AND p.modify_date >= DATEADD(MINUTE, -5, SYSDATETIME())" : ""
        let joinComments = includeComments ? "LEFT JOIN \(dbPrefix)sys.extended_properties AS ep ON ep.class = 1 AND ep.major_id = p.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT
            schema_name = s.name,
            object_name = p.name\(definitionSelect)\(commentSelect)
        FROM \(dbPrefix)sys.procedures AS p
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = p.schema_id
        \(joinModules)
        \(joinComments)
        WHERE \(whereClause)
        ORDER BY s.name, p.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let schemaName = row.column("schema_name")?.string, let name = row.column("object_name")?.string else { return nil }
                let def = self.configuration.includeRoutineDefinitions ? row.column("definition")?.string : nil
                return RoutineMetadata(schema: schemaName, name: name, type: .procedure, definition: def, isSystemObject: false, comment: row.column("comment")?.string)
            }
        }
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[RoutineMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""

        var predicates: [String] = ["o.type IN ('FN', 'TF', 'IF')"]
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        } else if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        predicates.append("o.name NOT LIKE 'meta_client_%'")
        let whereClause = predicates.joined(separator: " AND ")

        let definitionSelect = self.configuration.includeRoutineDefinitions ? ", m.definition" : ""
        let commentSelect = includeComments ? ", ISNULL(CAST(ep.value AS NVARCHAR(4000)), '') AS comment" : ""
        // Only join module definitions for recently modified functions to avoid heavy I/O on large schemas.
        let joinModules = self.configuration.includeRoutineDefinitions ? "LEFT JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = o.object_id AND o.modify_date >= DATEADD(MINUTE, -5, SYSDATETIME())" : ""
        let joinComments = includeComments ? "LEFT JOIN \(dbPrefix)sys.extended_properties AS ep ON ep.class = 1 AND ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.name = N'MS_Description'" : ""

        let sql = """
        SELECT
            schema_name = s.name,
            object_name = o.name,
            type_desc = o.type_desc,
            is_ms_shipped = o.is_ms_shipped\(definitionSelect)\(commentSelect)
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
        \(joinModules)
        \(joinComments)
        WHERE \(whereClause)
        ORDER BY s.name, o.name;
        """

        return queryExecutor(sql).map { rows in
            rows.compactMap { row -> RoutineMetadata? in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let objectName = row.column("object_name")?.string,
                    let typeDesc = row.column("type_desc")?.string
                else {
                    return nil
                }
                let objectType = ObjectDefinition.ObjectType.from(typeDesc: typeDesc)
                let def = self.configuration.includeRoutineDefinitions ? row.column("definition")?.string : nil
                return RoutineMetadata(
                    schema: schemaName,
                    name: objectName,
                    type: objectType == .tableFunction ? .tableFunction : .scalarFunction,
                    definition: def,
                    isSystemObject: row.column("is_ms_shipped")?.bool ?? false,
                    comment: row.column("comment")?.string
                )
            }
        }
    }
    // MARK: - Search

    public func searchMetadata(
        query: String,
        database: String? = nil,
        schema: String? = nil,
        scopes: MetadataSearchScope = .default
    ) -> EventLoopFuture<[MetadataSearchHit]> {
        guard scopes.rawValue != 0 else {
            return connection.eventLoop.makeSucceededFuture([])
        }

        let escapedPattern = SQLServerMetadataClient.escapeLiteral("%" + query + "%")
        let patternExpression = "N'\(escapedPattern)' COLLATE Latin1_General_CI_AI"
        let escapedSchema = schema.map { SQLServerMetadataClient.escapeLiteral($0) }
        let includeSystemSchemas = self.configuration.includeSystemSchemas

        func schemaPredicate(alias: String) -> String {
            if let escapedSchema {
                return "\(alias).name = N'\(escapedSchema)'"
            }
            if !includeSystemSchemas {
                return "\(alias).name NOT IN ('sys', 'INFORMATION_SCHEMA')"
            }
            return "1=1"
        }

        func combine(_ filters: [String]) -> String {
            let filtered = filters.filter { $0 != "1=1" }
            if filtered.isEmpty {
                return "1=1"
            }
            return filtered.joined(separator: " AND ")
        }

        let metaFilter = "o.name NOT LIKE 'meta_client_%'"
        let resolvedDatabase = effectiveDatabase(database)
        let dbPrefix = resolvedDatabase.map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""

        var selects: [String] = []

        if scopes.contains(.objectNames) {
            let whereClause = combine([
                schemaPredicate(alias: "s"),
                metaFilter,
                "o.name LIKE \(patternExpression)"
            ])
            selects.append(
                """
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'name' AS match_kind, NULL AS detail
                FROM \(dbPrefix)sys.objects AS o
                JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                WHERE \(whereClause)
                """
            )
        }

        if scopes.contains(.definitions) {
            let whereClause = combine([
                schemaPredicate(alias: "s"),
                metaFilter,
                "m.definition COLLATE Latin1_General_CI_AI LIKE \(patternExpression)"
            ])
            selects.append(
                """
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'definition' AS match_kind, m.definition AS detail
                FROM \(dbPrefix)sys.objects AS o
                JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                JOIN \(dbPrefix)sys.sql_modules AS m ON o.object_id = m.object_id
                WHERE \(whereClause)
                """
            )
        }

        if scopes.contains(.columns) {
            let whereClause = combine([
                schemaPredicate(alias: "s"),
                metaFilter,
                "c.name COLLATE Latin1_General_CI_AI LIKE \(patternExpression)"
            ])
            selects.append(
                """
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'column' AS match_kind, c.name AS detail
                FROM \(dbPrefix)sys.objects AS o
                JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                JOIN \(dbPrefix)sys.columns AS c ON o.object_id = c.object_id
                WHERE \(whereClause)
                """
            )
        }

        if scopes.contains(.indexes) {
            let whereClause = combine([
                schemaPredicate(alias: "s"),
                metaFilter,
                "i.name IS NOT NULL",
                "i.name COLLATE Latin1_General_CI_AI LIKE \(patternExpression)"
            ])
            selects.append(
                """
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'index' AS match_kind, i.name AS detail
                FROM \(dbPrefix)sys.indexes AS i
                JOIN \(dbPrefix)sys.objects AS o ON i.object_id = o.object_id
                JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                WHERE \(whereClause)
                """
            )
        }

        if scopes.contains(.constraints) {
            let keyWhere = combine([
                schemaPredicate(alias: "s"),
                metaFilter,
                "kc.name COLLATE Latin1_General_CI_AI LIKE \(patternExpression)"
            ])
            selects.append(
                """
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'constraint' AS match_kind, kc.name AS detail
                FROM \(dbPrefix)sys.key_constraints AS kc
                JOIN \(dbPrefix)sys.objects AS o ON kc.parent_object_id = o.object_id
                JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                WHERE \(keyWhere)
                """
            )

            let fkWhere = combine([
                schemaPredicate(alias: "s"),
                metaFilter,
                "fk.name COLLATE Latin1_General_CI_AI LIKE \(patternExpression)"
            ])
            selects.append(
                """
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc, 'constraint' AS match_kind, fk.name AS detail
                FROM \(dbPrefix)sys.foreign_keys AS fk
                JOIN \(dbPrefix)sys.objects AS o ON fk.parent_object_id = o.object_id
                JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
                WHERE \(fkWhere)
                """
            )
        }

        guard !selects.isEmpty else {
            return connection.eventLoop.makeSucceededFuture([])
        }

        let sql = selects.joined(separator: "\nUNION ALL\n") + "\nORDER BY schema_name, object_name, match_kind;"

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let objectName = row.column("object_name")?.string,
                    let typeDesc = row.column("type_desc")?.string,
                    let matchRaw = row.column("match_kind")?.string
                else {
                    return nil
                }

                guard let match = MetadataSearchHit.MatchKind(rawValue: matchRaw.lowercased()) else {
                    return nil
                }

                return MetadataSearchHit(
                    schema: schemaName,
                    name: objectName,
                    type: ObjectDefinition.ObjectType.from(typeDesc: typeDesc),
                    matchKind: match,
                    detail: row.column("detail")?.string
                )
            }
        }
    }

    // MARK: - Utilities

    private static func escapeIdentifier(_ identifier: String) -> String {
        return identifier.replacingOccurrences(of: "]", with: "]]")
    }

    private static func escapeLiteral(_ literal: String) -> String {
        return literal.replacingOccurrences(of: "'", with: "''")
    }

    private func loadParameterDefaults(
        database: String?,
        schema: String,
        object: String
    ) -> EventLoopFuture<[String: (hasDefault: Bool, defaultValue: String?)]> {
        let candidates: [SQLServerMetadataObjectIdentifier] = [
            .init(database: database, schema: schema, name: object, kind: .procedure),
            .init(database: database, schema: schema, name: object, kind: .function)
        ]

        return fetchObjectDefinitions(candidates).map { definitions in
            guard let definition = definitions.first(where: { $0.definition?.isEmpty == false })?.definition else {
                return [:]
            }
            return SQLServerMetadataClient.extractParameterDefaults(from: definition)
        }
    }

    // Module definition with SSMS-style preamble and view index scripting
    private func fetchModuleDefinitionWithPreamble(
        database: String?,
        schema: String,
        object: String,
        type: ObjectDefinition.ObjectType,
        dbPrefix: String
    ) -> EventLoopFuture<String?> {
        let defSql = """
        SELECT m.definition
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = o.object_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))' AND o.name = N'\(SQLServerMetadataClient.escapeLiteral(object))';
        """ 
        let preambleSql = """
        SELECT m.uses_ansi_nulls, m.uses_quoted_identifier
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        LEFT JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = o.object_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))' AND o.name = N'\(SQLServerMetadataClient.escapeLiteral(object))';
        """

        let defF = queryExecutor(defSql).map { rows -> String? in
            guard let text = rows.first?.column("definition")?.string, !text.isEmpty else { return nil }
            return text.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        let preF = queryExecutor(preambleSql).map { rows -> String in
            guard let row = rows.first else { return "" }
            let ansi = (row.column("uses_ansi_nulls")?.int ?? 1) != 0
            let qi = (row.column("uses_quoted_identifier")?.int ?? 1) != 0
            let text = "SET ANSI_NULLS \(ansi ? "ON" : "OFF")\nGO\nSET QUOTED_IDENTIFIER \(qi ? "ON" : "OFF")\nGO\n"
            return text
        }
        return defF.and(preF).flatMap { (bodyOpt, preamble) in
            guard let body = bodyOpt else { return self.connection.eventLoop.makeSucceededFuture(nil) }

            if type == .view {
                // Append view indexes if any
                return self.fetchObjectIndexDetails(database: database, schema: schema, object: object).map { ixs in
                    var script = preamble + body
                    let constraintNames = Set<String>()
                    for ix in ixs where !constraintNames.contains(ix.name) {
                        if ix.indexType == 5 || ix.indexType == 6 {
                            let csKind = ix.indexType == 5 ? "CLUSTERED COLUMNSTORE" : "NONCLUSTERED COLUMNSTORE"
                            var stmt = "\n\nCREATE \(csKind) INDEX [\(SQLServerMetadataClient.escapeIdentifier(ix.name))] ON [\(SQLServerMetadataClient.escapeIdentifier(schema))].[\(SQLServerMetadataClient.escapeIdentifier(object))]"
                            if ix.indexType == 6 {
                                let cols = ix.columns.sorted { $0.ordinal < $1.ordinal }.map { "[\(SQLServerMetadataClient.escapeIdentifier($0.column))]" }.joined(separator: ", ")
                                if !cols.isEmpty { stmt += " (\(cols))" }
                            }
                            if let opts = ix.optionClause, !opts.isEmpty { stmt += " WITH (\(opts))" }
                            if let storage = ix.storageClause, !storage.isEmpty { stmt += " \(storage)" }
                            stmt += ";"
                            script += stmt
                            continue
                        }
                        let kind = ix.isClustered ? "CLUSTERED" : "NONCLUSTERED"
                        let uniq = ix.isUnique ? "UNIQUE " : ""
                        let cols = ix.columns.filter { !$0.isIncluded }.sorted { $0.ordinal < $1.ordinal }
                            .map { "[\(SQLServerMetadataClient.escapeIdentifier($0.column))] \($0.isDescending ? "DESC" : "ASC")" }
                            .joined(separator: ", ")
                        var stmt = "\n\nCREATE \(uniq)\(kind) INDEX [\(SQLServerMetadataClient.escapeIdentifier(ix.name))] ON [\(SQLServerMetadataClient.escapeIdentifier(schema))].[\(SQLServerMetadataClient.escapeIdentifier(object))] (\(cols))"
                        let includedCols = ix.columns.filter { $0.isIncluded }.map { "[\(SQLServerMetadataClient.escapeIdentifier($0.column))]" }
                        if !includedCols.isEmpty { stmt += " INCLUDE (\(includedCols.joined(separator: ", ")))" }
                        if let filter = ix.filterDefinition, !filter.isEmpty { stmt += " WHERE \(filter)" }
                        if let opts = ix.optionClause, !opts.isEmpty { stmt += " WITH (\(opts))" }
                        if let storage = ix.storageClause, !storage.isEmpty { stmt += " \(storage)" }
                        stmt += ";"
                        script += stmt
                    }
                    return script
                }
            } else {
                return self.connection.eventLoop.makeSucceededFuture(preamble + body)
            }
        }
    }

    private static func extractParameterDefaults(from definition: String) -> [String: (hasDefault: Bool, defaultValue: String?)] {
        // We support both CREATE FUNCTION and CREATE PROCEDURE forms.
        // - FUNCTION: parameter list is enclosed in parentheses after the object name
        // - PROCEDURE: parameters appear after the object name up to the AS keyword (no enclosing parens)
        let text = definition
        let lower = text.lowercased()

        func indexAfterObjectName(startOfKeyword kw: String) -> String.Index? {
            guard let kRange = lower.range(of: kw) else { return nil }
            // Advance past the keyword token
            var i = kRange.upperBound
            // Skip whitespace
            while i < lower.endIndex, lower[i].isWhitespace { i = lower.index(after: i) }
            // Skip optional schema/object with brackets or quoted identifiers, stopping before parameters or AS/RETURNS
            // Consume until we hit '@', '(', 'a' (for 'as'), or 'r' (for 'returns') at top level
            // We’ll return the index at this point to begin parameter scanning
            return i
        }

        // Split a span into top-level comma-separated segments respecting (), [], and quotes
        func splitTopLevel(byComma span: Substring) -> [String] {
            var parts: [String] = []
            var current = ""
            var depth = 0
            var inSingle = false
            var inBracket = false
            var prev: Character = "\u{0}"
            for ch in span {
                if inSingle {
                    current.append(ch)
                    if ch == "'" && prev != "'" { inSingle = false }
                } else if inBracket {
                    current.append(ch)
                    if ch == "]" { inBracket = false }
                } else {
                    switch ch {
                    case "'": inSingle = true; current.append(ch)
                    case "[": inBracket = true; current.append(ch)
                    case "(": depth += 1; current.append(ch)
                    case ")": depth = max(0, depth - 1); current.append(ch)
                    case ",":
                        if depth == 0 {
                            parts.append(current.trimmingCharacters(in: .whitespacesAndNewlines))
                            current.removeAll(keepingCapacity: true)
                        } else { current.append(ch) }
                    default:
                        current.append(ch)
                    }
                }
                prev = ch
            }
            if !current.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                parts.append(current.trimmingCharacters(in: .whitespacesAndNewlines))
            }
            return parts
        }

        var paramBlock: Substring = ""
        if let fRange = lower.range(of: "create function") {
            // Find the first '(' after the function name (outside quotes/brackets)
            var i = fRange.upperBound
            var inSingle = false, inBracket = false
            while i < lower.endIndex {
                let ch = lower[i]
                if inSingle { if ch == "'" { inSingle = false } }
                else if inBracket { if ch == "]" { inBracket = false } }
                else {
                    if ch == "'" { inSingle = true }
                    else if ch == "[" { inBracket = true }
                    else if ch == "(" { break }
                }
                i = lower.index(after: i)
            }
            guard i < lower.endIndex else { return [:] }
            // Extract until the matching ')'
            var depth = 0
            var j = i
            repeat {
                let ch = lower[j]
                if ch == "(" { depth += 1 }
                else if ch == ")" { depth -= 1 }
                j = lower.index(after: j)
            } while j <= lower.endIndex && depth > 0
            paramBlock = text[lower.index(after: i)..<lower.index(before: j)] // exclude both '(' and ')'
        } else if let pRange = lower.range(of: "create procedure") ?? lower.range(of: "create proc") {
            // Parameters run from after the name up to the AS keyword (not within quotes)
            var i = pRange.upperBound
            // Find first '@' after the object name; if not found, assume no parameters
            while i < lower.endIndex, lower[i] != "@" && lower[i] != "a" { i = lower.index(after: i) }
            guard i < lower.endIndex else { return [:] }
            // Find AS boundary not in quotes/brackets/paren
            var inSingle = false, inBracket = false, depth = 0
            var j = i
            while j < lower.endIndex {
                let ch = lower[j]
                if inSingle { if ch == "'" { inSingle = false } }
                else if inBracket { if ch == "]" { inBracket = false } }
                else {
                    if ch == "'" { inSingle = true }
                    else if ch == "[" { inBracket = true }
                    else if ch == "(" { depth += 1 }
                    else if ch == ")" { depth = max(0, depth - 1) }
                    else if depth == 0 {
                        // Check for AS keyword
                        if j <= lower.index(lower.endIndex, offsetBy: -2) {
                            let ahead = lower[j...]
                            if ahead.hasPrefix("as ") || ahead.hasPrefix("as\n") || ahead.hasPrefix("as\r") || ahead.hasPrefix("as\t") {
                                break
                            }
                        }
                    }
                }
                j = lower.index(after: j)
            }
            paramBlock = text[i..<j]
        } else {
            return [:]
        }

        let segments = splitTopLevel(byComma: paramBlock)
        var defaults: [String: (hasDefault: Bool, defaultValue: String?)] = [:]
        defaults.reserveCapacity(segments.count)

        for segmentRaw in segments {
            let segment = segmentRaw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let at = segment.firstIndex(of: "@") else { continue }
            let nameStart = at
            var nameEnd = segment.index(after: nameStart)
            while nameEnd < segment.endIndex {
                let c = segment[nameEnd]
                if c == " " || c == "\t" || c == "\n" || c == "=" || c == "," { break }
                nameEnd = segment.index(after: nameEnd)
            }
            let key = segment[nameStart..<nameEnd].lowercased()
            guard let eq = segment.firstIndex(of: "=") else {
                defaults[String(key)] = (hasDefault: false, defaultValue: nil)
                continue
            }
            var defaultPart = segment[segment.index(after: eq)...].trimmingCharacters(in: .whitespacesAndNewlines)
            // Strip trailing mode keywords
            while let range = defaultPart.range(of: "[A-Za-z_]+$", options: .regularExpression) {
                let kw = defaultPart[range].lowercased()
                if kw == "output" || kw == "out" || kw == "readonly" {
                    defaultPart = defaultPart[..<range.lowerBound].trimmingCharacters(in: .whitespacesAndNewlines)
                } else { break }
            }
            let cleaned = defaultPart.isEmpty ? nil : String(defaultPart)
            defaults[String(key)] = (hasDefault: cleaned != nil, defaultValue: cleaned)
        }

        return defaults
    }

    internal static func normalizeRoutineName(_ raw: String) -> String {
        let stripped = raw.split(separator: ";", maxSplits: 1, omittingEmptySubsequences: false).first.map(String.init) ?? raw
        if let dotIndex = stripped.firstIndex(of: ".") {
            return String(stripped[stripped.index(after: dotIndex)...])
        }
        return stripped
    }

    private func qualified(_ database: String?, object: String) -> String {
        if let resolved = effectiveDatabase(database) {
            return "[\(SQLServerMetadataClient.escapeIdentifier(resolved))].\(object)"
        } else {
            return object
        }
    }
}
