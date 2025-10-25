import Foundation
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
    public struct Configuration: Sendable {
        public var includeSystemSchemas: Bool
        public var enableColumnCache: Bool
        public var includeRoutineDefinitions: Bool

        public init(
            includeSystemSchemas: Bool = false,
            enableColumnCache: Bool = true,
            includeRoutineDefinitions: Bool = false
        ) {
            self.includeSystemSchemas = includeSystemSchemas
            self.enableColumnCache = enableColumnCache
            self.includeRoutineDefinitions = includeRoutineDefinitions
        }
    }

    private let connection: TDSConnection
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
            connection: connection.underlying,
            configuration: configuration,
            sharedCache: nil,
            defaultDatabase: connection.currentDatabase,
            queryExecutor: executor
        )
    }

    @available(*, deprecated, message: "Pass SQLServerConnection instead")
    public convenience init(
        connection: TDSConnection,
        configuration: Configuration = Configuration()
    ) {
        self.init(connection: connection, configuration: configuration, sharedCache: nil, defaultDatabase: nil)
    }

    internal init(
        connection: TDSConnection,
        configuration: Configuration,
        sharedCache: MetadataCache<[ColumnMetadata]>?,
        defaultDatabase: String?,
        queryExecutor: (@Sendable (String) -> EventLoopFuture<[TDSRow]>)? = nil
    ) {
        self.connection = connection
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
                connection.rawSql(sql)
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
                parts.append(col.isNullable ? "NULL" : "NOT NULL")
                if let def = col.defaultDefinition, !def.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    if let defName = col.defaultName {
                        parts.append("CONSTRAINT \(ident(defName)) DEFAULT \(def)")
                    } else {
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
                var constraintLine = "    CONSTRAINT \(ident(pk.name)) PRIMARY KEY \(clustered) (\(clause))"
                if let idx = ixs.first(where: { $0.name.caseInsensitiveCompare(pk.name) == .orderedSame }) {
                    if let opts = idx.optionClause, !opts.isEmpty { constraintLine += " WITH (\(opts))" }
                    if let storage = idx.storageClause, !storage.isEmpty { constraintLine += " \(storage)" }
                }
                lines.append(constraintLine)
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

            let header = "CREATE TABLE \(qualified(schema, table)) (\n"
            let body = lines.joined(separator: ",\n")
            var script = header + body + "\n)"

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
                if let filter = ix.filterDefinition, !filter.isEmpty { ixStmt += " WHERE \(filter)" }
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
        var computedDefinition: String?
        var isComputedPersisted: Bool
        var defaultName: String?
        var defaultDefinition: String?
        var collationName: String?
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
            t.name AS table_name,
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
        ORDER BY s.name, t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
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

    public func listTables(database: String? = nil, schema: String? = nil) -> EventLoopFuture<[TableMetadata]> {
        let qualifiedObjects = qualified(database, object: "sys.objects")
        let qualifiedSchemas = qualified(database, object: "sys.schemas")

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
            o.is_ms_shipped
        FROM \(qualifiedObjects) AS o
        INNER JOIN \(qualifiedSchemas) AS s
            ON s.schema_id = o.schema_id
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
                return TableMetadata(
                    schema: schemaName,
                    name: tableName,
                    type: normalizedType,
                    isSystemObject: isSystemObject
                )
            }
        }
    }

    // MARK: - Columns

    public func listColumns(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[ColumnMetadata]> {
        let cacheKey = "\(effectiveDatabase(database) ?? "").\(schema).\(table)"
        if let cache, let cached = cache.value(forKey: cacheKey) {
            return connection.eventLoop.makeSucceededFuture(cached)
        }

        var parameters: [String] = [
            "@table_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'",
            "@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'",
            "@ODBCVer = 3"
        ]
        if let qualifier = effectiveDatabase(database) {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(qualifier))'")
        }
        let sql = "EXEC sp_columns_100 \(parameters.joined(separator: ", "));"

        return queryExecutor(sql).map { rows in
            let columns: [ColumnMetadata] = rows.compactMap { row -> ColumnMetadata? in
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
                let collationName: String? = nil
                let defaultDefinition = row.column("COLUMN_DEF")?.string
                let computedDefinition: String? = nil
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
                    collationName: collationName,
                    isNullable: isNullable,
                    isIdentity: isIdentity,
                    isComputed: isComputed,
                    hasDefaultValue: hasDefaultValue,
                    defaultDefinition: defaultDefinition,
                    computedDefinition: computedDefinition,
                    ordinalPosition: ordinal
                )
            }

            if !columns.isEmpty {
                if let cache = self.cache {
                    cache.setValue(columns, forKey: cacheKey)
                }
            }

            return columns
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
            parameter_id = p.parameter_id,
            parameter_name = p.name,
            type_name = TYPE_NAME(p.user_type_id),
            system_type_name = TYPE_NAME(p.system_type_id),
            max_length = p.max_length,
            precision = p.precision,
            scale = p.scale,
            is_output = p.is_output,
            is_readonly = ISNULL(p.is_readonly, 0),
            has_default_value = p.has_default_value
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        LEFT JOIN \(dbPrefix)sys.parameters AS p ON p.object_id = o.object_id
        WHERE s.name = N'\(escapedSchema)'
          AND o.name = N'\(escapedObject)'
          AND o.type IN ('P','PC','RF','AF','FN','TF','IF')
          AND p.parameter_id IS NOT NULL
        ORDER BY p.parameter_id;
        """

        return queryExecutor(sql).flatMap { rows in
            let defaultsFuture: EventLoopFuture<[String: (hasDefault: Bool, defaultValue: String?) ]>
            if rows.isEmpty {
                defaultsFuture = self.connection.eventLoop.makeSucceededFuture([:])
            } else {
                defaultsFuture = self.loadParameterDefaults(database: database, schema: schema, object: object)
            }

            return defaultsFuture.map { defaults in
                rows.compactMap { row -> ParameterMetadata? in
                    guard
                        let schemaName = row.column("schema_name")?.string,
                        let rawObjectName = row.column("object_name")?.string,
                        let name = row.column("parameter_name")?.string,
                        let ordinal = row.column("parameter_id")?.int,
                        let typeName = row.column("type_name")?.string
                    else {
                        return nil
                    }

                    let objectName = SQLServerMetadataClient.normalizeRoutineName(rawObjectName)
                    let systemType = row.column("system_type_name")?.string
                    let maxLength = row.column("max_length")?.int
                    let precision = row.column("precision")?.int
                    let scale = row.column("scale")?.int
                    let normalizedName = name.lowercased()
                    let override = defaults[normalizedName]
                    let resolvedDefault = override?.defaultValue
                    let hasDefault = override?.hasDefault ?? (row.column("has_default_value")?.bool ?? false)
                    let isOutput = row.column("is_output")?.bool ?? false
                    let isReturnValue = ordinal == 0 || normalizedName == "@return_value"

                    return ParameterMetadata(
                        schema: schemaName,
                        object: objectName,
                        name: name,
                        ordinal: ordinal,
                        isReturnValue: isReturnValue,
                        typeName: typeName,
                        systemTypeName: systemType,
                        maxLength: maxLength,
                        precision: precision,
                        scale: scale,
                        isOutput: isOutput,
                        hasDefaultValue: hasDefault,
                        defaultValue: resolvedDefault,
                        isReadOnly: row.column("is_readonly")?.bool ?? false
                    )
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
    let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
    var predicates: [String] = ["kc.type = 'PK'"]
    if let schema {
        predicates.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
    } else if !configuration.includeSystemSchemas {
        predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
    }
    if let table {
        predicates.append("t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'")
    }
    let whereClause = predicates.joined(separator: " AND ")

    let sql = """
    SELECT
        schema_name = s.name,
        table_name = t.name,
        constraint_name = kc.name,
        is_clustered = CASE WHEN i.type = 1 THEN 1 ELSE 0 END,
        column_name = c.name,
        key_ordinal = ic.key_ordinal,
        is_descending = ic.is_descending_key
    FROM \(dbPrefix)sys.key_constraints AS kc
    JOIN \(dbPrefix)sys.tables AS t ON t.object_id = kc.parent_object_id
    JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
    JOIN \(dbPrefix)sys.indexes AS i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
    JOIN \(dbPrefix)sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN \(dbPrefix)sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE \(whereClause)
    ORDER BY s.name, t.name, kc.name, ic.key_ordinal;
    """

    return queryExecutor(sql).map { rows in
        var grouped: [String: (schema: String, table: String, name: String, isClustered: Bool, columns: [KeyColumnMetadata])] = [:]

        for row in rows {
            guard
                let schemaName = row.column("schema_name")?.string,
                let tableName = row.column("table_name")?.string,
                let constraintName = row.column("constraint_name")?.string,
                let columnName = row.column("column_name")?.string,
                let ordinal = row.column("key_ordinal")?.int
            else {
                continue
            }

            let key = "\(schemaName)|\(tableName)|\(constraintName)"
            var entry = grouped[key] ?? (
                schema: schemaName,
                table: tableName,
                name: constraintName,
                isClustered: row.column("is_clustered")?.bool ?? false,
                columns: []
            )

            entry.columns.append(
                KeyColumnMetadata(
                    column: columnName,
                    ordinal: ordinal,
                    isDescending: row.column("is_descending")?.bool ?? false
                )
            )
            grouped[key] = entry
        }

        return grouped.values.map { entry in
            KeyConstraintMetadata(
                schema: entry.schema,
                table: entry.table,
                name: entry.name,
                type: .primaryKey,
                isClustered: entry.isClustered,
                columns: entry.columns.sorted { $0.ordinal < $1.ordinal }
            )
        }.sorted { lhs, rhs in
            if lhs.schema == rhs.schema {
                if lhs.table == rhs.table {
                    return lhs.name < rhs.name
                }
                return lhs.table < rhs.table
            }
            return lhs.schema < rhs.schema
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
        // Use catalog views to capture included columns and filters, which sp_statistics lacks
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataClient.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            i.name AS index_name,
            i.is_unique,
            i.type AS index_type,
            i.filter_definition,
            ic.key_ordinal,
            ic.is_descending_key,
            ic.is_included_column,
            c.name AS column_name
        FROM \(dbPrefix)sys.indexes AS i
        JOIN \(dbPrefix)sys.tables AS t ON i.object_id = t.object_id
        JOIN \(dbPrefix)sys.schemas AS s ON t.schema_id = s.schema_id
        JOIN \(dbPrefix)sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN \(dbPrefix)sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'
          AND i.index_id > 0
          AND i.is_hypothetical = 0
        ORDER BY s.name, t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
        """

        return queryExecutor(sql).map { rows in
            struct PartialIndex { var schema: String; var table: String; var name: String; var isUnique: Bool; var isClustered: Bool; var filter: String?; var cols: [IndexColumnMetadata] }
            var grouped: [String: PartialIndex] = [:]
            for row in rows {
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let indexName = row.column("index_name")?.string,
                    let columnName = row.column("column_name")?.string
                else { continue }
                let key = "\(schemaName)|\(tableName)|\(indexName)"
                var entry = grouped[key] ?? PartialIndex(
                    schema: schemaName,
                    table: tableName,
                    name: indexName,
                    isUnique: (row.column("is_unique")?.int ?? 0) != 0,
                    isClustered: (row.column("index_type")?.int ?? 0) == 1,
                    filter: row.column("filter_definition")?.string,
                    cols: []
                )
                if entry.cols.isEmpty {
                    entry.isUnique = (row.column("is_unique")?.int ?? 0) != 0
                    entry.isClustered = (row.column("index_type")?.int ?? 0) == 1
                    entry.filter = row.column("filter_definition")?.string
                }
                let isIncluded = (row.column("is_included_column")?.int ?? 0) != 0
                let ord = row.column("key_ordinal")?.int ?? 0
                let isDesc = (row.column("is_descending_key")?.int ?? 0) != 0
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
        table: String? = nil
    ) -> EventLoopFuture<[TriggerMetadata]> {
        var sql = """
        SELECT
            schema_name = s.name,
            table_name = t.name,
            trigger_name = tr.name,
            tr.is_instead_of_trigger,
            tr.is_disabled,
            definition = m.definition
        FROM \(qualified(database, object: "sys.triggers")) AS tr
        JOIN \(qualified(database, object: "sys.tables")) AS t ON tr.parent_id = t.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON t.schema_id = s.schema_id
        LEFT JOIN \(qualified(database, object: "sys.sql_modules")) AS m ON tr.object_id = m.object_id
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
            rows.compactMap { row in
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
                    definition: row.column("definition")?.string
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
        schema: String? = nil
    ) -> EventLoopFuture<[RoutineMetadata]> {
        var parameters: [String] = []
        if let qualifier = effectiveDatabase(database) {
            parameters.append("@sp_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(qualifier))'")
        }
        if let schema {
            parameters.append("@sp_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        }
        let sql = "EXEC sp_stored_procedures\(parameters.isEmpty ? "" : " " + parameters.joined(separator: ", "));"

        return queryExecutor(sql).flatMap { rows in
            let procedures: [RoutineMetadata] = rows.compactMap { row -> RoutineMetadata? in
                guard
                    let schemaName = row.column("PROCEDURE_OWNER")?.string,
                    let rawName = row.column("PROCEDURE_NAME")?.string
                else {
                    return nil
                }

                if !self.configuration.includeSystemSchemas,
                   schemaName.caseInsensitiveCompare("sys") == .orderedSame ||
                   schemaName.caseInsensitiveCompare("INFORMATION_SCHEMA") == .orderedSame {
                    return nil
                }

                let name = SQLServerMetadataClient.normalizeRoutineName(rawName)

                if name.hasPrefix("meta_client_") {
                    return nil
                }

                if let schema,
                   schemaName.caseInsensitiveCompare(schema) != .orderedSame {
                    return nil
                }

                return RoutineMetadata(
                    schema: schemaName,
                    name: name,
                    type: .procedure,
                    definition: nil,
                    isSystemObject: false
                )
            }

            guard self.configuration.includeRoutineDefinitions, !procedures.isEmpty else {
                return self.connection.eventLoop.makeSucceededFuture(procedures)
            }

            let db = self.effectiveDatabase(database)
            let identifiers = procedures.map { routine in
                SQLServerMetadataObjectIdentifier(
                    database: db,
                    schema: routine.schema,
                    name: routine.name,
                    kind: .procedure
                )
            }

            return self.fetchObjectDefinitions(identifiers).map { definitions in
                let lookup = Dictionary(uniqueKeysWithValues: definitions.map { definition in
                    let key = "\(definition.schema.lowercased())|\(definition.name.lowercased())"
                    return (key, definition)
                })

                return procedures.map { procedure in
                    let key = "\(procedure.schema.lowercased())|\(procedure.name.lowercased())"
                    guard let definition = lookup[key] else { return procedure }
                    return RoutineMetadata(
                        schema: procedure.schema,
                        name: procedure.name,
                        type: .procedure,
                        definition: definition.definition,
                        isSystemObject: definition.isSystemObject
                    )
                }
            }
        }
    }

    public func listFunctions(
        database: String? = nil,
        schema: String? = nil
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

        let sql = """
        SELECT
            schema_name = s.name,
            object_name = o.name,
            type_desc = o.type_desc,
            is_ms_shipped = o.is_ms_shipped
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
        WHERE \(whereClause)
        ORDER BY s.name, o.name;
        """

        return queryExecutor(sql).flatMap { rows in
            let functions: [RoutineMetadata] = rows.compactMap { row -> RoutineMetadata? in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let objectName = row.column("object_name")?.string,
                    let typeDesc = row.column("type_desc")?.string
                else {
                    return nil
                }

                let objectType = ObjectDefinition.ObjectType.from(typeDesc: typeDesc)
                return RoutineMetadata(
                    schema: schemaName,
                    name: objectName,
                    type: objectType == .tableFunction ? .tableFunction : .scalarFunction,
                    definition: nil,
                    isSystemObject: row.column("is_ms_shipped")?.bool ?? false
                )
            }

            guard !functions.isEmpty else {
                return self.connection.eventLoop.makeSucceededFuture([])
            }

            guard self.configuration.includeRoutineDefinitions else {
                return self.connection.eventLoop.makeSucceededFuture(functions)
            }

            let identifiers = functions.map { function in
                SQLServerMetadataObjectIdentifier(
                    database: resolvedDatabase,
                    schema: function.schema,
                    name: function.name,
                    kind: .function
                )
            }

            return self.fetchObjectDefinitions(identifiers).map { definitions in
                let lookup = Dictionary(uniqueKeysWithValues: definitions.map { definition in
                    let key = "\(definition.schema.lowercased())|\(definition.name.lowercased())"
                    return (key, definition)
                })

                return functions.map { function in
                    let key = "\(function.schema.lowercased())|\(function.name.lowercased())"
                    guard let definition = lookup[key] else { return function }

                    let resolvedType: RoutineMetadata.RoutineType
                    switch definition.type {
                    case .tableFunction:
                        resolvedType = .tableFunction
                    case .scalarFunction:
                        resolvedType = .scalarFunction
                    default:
                        resolvedType = function.type
                    }

                    return RoutineMetadata(
                        schema: function.schema,
                        name: function.name,
                        type: resolvedType,
                        definition: definition.definition,
                        isSystemObject: definition.isSystemObject
                    )
                }
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
        let literal = SQLServerMetadataClient.escapeLiteral("[\(schema)].[\(object)]")
        let defSql = "EXEC \(dbPrefix)sys.sp_helptext @objname = N'\(literal)';"
        let preambleSql = """
        SELECT m.uses_ansi_nulls, m.uses_quoted_identifier
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        LEFT JOIN \(dbPrefix)sys.sql_modules AS m ON m.object_id = o.object_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))' AND o.name = N'\(SQLServerMetadataClient.escapeLiteral(object))';
        """

        let defF = queryExecutor(defSql).map { rows -> String? in
            let segments = rows.compactMap { $0.column("Text")?.string }
            guard !segments.isEmpty else { return nil }
            return segments.joined().trimmingCharacters(in: .whitespacesAndNewlines)
        }
        let preF = queryExecutor(preambleSql).map { rows -> String in
            guard let row = rows.first else { return "" }
            let ansi = (row.column("uses_ansi_nulls")?.int ?? 1) != 0
            let qi = (row.column("uses_quoted_identifier")?.int ?? 1) != 0
            var text = "SET ANSI_NULLS \(ansi ? "ON" : "OFF")\nGO\nSET QUOTED_IDENTIFIER \(qi ? "ON" : "OFF")\nGO\n"
            return text
        }
        return defF.and(preF).flatMap { (bodyOpt, preamble) in
            guard var body = bodyOpt else { return self.connection.eventLoop.makeSucceededFuture(nil) }

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
        guard let openIndex = definition.firstIndex(of: "(") else { return [:] }

        var index = definition.index(after: openIndex)
        var level = 1
        var current = ""
        var segments: [String] = []

        while index < definition.endIndex, level > 0 {
            let character = definition[index]
            if character == "(" {
                level += 1
                current.append(character)
            } else if character == ")" {
                level -= 1
                if level == 0 {
                    segments.append(current)
                    break
                } else {
                    current.append(character)
                }
            } else if character == "," && level == 1 {
                segments.append(current)
                current.removeAll(keepingCapacity: true)
            } else {
                current.append(character)
            }
            index = definition.index(after: index)
        }

        var defaults: [String: (hasDefault: Bool, defaultValue: String?)] = [:]
        defaults.reserveCapacity(segments.count)

        for rawSegment in segments {
            let segment = rawSegment.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let atIndex = segment.firstIndex(of: "@") else { continue }
            let nameStart = atIndex
            var nameEnd = segment.index(after: nameStart)
            while nameEnd < segment.endIndex {
                let scalar = segment[nameEnd]
                if scalar == " " || scalar == "\t" || scalar == "\n" || scalar == "=" || scalar == "," {
                    break
                }
                nameEnd = segment.index(after: nameEnd)
            }
            let name = String(segment[nameStart..<nameEnd])
            let key = name.lowercased()

            guard let equalsIndex = segment.firstIndex(of: "=") else {
                defaults[key] = (hasDefault: false, defaultValue: nil)
                continue
            }

            var defaultPart = segment[segment.index(after: equalsIndex)...].trimmingCharacters(in: .whitespacesAndNewlines)

            while let range = defaultPart.range(of: "[A-Za-z_]+$", options: .regularExpression) {
                let keyword = defaultPart[range].lowercased()
                if keyword == "output" || keyword == "out" || keyword == "readonly" {
                    defaultPart = defaultPart[..<range.lowerBound].trimmingCharacters(in: .whitespacesAndNewlines)
                } else {
                    break
                }
            }

            let cleaned = defaultPart.isEmpty ? nil : String(defaultPart)
            defaults[key] = (hasDefault: cleaned != nil, defaultValue: cleaned)
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
