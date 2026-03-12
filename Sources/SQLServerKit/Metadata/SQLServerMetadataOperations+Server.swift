import Foundation
import NIO
import SQLServerTDS

extension SQLServerMetadataOperations {
    // MARK: - Server Info

    public func serverVersion() -> EventLoopFuture<String> {
        let sql = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS version;"
        return queryExecutor(sql).flatMapThrowing { rows in
            if let value = rows.first?.column("version")?.string, !value.isEmpty {
                return value
            }
            throw SQLServerError.sqlExecutionError(message: "Failed to fetch server version")
        }
    }

    public func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        queryExecutor("SELECT name, state_desc FROM sys.databases WITH (NOLOCK) ORDER BY name").map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return DatabaseMetadata(name: name, stateDescription: row.column("state_desc")?.string)
            }
        }
    }

    @available(macOS 12.0, *)
    public func databaseState(name: String) async throws -> DatabaseMetadata {
        let sql = "SELECT name, state_desc FROM sys.databases WITH (NOLOCK) WHERE name = N'\(Self.escapeLiteral(name))'"
        let rows = try await queryExecutor(sql).get()
        guard let row = rows.first, let dbName = row.column("name")?.string else {
            throw SQLServerError.databaseDoesNotExist(name)
        }
        return DatabaseMetadata(name: dbName, stateDescription: row.column("state_desc")?.string)
    }

    internal func updateDefaultDatabase(_ database: String) {
        defaultDatabaseLock.withLock {
            defaultDatabase = database
        }
    }

    // MARK: - Database Structure

    public func loadSchemaStructure(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false
    ) -> EventLoopFuture<SQLServerSchemaStructure> {
        let resolvedDatabase = effectiveDatabase(database)
        return listTables(database: resolvedDatabase, schema: schema, includeComments: includeComments).flatMap { tables in
            let tableCandidates = tables.filter { $0.kind == .table || $0.kind == .view }
            let columnsF = self.listColumnsForSchema(database: resolvedDatabase, schema: schema, includeComments: includeComments)
            let pkF = self.listPrimaryKeysFromCatalog(database: resolvedDatabase, schema: schema, table: nil)
            let funcF = self.listFunctions(database: resolvedDatabase, schema: schema, includeComments: includeComments)
            let procF = self.listProcedures(database: resolvedDatabase, schema: schema, includeComments: includeComments)
            let trigF = self.listTriggers(database: resolvedDatabase, schema: schema, table: nil, includeComments: includeComments)

            return columnsF.and(pkF).and(funcF).and(procF).and(trigF).map { data in
                let ((((columns, primaryKeys), functions), procedures), triggers) = data
                var columnsByTable: [String: [ColumnMetadata]] = [:]
                for col in columns { columnsByTable["\(col.schema.lowercased())|\(col.table.lowercased())", default: []].append(col) }
                var pkByTable: [String: KeyConstraintMetadata] = [:]
                for pk in primaryKeys { pkByTable["\(pk.schema.lowercased())|\(pk.table.lowercased())"] = pk }

                var tableStructures: [SQLServerTableStructure] = []
                var viewStructures: [SQLServerTableStructure] = []
                for table in tableCandidates {
                    let key = "\(table.schema.lowercased())|\(table.name.lowercased())"
                    let structure = SQLServerTableStructure(table: table, columns: (columnsByTable[key] ?? []).sorted { $0.ordinalPosition < $1.ordinalPosition }, primaryKey: pkByTable[key])
                    if table.isView { viewStructures.append(structure) } else { tableStructures.append(structure) }
                }
                return SQLServerSchemaStructure(name: schema, tables: tableStructures, views: viewStructures, functions: functions, procedures: procedures, triggers: triggers)
            }
        }
    }

    public func loadDatabaseStructure(
        database: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<SQLServerDatabaseStructure> {
        let resolvedDatabase = effectiveDatabase(database)
        return listSchemas(in: resolvedDatabase).flatMap { schemas in
            let initial = self.eventLoop.makeSucceededFuture([SQLServerSchemaStructure]())
            return schemas.reduce(initial) { partial, schema in
                partial.flatMap { collected in
                    self.loadSchemaStructure(database: resolvedDatabase, schema: schema.name, includeComments: includeComments).map { collected + [$0] }
                }
            }.map { SQLServerDatabaseStructure(database: resolvedDatabase, schemas: $0) }
        }
    }

    @available(macOS 12.0, *)
    public func loadSchemaStructure(database: String? = nil, schema: String, includeComments: Bool = false) async throws -> SQLServerSchemaStructure {
        try await loadSchemaStructure(database: database, schema: schema, includeComments: includeComments).get()
    }

    @available(macOS 12.0, *)
    public func loadDatabaseStructure(database: String? = nil, includeComments: Bool = false) async throws -> SQLServerDatabaseStructure {
        try await loadDatabaseStructure(database: database, includeComments: includeComments).get()
    }

    internal func scriptTableDefinition(database: String?, schema: String, table: String) -> EventLoopFuture<String?> {
        let colsFuture = fetchDetailedColumns(database: database, schema: schema, table: table)
        // Prefer catalog metadata here; sp_pkeys is slower and has timed out on
        // partitioned/temp test databases during Swift 6.2 integration runs.
        let pkFuture = listPrimaryKeysFromCatalog(database: database, schema: schema, table: table)
        let uqFuture = listUniqueConstraints(database: database, schema: schema, table: table)
        let fkFuture = listForeignKeys(database: database, schema: schema, table: table)
        let ixFuture = fetchTableScriptingIndexDetails(database: database, schema: schema, object: table)
        let ckFuture = listCheckConstraints(database: database, schema: schema, table: table)
        let group1 = colsFuture.and(pkFuture).and(uqFuture)
        let group2 = fkFuture.and(ixFuture).and(ckFuture)

        return group1.flatMap { group1Values in
            let ((columns, pks), uqs) = group1Values
            return group2.flatMap { group2Values in
                let ((fks, ixs), checks) = group2Values
                let lobFuture: EventLoopFuture<LobFilestreamInfo>
                if columns.contains(where: Self.requiresLobStorageMetadata) {
                    lobFuture = self.fetchLobAndFilestreamStorage(database: database, schema: schema, table: table)
                } else {
                    lobFuture = self.eventLoop.makeSucceededFuture(LobFilestreamInfo(textImageClause: nil, filestreamClause: nil))
                }
                let temporalFuture: EventLoopFuture<TemporalAndMemoryOptions>
                if columns.contains(where: { ($0.generatedAlwaysType ?? 0) != 0 }) {
                    temporalFuture = self.fetchTemporalAndMemoryOptions(database: database, schema: schema, table: table)
                } else {
                    temporalFuture = self.eventLoop.makeSucceededFuture(TemporalAndMemoryOptions())
                }
                let fg = pks
                    .first(where: \.isClustered)
                    .flatMap { clusteredPK in
                        ixs.first(where: { $0.name.caseInsensitiveCompare(clusteredPK.name) == .orderedSame })?.storageClause
                    }
                return lobFuture.and(temporalFuture).map { lob, temporal in

                func ident(_ name: String) -> String { "[\(Self.escapeIdentifier(name))]" }
                func qualified(_ s: String, _ n: String) -> String { "\(ident(s)).\(ident(n))" }

                func formatType(_ c: DetailedColumn) -> String {
                    let t = c.typeName.uppercased()
                    switch t {
                    case "DECIMAL", "NUMERIC":
                        return "\(t)(\(c.precision ?? 18), \(c.scale ?? 0))"
                    case "DATETIME2", "DATETIMEOFFSET", "TIME":
                        return "\(t)(\(c.scale ?? 7))"
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

                var lines: [String] = []
                for col in columns.sorted(by: { $0.columnId < $1.columnId }) {
                    if let expr = col.computedDefinition, !expr.isEmpty {
                        var computed = "    \(ident(col.name)) AS (\(expr))"
                        if col.isComputedPersisted { computed += " PERSISTED" }
                        lines.append(computed)
                        continue
                    }

                    var parts: [String] = [ident(col.name), formatType(col)]
                    if let collation = col.collationName, !collation.isEmpty { parts.append("COLLATE \(collation)") }
                    if col.isSparse { parts.append("SPARSE") }
                    if col.isRowGuidCol { parts.append("ROWGUIDCOL") }
                    if col.isIdentity {
                        if let seed = col.identitySeed, let increment = col.identityIncrement {
                            parts.append("IDENTITY(\(seed), \(increment))")
                        } else {
                            parts.append("IDENTITY")
                        }
                    }
                    if let generatedAlwaysType = col.generatedAlwaysType {
                        if generatedAlwaysType == 1 { parts.append("GENERATED ALWAYS AS ROW START") }
                        if generatedAlwaysType == 2 { parts.append("GENERATED ALWAYS AS ROW END") }
                    }
                    parts.append(col.isNullable ? "NULL" : "NOT NULL")
                    if let def = col.defaultDefinition, !def.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        if let defName = col.defaultName, !defName.isEmpty, !Self.isSystemGeneratedDefaultName(defName) {
                            parts.append("CONSTRAINT \(ident(defName)) DEFAULT \(def)")
                        } else {
                            parts.append("DEFAULT \(def)")
                        }
                    }
                    lines.append("    " + parts.joined(separator: " "))
                }

                if let periodClause = temporal.periodClause {
                    lines.append("    \(periodClause)")
                }

                func formatKeyColumns(_ columns: [KeyColumnMetadata]) -> String {
                    columns.sorted(by: { $0.ordinal < $1.ordinal })
                        .map { "\(ident($0.column)) \($0.isDescending ? "DESC" : "ASC")" }
                        .joined(separator: ", ")
                }

                for pk in pks {
                    var clause = "    PRIMARY KEY \(pk.isClustered ? "CLUSTERED" : "NONCLUSTERED") (\(formatKeyColumns(pk.columns)))"
                    if let idx = ixs.first(where: { $0.name.caseInsensitiveCompare(pk.name) == .orderedSame }) {
                        if let options = idx.optionClause, !options.isEmpty { clause += " WITH (\(options))" }
                        if let storage = idx.storageClause, !storage.isEmpty { clause += " \(storage)" }
                    }
                    lines.append(clause)
                }

                for uq in uqs {
                    var clause = "    CONSTRAINT \(ident(uq.name)) UNIQUE \(uq.isClustered ? "CLUSTERED" : "NONCLUSTERED") (\(formatKeyColumns(uq.columns)))"
                    if let idx = ixs.first(where: { $0.name.caseInsensitiveCompare(uq.name) == .orderedSame }) {
                        if let options = idx.optionClause, !options.isEmpty { clause += " WITH (\(options))" }
                        if let storage = idx.storageClause, !storage.isEmpty { clause += " \(storage)" }
                    }
                    lines.append(clause)
                }

                for check in checks {
                    lines.append("    CONSTRAINT \(ident(check.name)) CHECK (\(check.definition))")
                }

                for fk in fks {
                    let cols = fk.columns.sorted(by: { $0.ordinal < $1.ordinal })
                    let parentCols = cols.map { ident($0.parentColumn) }.joined(separator: ", ")
                    let refCols = cols.map { ident($0.referencedColumn) }.joined(separator: ", ")
                    var clause = "    CONSTRAINT \(ident(fk.name)) FOREIGN KEY (\(parentCols)) REFERENCES \(qualified(fk.referencedSchema, fk.referencedTable)) (\(refCols))"
                    let deleteAction = fk.deleteAction.uppercased()
                    let updateAction = fk.updateAction.uppercased()
                    if deleteAction != "NO ACTION" { clause += " ON DELETE \(deleteAction)" }
                    if updateAction != "NO ACTION" { clause += " ON UPDATE \(updateAction)" }
                    lines.append(clause)
                }

                let dbPreamble: String = {
                    if let db = self.effectiveDatabase(database), !db.isEmpty {
                        return "USE [\(Self.escapeIdentifier(db))]\nGO\n"
                    }
                    return ""
                }()

                var script = dbPreamble + "CREATE TABLE \(qualified(schema, table)) (\n" + lines.joined(separator: ",\n") + "\n)"
                if let storage = fg, !storage.isEmpty { script += " \(storage)" }
                if let textImage = lob.textImageClause, !textImage.isEmpty { script += " \(textImage)" }
                if let filestream = lob.filestreamClause, !filestream.isEmpty { script += " \(filestream)" }

                var withOptions: [String] = []
                if let memoryOptimized = temporal.memoryOptimizedClause, !memoryOptimized.isEmpty {
                    withOptions.append(memoryOptimized)
                }
                if let systemVersioning = temporal.systemVersioningClause, !systemVersioning.isEmpty {
                    withOptions.append(systemVersioning)
                }
                if !withOptions.isEmpty {
                    script += " WITH (\(withOptions.joined(separator: ", ")))"
                }
                script += ";"

                let constraintNames = Set((pks + uqs).map(\.name))
                func normalizePredicate(_ predicate: String) -> String {
                    var value = predicate.trimmingCharacters(in: .whitespacesAndNewlines)
                    while value.hasPrefix("("), value.hasSuffix(")") {
                        let stripped = String(value.dropFirst().dropLast()).trimmingCharacters(in: .whitespacesAndNewlines)
                        if stripped.isEmpty { break }
                        value = stripped
                    }
                    value.removeAll(where: { $0 == "[" || $0 == "]" })
                    return value
                }

                for ix in ixs where !constraintNames.contains(ix.name) {
                    if ix.indexType == 5 || ix.indexType == 6 {
                        var statement = "\n\nCREATE \(ix.indexType == 5 ? "CLUSTERED COLUMNSTORE" : "NONCLUSTERED COLUMNSTORE") INDEX \(ident(ix.name)) ON \(qualified(schema, table))"
                        if ix.indexType == 6 {
                            let cols = ix.columns.sorted(by: { $0.ordinal < $1.ordinal }).map { ident($0.column) }.joined(separator: ", ")
                            if !cols.isEmpty { statement += " (\(cols))" }
                        }
                        if let options = ix.optionClause, !options.isEmpty { statement += " WITH (\(options))" }
                        if let storage = ix.storageClause, !storage.isEmpty { statement += " \(storage)" }
                        statement += ";"
                        script.append(statement)
                        continue
                    }

                    let keyColumns = ix.columns
                        .filter { !$0.isIncluded }
                        .sorted(by: { $0.ordinal < $1.ordinal })
                        .map { "\(ident($0.column)) \($0.isDescending ? "DESC" : "ASC")" }
                        .joined(separator: ", ")
                    let includedColumns = ix.columns.filter(\.isIncluded).map { ident($0.column) }
                    var statement = "\n\nCREATE \(ix.isUnique ? "UNIQUE " : "")\(ix.isClustered ? "CLUSTERED" : "NONCLUSTERED") INDEX \(ident(ix.name)) ON \(qualified(schema, table)) (\(keyColumns))"
                    if !includedColumns.isEmpty {
                        statement += " INCLUDE (\(includedColumns.joined(separator: ", ")))"
                    }
                    if let filter = ix.filterDefinition, !filter.isEmpty {
                        statement += " WHERE \(normalizePredicate(filter))"
                    }
                    if let options = ix.optionClause, !options.isEmpty { statement += " WITH (\(options))" }
                    if let storage = ix.storageClause, !storage.isEmpty { statement += " \(storage)" }
                    statement += ";"
                    script.append(statement)
                }

                return script
                }
            }
        }
    }

    private static func requiresLobStorageMetadata(_ column: DetailedColumn) -> Bool {
        let typeName = column.typeName.uppercased()
        switch typeName {
        case "TEXT", "NTEXT", "IMAGE", "XML":
            return true
        case "VARCHAR", "NVARCHAR", "VARBINARY":
            return (column.maxLength ?? 0) < 0 || (column.maxLength ?? 0) >= 8000
        default:
            return false
        }
    }

    private struct CheckConstraintInfo {
        var name: String
        var definition: String
    }

    private func listCheckConstraints(database: String?, schema: String, table: String) -> EventLoopFuture<[CheckConstraintInfo]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(Self.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT ck.name, CAST(ck.definition AS NVARCHAR(4000)) AS definition
        FROM \(dbPrefix)sys.check_constraints AS ck
        JOIN \(dbPrefix)sys.tables AS t ON ck.parent_object_id = t.object_id
        JOIN \(dbPrefix)sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE s.name = N'\(Self.escapeLiteral(schema))'
          AND t.name = N'\(Self.escapeLiteral(table))'
        ORDER BY ck.name;
        """
        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string, let definition = row.column("definition")?.string else { return nil }
                return CheckConstraintInfo(name: name, definition: definition.trimmingCharacters(in: .whitespacesAndNewlines))
            }
        }
    }

    private func fetchTableFilegroup(database: String?, schema: String, table: String) -> EventLoopFuture<String?> {
        let dbPrefix = effectiveDatabase(database).map { "[\(Self.escapeIdentifier($0))]." } ?? ""
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
        WHERE s.name = N'\(Self.escapeLiteral(schema))'
          AND t.name = N'\(Self.escapeLiteral(table))'
        GROUP BY ds.name, ps.name
        ORDER BY CASE WHEN ps.name IS NULL THEN 1 ELSE 0 END, ds.name;
        """
        return queryExecutor(sql).map { rows in
            guard let row = rows.first else { return nil }
            let dataSpace = row.column("data_space_name")?.string
            let scheme = row.column("partition_scheme_name")?.string
            let partitionColumns = row.column("partition_columns")?.string?.split(separator: ",").map(String.init) ?? []
            if let scheme, !scheme.isEmpty {
                let cols = partitionColumns.map { "[\(Self.escapeIdentifier($0))]" }.joined(separator: ", ")
                return "ON [\(Self.escapeIdentifier(scheme))](\(cols))"
            }
            if let dataSpace, !dataSpace.isEmpty {
                return "ON [\(Self.escapeIdentifier(dataSpace))]"
            }
            return nil
        }
    }

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
        var collationName: String?
    }

    private static func isSystemGeneratedDefaultName(_ name: String) -> Bool {
        name.hasPrefix("DF__")
    }

    private func fetchDetailedColumns(database: String?, schema: String, table: String) -> EventLoopFuture<[DetailedColumn]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(Self.escapeIdentifier($0))]." } ?? ""
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
            CAST(cc.definition AS NVARCHAR(4000)) AS computed_definition,
            cc.is_persisted,
            dc.name AS default_name,
            CAST(dc.definition AS NVARCHAR(4000)) AS default_definition,
            c.collation_name
        FROM \(dbPrefix)sys.columns AS c
        JOIN \(dbPrefix)sys.tables AS t ON t.object_id = c.object_id
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN \(dbPrefix)sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
        LEFT JOIN \(dbPrefix)sys.identity_columns AS ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        LEFT JOIN \(dbPrefix)sys.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE s.name = N'\(Self.escapeLiteral(schema))'
          AND t.name = N'\(Self.escapeLiteral(table))'
        ORDER BY c.column_id;
        """

        @Sendable
        func intLike(_ row: TDSRow, _ key: String) -> Int? {
            if let value = row.column(key)?.int { return value }
            if let value = row.column(key)?.string, let parsed = Int(value) { return parsed }
            return nil
        }

        return queryExecutor(sql).map { rows in
            rows.compactMap { row in
                guard let columnId = row.column("column_id")?.int,
                      let name = row.column("column_name")?.string,
                      let typeName = row.column("type_name")?.string else { return nil }
                return DetailedColumn(
                    columnId: columnId,
                    name: name,
                    typeName: typeName,
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

    private struct LobFilestreamInfo {
        var textImageClause: String?
        var filestreamClause: String?
    }

    private func fetchLobAndFilestreamStorage(database: String?, schema: String, table: String) -> EventLoopFuture<LobFilestreamInfo> {
        let dbPrefix = effectiveDatabase(database).map { "[\(Self.escapeIdentifier($0))]." } ?? ""
        let lobSQL = """
        SELECT TOP 1 ds.name AS lob_data_space
        FROM \(dbPrefix)sys.objects AS o
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = o.schema_id
        JOIN \(dbPrefix)sys.partitions AS p ON p.object_id = o.object_id
        JOIN \(dbPrefix)sys.allocation_units AS au ON au.container_id = p.hobt_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = au.data_space_id
        WHERE s.name = N'\(Self.escapeLiteral(schema))'
          AND o.name = N'\(Self.escapeLiteral(table))'
          AND au.type IN (2)
        ORDER BY ds.name;
        """
        let filestreamSQL = """
        SELECT ds.name AS filestream_data_space
        FROM \(dbPrefix)sys.tables AS t
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        JOIN \(dbPrefix)sys.data_spaces AS ds ON ds.data_space_id = t.filestream_data_space_id
        WHERE t.filestream_data_space_id IS NOT NULL
          AND s.name = N'\(Self.escapeLiteral(schema))'
          AND t.name = N'\(Self.escapeLiteral(table))';
        """
        let lobFuture = queryExecutor(lobSQL).map { rows in
            rows.first?.column("lob_data_space")?.string.map { "TEXTIMAGE_ON [\(Self.escapeIdentifier($0))]" }
        }
        let fsFuture = queryExecutor(filestreamSQL).map { rows in
            rows.first?.column("filestream_data_space")?.string.map { "FILESTREAM_ON [\(Self.escapeIdentifier($0))]" }
        }
        return lobFuture.and(fsFuture).map { LobFilestreamInfo(textImageClause: $0.0, filestreamClause: $0.1) }
    }

    private struct TemporalAndMemoryOptions {
        var periodClause: String?
        var systemVersioningClause: String?
        var memoryOptimizedClause: String?
    }

    private func fetchTemporalAndMemoryOptions(database: String?, schema: String, table: String) -> EventLoopFuture<TemporalAndMemoryOptions> {
        let dbPrefix = effectiveDatabase(database).map { "[\(Self.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT
            t.temporal_type,
            t.history_table_id,
            t.is_memory_optimized,
            t.durability_desc,
            p.start_column_id,
            p.end_column_id,
            hs.name AS history_schema,
            ht.name AS history_table
        FROM \(dbPrefix)sys.tables AS t
        JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN \(dbPrefix)sys.tables AS ht ON ht.object_id = t.history_table_id
        LEFT JOIN \(dbPrefix)sys.schemas AS hs ON hs.schema_id = ht.schema_id
        LEFT JOIN \(dbPrefix)sys.periods AS p ON p.object_id = t.object_id
        WHERE s.name = N'\(Self.escapeLiteral(schema))'
          AND t.name = N'\(Self.escapeLiteral(table))';
        """

        @Sendable
        func resolvePeriodColumnName(columnID: Int?) -> EventLoopFuture<String?> {
            guard let columnID else { return eventLoop.makeSucceededFuture(nil) }
            let query = """
            SELECT c.name
            FROM \(dbPrefix)sys.columns AS c
            JOIN \(dbPrefix)sys.tables AS t ON t.object_id = c.object_id
            JOIN \(dbPrefix)sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = N'\(Self.escapeLiteral(schema))'
              AND t.name = N'\(Self.escapeLiteral(table))'
              AND c.column_id = \(columnID);
            """
            return queryExecutor(query).map { $0.first?.column("name")?.string }
        }

        return queryExecutor(sql).flatMap { rows in
            guard let row = rows.first else {
                return self.eventLoop.makeSucceededFuture(TemporalAndMemoryOptions())
            }

            let temporalType = row.column("temporal_type")?.int ?? 0
            let isMemoryOptimized = (row.column("is_memory_optimized")?.int ?? 0) != 0
            let durability = row.column("durability_desc")?.string
            let historySchema = row.column("history_schema")?.string
            let historyTable = row.column("history_table")?.string
            let startColumnID = row.column("start_column_id")?.int
            let endColumnID = row.column("end_column_id")?.int

            let memoryClause: String? = isMemoryOptimized ? "MEMORY_OPTIMIZED = ON\(durability.map { ", DURABILITY = \($0)" } ?? "")" : nil
            if temporalType == 0 {
                return self.eventLoop.makeSucceededFuture(
                    TemporalAndMemoryOptions(periodClause: nil, systemVersioningClause: nil, memoryOptimizedClause: memoryClause)
                )
            }

            return resolvePeriodColumnName(columnID: startColumnID)
                .and(resolvePeriodColumnName(columnID: endColumnID))
                .map { startName, endName in
                    let periodClause: String?
                    if let startName, let endName {
                        periodClause = "PERIOD FOR SYSTEM_TIME ([\(Self.escapeIdentifier(startName))], [\(Self.escapeIdentifier(endName))])"
                    } else {
                        periodClause = nil
                    }

                    var systemVersioning = "SYSTEM_VERSIONING = ON"
                    if let historySchema, let historyTable, !historySchema.isEmpty, !historyTable.isEmpty {
                        systemVersioning += " (HISTORY_TABLE = [\(Self.escapeIdentifier(historySchema))].[\(Self.escapeIdentifier(historyTable))])"
                    }

                    return TemporalAndMemoryOptions(
                        periodClause: periodClause,
                        systemVersioningClause: systemVersioning,
                        memoryOptimizedClause: memoryClause
                    )
                }
        }
    }

    private struct IndexDetail {
        var name: String
        var isUnique: Bool
        var isClustered: Bool
        var indexType: Int
        var filterDefinition: String?
        var columns: [IndexColumnMetadata]
        var storageClause: String?
        var optionClause: String?
    }

    private func fetchTableScriptingIndexDetails(database: String?, schema: String, object: String) -> EventLoopFuture<[IndexDetail]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(Self.escapeIdentifier($0))]." } ?? ""
        let sql = """
        SELECT
            i.name AS index_name,
            i.is_unique,
            i.type AS index_type,
            i.is_padded,
            i.fill_factor,
            i.allow_row_locks,
            i.allow_page_locks,
            i.ignore_dup_key,
            CAST(i.filter_definition AS NVARCHAR(4000)) AS filter_definition,
            ds.name AS data_space_name,
            ps.name AS partition_scheme_name,
            st.no_recompute,
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
        JOIN \(dbPrefix)sys.stats AS st ON st.object_id = i.object_id AND st.stats_id = i.index_id
        LEFT JOIN \(dbPrefix)sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
        JOIN \(dbPrefix)sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN \(dbPrefix)sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        OUTER APPLY (
            SELECT CASE WHEN MIN(p.data_compression_desc) = MAX(p.data_compression_desc)
                        THEN MIN(p.data_compression_desc) ELSE NULL END AS comp_desc
            FROM \(dbPrefix)sys.partitions AS p
            WHERE p.object_id = i.object_id AND p.index_id = i.index_id
        ) AS pcomp
        WHERE s.name = N'\(Self.escapeLiteral(schema))'
          AND o.name = N'\(Self.escapeLiteral(object))'
          AND i.index_id > 0
          AND i.is_hypothetical = 0
        ORDER BY i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
        """

        struct Partial {
            var isUnique: Bool
            var isClustered: Bool
            var indexType: Int
            var filterDefinition: String?
            var columns: [IndexColumnMetadata]
            var partitionColumns: [String]
            var dataSpaceName: String?
            var partitionSchemeName: String?
            var isPadded: Bool
            var fillFactor: Int
            var allowRowLocks: Bool
            var allowPageLocks: Bool
            var ignoreDupKey: Bool
            var compression: String?
            var noRecompute: Bool
        }

        return queryExecutor(sql).map { rows in
            var grouped: [String: Partial] = [:]
            for row in rows {
                guard let indexName = row.column("index_name")?.string else { continue }
                var partial = grouped[indexName] ?? Partial(
                    isUnique: (row.column("is_unique")?.int ?? 0) != 0,
                    isClustered: (row.column("index_type")?.int ?? 0) == 1,
                    indexType: row.column("index_type")?.int ?? 0,
                    filterDefinition: row.column("filter_definition")?.string,
                    columns: [],
                    partitionColumns: [],
                    dataSpaceName: row.column("data_space_name")?.string,
                    partitionSchemeName: row.column("partition_scheme_name")?.string,
                    isPadded: (row.column("is_padded")?.int ?? 0) != 0,
                    fillFactor: row.column("fill_factor")?.int ?? 0,
                    allowRowLocks: (row.column("allow_row_locks")?.int ?? 1) != 0,
                    allowPageLocks: (row.column("allow_page_locks")?.int ?? 1) != 0,
                    ignoreDupKey: (row.column("ignore_dup_key")?.int ?? 0) != 0,
                    compression: row.column("compression_desc")?.string,
                    noRecompute: (row.column("no_recompute")?.int ?? 0) != 0
                )
                if let columnName = row.column("column_name")?.string {
                    partial.columns.append(
                        IndexColumnMetadata(
                            column: columnName,
                            ordinal: row.column("key_ordinal")?.int ?? 0,
                            isDescending: (row.column("is_descending_key")?.int ?? 0) != 0,
                            isIncluded: (row.column("is_included_column")?.int ?? 0) != 0
                        )
                    )
                    if (row.column("partition_ordinal")?.int ?? 0) > 0 {
                        partial.partitionColumns.append(columnName)
                    }
                }
                grouped[indexName] = partial
            }

            func buildOptions(from partial: Partial) -> String? {
                var parts: [String] = []
                parts.append("PAD_INDEX = \(partial.isPadded ? "ON" : "OFF")")
                if partial.fillFactor > 0 { parts.append("FILLFACTOR = \(partial.fillFactor)") }
                parts.append("ALLOW_ROW_LOCKS = \(partial.allowRowLocks ? "ON" : "OFF")")
                parts.append("ALLOW_PAGE_LOCKS = \(partial.allowPageLocks ? "ON" : "OFF")")
                if partial.ignoreDupKey { parts.append("IGNORE_DUP_KEY = ON") }
                if partial.noRecompute { parts.append("STATISTICS_NORECOMPUTE = ON") }
                if let compression = partial.compression, compression != "NONE" {
                    parts.append("DATA_COMPRESSION = \(compression)")
                }
                return parts.isEmpty ? nil : parts.joined(separator: ", ")
            }

            func buildStorage(from partial: Partial) -> String? {
                if let scheme = partial.partitionSchemeName, !scheme.isEmpty {
                    if !partial.partitionColumns.isEmpty {
                        let cols = partial.partitionColumns.map { "[\(Self.escapeIdentifier($0))]" }.joined(separator: ", ")
                        return "ON [\(Self.escapeIdentifier(scheme))](\(cols))"
                    }
                    return "ON [\(Self.escapeIdentifier(scheme))]"
                }
                if let dataSpace = partial.dataSpaceName, !dataSpace.isEmpty {
                    return "ON [\(Self.escapeIdentifier(dataSpace))]"
                }
                return nil
            }

            return grouped.map { name, partial in
                IndexDetail(
                    name: name,
                    isUnique: partial.isUnique,
                    isClustered: partial.isClustered,
                    indexType: partial.indexType,
                    filterDefinition: partial.filterDefinition,
                    columns: partial.columns,
                    storageClause: buildStorage(from: partial),
                    optionClause: buildOptions(from: partial)
                )
            }.sorted(by: { $0.name < $1.name })
        }
    }
}
