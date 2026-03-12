import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerMetadataOperations {
    private func deduplicateColumns(_ columns: [ColumnMetadata]) -> [ColumnMetadata] {
        var seen = Set<String>()
        var deduplicated: [ColumnMetadata] = []
        deduplicated.reserveCapacity(columns.count)

        for column in columns {
            let key = "\(column.schema).\(column.table).\(column.name)"
            if seen.insert(key).inserted {
                deduplicated.append(column)
            }
        }

        return deduplicated
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
            return eventLoop.makeSucceededFuture(cached)
        }

        let willUseSchemaBulk = !includeComments && !self.configuration.preferStoredProcedureColumns

        let isViewFuture: EventLoopFuture<Bool>
        if willUseSchemaBulk {
            isViewFuture = eventLoop.makeSucceededFuture(false)
        } else if let objectTypeHint {
            let normalized = objectTypeHint.uppercased()
            let isView = normalized.contains("VIEW") || normalized == "V"
            isViewFuture = eventLoop.makeSucceededFuture(isView)
        } else {
            isViewFuture = isViewObject(database: resolvedDatabase, schema: schema, table: table)
        }

        let lastResolvedIsView = NIOLockedValueBox(false)

        return isViewFuture.flatMap { isView -> EventLoopFuture<[ColumnMetadata]> in
            lastResolvedIsView.withLockedValue { $0 = isView }
            let useStoredProc = !isView && self.configuration.preferStoredProcedureColumns
            let useSchemaBulk = !includeComments && !self.configuration.preferStoredProcedureColumns
            
            let baseSource: EventLoopFuture<[ColumnMetadata]>
            if useSchemaBulk {
                baseSource = self.loadColumnsFromCatalog(
                    database: resolvedDatabase,
                    schema: schema,
                    table: table,
                    isView: false,
                    includeDefaultMetadata: true,
                    includeComments: includeComments
                )
            } else if isView || !useStoredProc {
                baseSource = self.loadColumnsFromCatalog(database: resolvedDatabase, schema: schema, table: table, isView: isView, includeDefaultMetadata: false, includeComments: includeComments)
            } else {
                baseSource = self.loadColumnsUsingStoredProcedure(database: resolvedDatabase, schema: schema, table: table).flatMap { cols in
                    guard includeComments else { return self.eventLoop.makeSucceededFuture(cols) }
                    return self.fetchColumnComments(database: resolvedDatabase, schema: schema, table: table).map { commentMap in
                        cols.map { c in
                            return ColumnMetadata(
                                schema: c.schema, table: c.table, name: c.name, typeName: c.typeName,
                                systemTypeName: c.systemTypeName, maxLength: c.maxLength, precision: c.precision, scale: c.scale,
                                collationName: c.collationName, isNullable: c.isNullable, isIdentity: c.isIdentity,
                                isComputed: c.isComputed, hasDefaultValue: c.hasDefaultValue, defaultDefinition: c.defaultDefinition,
                                computedDefinition: c.computedDefinition, ordinalPosition: c.ordinalPosition,
                                identitySeed: c.identitySeed, identityIncrement: c.identityIncrement,
                                checkDefinition: c.checkDefinition, comment: commentMap[c.name]
                            )
                        }
                    }
                }
            }

            return baseSource.map { columns in
                if !includeComments, !columns.isEmpty, let cache = self.cache {
                    cache.setValue(columns, forKey: cacheKey)
                }
                return columns
            }
        }.flatMapError { _ in
            @Sendable
            func filterTable(_ columns: [ColumnMetadata]) -> [ColumnMetadata] {
                columns
                    .filter { $0.table.caseInsensitiveCompare(table) == .orderedSame }
                    .sorted { $0.ordinalPosition < $1.ordinalPosition }
            }
            
            let fallbackSource: EventLoopFuture<[ColumnMetadata]>
            if lastResolvedIsView.withLockedValue({ $0 }) {
                fallbackSource = self.listColumnsForSchema(database: resolvedDatabase, schema: schema, includeComments: includeComments).map(filterTable)
            } else if self.configuration.preferStoredProcedureColumns {
                fallbackSource = self.listColumnsForSchema(database: resolvedDatabase, schema: schema, includeComments: includeComments).map(filterTable).flatMapError { _ in
                    self.loadColumnsFromCatalog(database: resolvedDatabase, schema: schema, table: table, isView: false, includeDefaultMetadata: false, includeComments: includeComments)
                }
            } else {
                fallbackSource = self.listColumnsForSchema(database: resolvedDatabase, schema: schema, includeComments: includeComments).map(filterTable).flatMapError { _ in
                    self.loadColumnsUsingStoredProcedure(database: resolvedDatabase, schema: schema, table: table)
                }
            }
            return fallbackSource
        }
    }

    public func listColumnsForSchema(
        database: String? = nil,
        schema: String,
        includeComments: Bool = false
    ) -> EventLoopFuture<[ColumnMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        return loadColumnsFromCatalogForDatabase(
            database: resolvedDatabase,
            schema: schema,
            includeComments: includeComments
        ).map { columns in
            let sorted = columns.sorted {
                if $0.schema != $1.schema { return $0.schema < $1.schema }
                if $0.table != $1.table { return $0.table < $1.table }
                return $0.ordinalPosition < $1.ordinalPosition
            }
            if !includeComments, let cache = self.cache {
                var grouped: [String: [ColumnMetadata]] = [:]
                for column in sorted {
                    let key = "\(resolvedDatabase ?? "").\(column.schema).\(column.table)"
                    grouped[key, default: []].append(column)
                }
                for (key, cols) in grouped {
                    cache.setValue(cols.sorted { $0.ordinalPosition < $1.ordinalPosition }, forKey: key)
                }
            }
            return sorted
        }
    }

    public func listColumnsForDatabase(
        database: String? = nil,
        includeComments: Bool = false
    ) -> EventLoopFuture<[ColumnMetadata]> {
        let resolvedDatabase = effectiveDatabase(database)
        return loadColumnsFromCatalogForDatabase(
            database: resolvedDatabase,
            schema: nil,
            includeComments: includeComments
        ).map { columns in
            let sorted = columns.sorted {
                if $0.schema != $1.schema { return $0.schema < $1.schema }
                if $0.table != $1.table { return $0.table < $1.table }
                return $0.ordinalPosition < $1.ordinalPosition
            }
            if !includeComments, let cache = self.cache {
                var grouped: [String: [ColumnMetadata]] = [:]
                for column in sorted {
                    let key = "\(resolvedDatabase ?? "").\(column.schema).\(column.table)"
                    grouped[key, default: []].append(column)
                }
                for (key, cols) in grouped {
                    cache.setValue(cols.sorted { $0.ordinalPosition < $1.ordinalPosition }, forKey: key)
                }
            }
            return sorted
        }
    }

    internal func isViewObject(database: String?, schema: String, table: String) -> EventLoopFuture<Bool> {
        let dbPrefix = database.map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let sql = "SELECT is_view = CONVERT(bit, OBJECTPROPERTYEX(OBJECT_ID(N'\(dbPrefix)[\(SQLServerMetadataOperations.escapeIdentifier(schema))].[\(SQLServerMetadataOperations.escapeIdentifier(table))]'), 'IsView'))"
        return queryExecutor(sql).map { rows in
            guard let value = rows.first?.column("is_view")?.int else { return false }
            return value != 0
        }
    }

    internal func fetchColumnComments(database: String?, schema: String, table: String) -> EventLoopFuture<[String: String]> {
        let dbPrefix = effectiveDatabase(database).map { "[\(SQLServerMetadataOperations.escapeIdentifier($0))]." } ?? ""
        let escapedSchema = SQLServerMetadataOperations.escapeLiteral(schema)
        let escapedTable = SQLServerMetadataOperations.escapeLiteral(table)
        let sql = """
        SELECT c.name AS column_name, comment = ISNULL(CAST(ep.value AS NVARCHAR(4000)), '')
        FROM \(dbPrefix)sys.columns AS c WITH (NOLOCK)
        JOIN \(dbPrefix)sys.objects AS o WITH (NOLOCK) ON c.object_id = o.object_id
        JOIN \(dbPrefix)sys.schemas AS s WITH (NOLOCK) ON o.schema_id = s.schema_id
        LEFT JOIN \(dbPrefix)sys.extended_properties AS ep WITH (NOLOCK)
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

    internal func loadColumnsUsingStoredProcedure(
        database: String?,
        schema: String,
        table: String
    ) -> EventLoopFuture<[ColumnMetadata]> {
        var parameters: [String] = [
            "@table_name = N'\(SQLServerMetadataOperations.escapeLiteral(table))'",
            "@table_owner = N'\(SQLServerMetadataOperations.escapeLiteral(schema))'",
            "@ODBCVer = 3"
        ]
        if let qualifier = effectiveDatabase(database) {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataOperations.escapeLiteral(qualifier))'")
        }
        let sql = "SET NOCOUNT ON; EXEC sp_columns_100 \(parameters.joined(separator: ", "));"

        return queryExecutor(sql).map { rows in
            let columns = rows.compactMap { row -> ColumnMetadata? in
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
                    schema: schemaName, table: tableName, name: columnName, typeName: typeName,
                    systemTypeName: systemTypeName, maxLength: maxLength, precision: precision, scale: scale,
                    collationName: nil, isNullable: isNullable, isIdentity: isIdentity,
                    isComputed: isComputed, hasDefaultValue: hasDefaultValue, defaultDefinition: defaultDefinition,
                    computedDefinition: nil, ordinalPosition: ordinal,
                    identitySeed: nil, identityIncrement: nil,
                    checkDefinition: nil, comment: nil
                )
            }
            return self.deduplicateColumns(columns)
        }
    }

    internal func loadColumnsFromCatalog(
        database: String?,
        schema: String,
        table: String,
        isView: Bool,
        includeDefaultMetadata: Bool,
        includeComments: Bool
    ) -> EventLoopFuture<[ColumnMetadata]> {
        let escapedSchema = SQLServerMetadataOperations.escapeLiteral(schema)
        let escapedTable = SQLServerMetadataOperations.escapeLiteral(table)
        
        let defaultSelect = isView || !includeDefaultMetadata ? "NULL AS default_definition" : "CAST(dc.definition AS NVARCHAR(4000)) AS default_definition"
        let computedSelect = isView || !includeDefaultMetadata ? "NULL AS computed_definition" : "CAST(cc.definition AS NVARCHAR(4000)) AS computed_definition"
        let identitySelect = isView || !includeDefaultMetadata ? "NULL AS identity_seed, NULL AS identity_increment" : "TRY_CONVERT(BIGINT, ic.seed_value) AS identity_seed, TRY_CONVERT(BIGINT, ic.increment_value) AS identity_increment"
        let checkSelect = isView || !includeDefaultMetadata ? "NULL AS check_definition" : "CAST(ck.definition AS NVARCHAR(4000)) AS check_definition"
        let defaultIdSelect = isView || includeDefaultMetadata ? "" : ", default_object_id = c.default_object_id"

        var fromClause = """
        FROM \(qualified(database, object: "sys.columns")) AS c WITH (NOLOCK)
        JOIN \(qualified(database, object: "sys.objects")) AS o WITH (NOLOCK) ON c.object_id = o.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s WITH (NOLOCK) ON o.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.types")) AS ut WITH (NOLOCK) ON c.user_type_id = ut.user_type_id
        JOIN \(qualified(database, object: "sys.types")) AS st WITH (NOLOCK) ON c.system_type_id = st.system_type_id AND st.user_type_id = st.system_type_id
        """

        if !isView && includeDefaultMetadata {
            fromClause += """

        LEFT JOIN \(qualified(database, object: "sys.default_constraints")) AS dc WITH (NOLOCK) ON c.default_object_id = dc.object_id
        LEFT JOIN \(qualified(database, object: "sys.computed_columns")) AS cc WITH (NOLOCK) ON c.object_id = cc.object_id AND c.column_id = cc.column_id
        LEFT JOIN \(qualified(database, object: "sys.identity_columns")) AS ic WITH (NOLOCK) ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        LEFT JOIN \(qualified(database, object: "sys.check_constraints")) AS ck WITH (NOLOCK) ON c.default_object_id = ck.object_id AND ck.parent_column_id = c.column_id
        """
        }

        if includeComments {
            fromClause += """

        LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS epc WITH (NOLOCK)
            ON epc.class = 1 AND epc.major_id = o.object_id AND epc.minor_id = c.column_id AND epc.name = N'MS_Description'
        """
        }

        let commentSelect = includeComments ? ", CAST(epc.value AS NVARCHAR(4000)) AS column_comment" : ""
        let sql = """
        SELECT DISTINCT
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
            is_computed = c.is_computed\(defaultIdSelect),
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

        return queryExecutor(sql).map { rows in
            let columns = rows.compactMap { row -> ColumnMetadata? in
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
                let defaultObjectId = includeDefaultMetadata ? nil : row.column("default_object_id")?.int
                let hasDefaultValue = includeDefaultMetadata ? (defaultDefinition?.isEmpty == false) : ((defaultObjectId ?? 0) != 0)
                
                let identitySeed = includeDefaultMetadata ? row.column("identity_seed")?.int : nil
                let identityIncrement = includeDefaultMetadata ? row.column("identity_increment")?.int : nil
                let checkDefinition = includeDefaultMetadata ? row.column("check_definition")?.string : nil

                return ColumnMetadata(
                    schema: schemaName, table: tableName, name: columnName, typeName: typeName,
                    systemTypeName: systemTypeName, maxLength: normalizedLength, precision: precision, scale: scale,
                    collationName: collationName, isNullable: isNullable, isIdentity: isIdentity,
                    isComputed: isComputed, hasDefaultValue: hasDefaultValue, 
                    defaultDefinition: includeDefaultMetadata ? defaultDefinition : nil,
                    computedDefinition: includeDefaultMetadata ? computedDefinition : nil,
                    ordinalPosition: ordinal, identitySeed: identitySeed, identityIncrement: identityIncrement,
                    checkDefinition: checkDefinition, comment: row.column("column_comment")?.string
                )
            }
            return self.deduplicateColumns(columns)
        }
    }

    internal func loadColumnsFromCatalogForDatabase(
        database: String?,
        schema: String?,
        includeComments: Bool
    ) -> EventLoopFuture<[ColumnMetadata]> {
        let commentSelect = includeComments ? ", CAST(epc.value AS NVARCHAR(4000)) AS column_comment" : ""
        let commentJoin = includeComments
            ? """
            LEFT JOIN \(qualified(database, object: "sys.extended_properties")) AS epc WITH (NOLOCK)
                ON epc.class = 1 AND epc.major_id = o.object_id AND epc.minor_id = c.column_id AND epc.name = N'MS_Description'
            """
            : ""

        var predicates: [String] = ["o.type IN ('U', 'V')"]
        if let schema {
            predicates.append("s.name = N'\(SQLServerMetadataOperations.escapeLiteral(schema))'")
        }
        if !self.configuration.includeSystemSchemas {
            predicates.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }

        let whereClause = predicates.isEmpty ? "" : "WHERE " + predicates.joined(separator: " AND ")

        let sql = """
        SELECT DISTINCT
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
            default_object_id = c.default_object_id,
            CAST(dc.definition AS NVARCHAR(4000)) AS default_definition,
            ordinal_position = c.column_id\(commentSelect)
        FROM \(qualified(database, object: "sys.columns")) AS c WITH (NOLOCK)
        JOIN \(qualified(database, object: "sys.objects")) AS o WITH (NOLOCK) ON c.object_id = o.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s WITH (NOLOCK) ON o.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.types")) AS ut WITH (NOLOCK) ON c.user_type_id = ut.user_type_id
        JOIN \(qualified(database, object: "sys.types")) AS st WITH (NOLOCK) ON c.system_type_id = st.system_type_id AND st.user_type_id = st.system_type_id
        LEFT JOIN \(qualified(database, object: "sys.default_constraints")) AS dc WITH (NOLOCK) ON c.default_object_id = dc.object_id AND o.type = 'U'
        \(commentJoin)
        \(whereClause);
        """

        return queryExecutor(sql).map { rows in
            let columns = rows.compactMap { row -> ColumnMetadata? in
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
                let defaultObjectId = row.column("default_object_id")?.int ?? 0
                let isNullable = (row.column("is_nullable")?.int ?? 1) != 0
                let isIdentity = (row.column("is_identity")?.int ?? 0) != 0
                let isComputed = (row.column("is_computed")?.int ?? 0) != 0
                let defaultDefinition = row.column("default_definition")?.string
                let hasDefaultValue = defaultObjectId != 0

                return ColumnMetadata(
                    schema: schemaName, table: tableName, name: columnName, typeName: typeName,
                    systemTypeName: systemTypeName, maxLength: normalizedLength, precision: precision, scale: scale,
                    collationName: collationName, isNullable: isNullable, isIdentity: isIdentity,
                    isComputed: isComputed, hasDefaultValue: hasDefaultValue, defaultDefinition: defaultDefinition,
                    computedDefinition: nil, ordinalPosition: ordinal,
                    identitySeed: nil, identityIncrement: nil,
                    checkDefinition: nil, comment: row.column("column_comment")?.string
                )
            }
            return self.deduplicateColumns(columns)
        }
    }
}
