import Foundation
import NIO
import NIOConcurrencyHelpers

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
        case procedure
        case scalarFunction
        case tableFunction
        case trigger
        case other

        public static func from(typeDesc: String, objectType: String? = nil) -> ObjectType {
            let normalized = typeDesc.uppercased()
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
    private let cache: MetadataCache<[ColumnMetadata]>?
    private let configuration: Configuration
    private let defaultDatabaseLock = NIOLock()
    private var defaultDatabase: String?

    public convenience init(
        connection: SQLServerConnection,
        configuration: Configuration = Configuration()
    ) {
        self.init(
            connection: connection.underlying,
            configuration: configuration,
            sharedCache: nil,
            defaultDatabase: connection.currentDatabase
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
        defaultDatabase: String?
    ) {
        self.connection = connection
        self.configuration = configuration
        if self.configuration.enableColumnCache {
            self.cache = sharedCache ?? MetadataCache<[ColumnMetadata]>()
        } else {
            self.cache = nil
        }
        self.defaultDatabase = defaultDatabase
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

    // MARK: - Databases

    public func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        let sql = "EXEC sp_databases;"
        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("DATABASE_NAME")?.string else { return nil }
                return DatabaseMetadata(name: name)
            }
        }
    }

    // MARK: - Schemas

    public func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        var parameters: [String] = []
        if let catalog = effectiveDatabase(database) {
            parameters.append("@catalog_name = N'\(SQLServerMetadataClient.escapeLiteral(catalog))'")
        }
        let sql = "EXEC sp_schemata\(parameters.isEmpty ? "" : " " + parameters.joined(separator: ", "));"
        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("SCHEMA_NAME")?.string else { return nil }

                if !self.configuration.includeSystemSchemas,
                   name.caseInsensitiveCompare("sys") == .orderedSame ||
                   name.caseInsensitiveCompare("INFORMATION_SCHEMA") == .orderedSame {
                    return nil
                }

                return SchemaMetadata(name: name)
            }
        }
    }

    // MARK: - Tables

    public func listTables(database: String? = nil, schema: String? = nil) -> EventLoopFuture<[TableMetadata]> {
        var parameters: [String] = []
        if let qualifier = effectiveDatabase(database) {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(qualifier))'")
        }
        if let schema {
            parameters.append("@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        }
        let sql = "EXEC sp_tables\(parameters.isEmpty ? "" : " " + parameters.joined(separator: ", "));"

        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("TABLE_SCHEM")?.string,
                    let tableName = row.column("TABLE_NAME")?.string,
                    let tableType = row.column("TABLE_TYPE")?.string
                else {
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

                if let schema, schemaName.caseInsensitiveCompare(schema) != .orderedSame {
                    return nil
                }

                let normalizedType = tableType.uppercased()
                let isSystem = normalizedType.contains("SYSTEM")
                return TableMetadata(schema: schemaName, name: tableName, type: normalizedType, isSystemObject: isSystem)
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

        return connection.rawSql(sql).map { rows in
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
        let qualifier = effectiveDatabase(database)
        var arguments: [String] = [
            "@procedure_name = N'\(SQLServerMetadataClient.escapeLiteral(object))'",
            "@procedure_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'",
            "@column_name = N'%'",
            "@ODBCVer = 3"
        ]
        if let qualifier {
            arguments.insert("@procedure_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(qualifier))'", at: 2)
        }

        let sql = "EXEC sp_sproc_columns_100 \(arguments.joined(separator: ", "));"

        return connection.rawSql(sql).flatMap { rows in
            let values: [ParameterMetadata] = rows.compactMap { row -> ParameterMetadata? in
                guard
                    let schemaName = row.column("PROCEDURE_OWNER")?.string,
                    let rawObjectName = row.column("PROCEDURE_NAME")?.string,
                    let columnType = row.column("COLUMN_TYPE")?.int,
                    let name = row.column("COLUMN_NAME")?.string,
                    let ordinal = row.column("ORDINAL_POSITION")?.int,
                    let typeName = row.column("TYPE_NAME")?.string
                else {
                    return nil
                }

                // COLUMN_TYPE: 1=input, 2=input/output, 3=output, 4=return value, 5=result column.
                if columnType == 5 {
                    return nil
                }

                let objectName = SQLServerMetadataClient.normalizeRoutineName(rawObjectName)
                let systemType = row.column("TYPE_NAME")?.string
                let maxLength = row.column("LENGTH")?.int
                let precision = row.column("PRECISION")?.int
                let scale = row.column("SCALE")?.int
                let defaultValue = row.column("COLUMN_DEF")?.string
                let hasDefault = defaultValue?.isEmpty == false
                let isOutput = columnType == 2 || columnType == 3
                let isReturnValue = columnType == 4 || ordinal == 0

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
                    defaultValue: defaultValue,
                    isReadOnly: row.column("IS_READONLY")?.bool ?? false
                )
            }
            return self.connection.eventLoop.makeSucceededFuture(values)
        }
    }

    // MARK: - Key Constraints

    public func listPrimaryKeys(
        database: String? = nil,
        schema: String? = nil,
        table: String? = nil
    ) -> EventLoopFuture<[KeyConstraintMetadata]> {
        var parameters: [String] = []

        if let table {
            parameters.append("@table_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'")
        }
        if let schema {
            parameters.append("@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        }
        if let database {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(database))'")
        }
        if parameters.isEmpty {
            parameters.append("@table_name = N'%'")
        }

        let sql = "EXEC sp_pkeys \(parameters.joined(separator: ", "));"

        return connection.rawSql(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, columns: [KeyColumnMetadata])] = [:]

            for row in rows {
                guard
                    let schemaName = row.column("TABLE_SCHEM")?.string,
                    let tableName = row.column("TABLE_NAME")?.string,
                    let columnName = row.column("COLUMN_NAME")?.string
                else {
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
                KeyConstraintMetadata(
                    schema: entry.schema,
                    table: entry.table,
                    name: entry.name,
                    type: .primaryKey,
                    isClustered: false,
                    columns: entry.columns.sorted { $0.ordinal < $1.ordinal }
                )
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
            is_clustered = i.is_clustered,
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

        return connection.rawSql(sql).map { rows in
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
                    isClustered: row.column("is_clustered")?.bool ?? false,
                    columns: []
                )

                let column = KeyColumnMetadata(
                    column: columnName,
                    ordinal: ordinal,
                    isDescending: row.column("is_descending")?.bool ?? false
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

    // MARK: - Indexes

    public func listIndexes(
        database: String? = nil,
        schema: String,
        table: String
    ) -> EventLoopFuture<[IndexMetadata]> {
        var parameters: [String] = [
            "@table_name = N'\(SQLServerMetadataClient.escapeLiteral(table))'",
            "@table_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'",
            "@index_name = N'%'",
            "@is_unique = 'N'",
            "@accuracy = 'Q'"
        ]
        if let database {
            parameters.append("@table_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(database))'")
        }

        let sql = "EXEC sp_statistics \(parameters.joined(separator: ", "));"

        return connection.rawSql(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, isUnique: Bool, columns: [IndexColumnMetadata])] = [:]

            for row in rows {
                guard
                    let schemaName = row.column("TABLE_SCHEM")?.string,
                    let tableName = row.column("TABLE_NAME")?.string,
                    let indexName = row.column("INDEX_NAME")?.string
                else {
                    continue
                }
                let key = "\(schemaName)|\(tableName)|\(indexName)"
                var entry = grouped[key] ?? (
                    schema: schemaName,
                    table: tableName,
                    name: indexName,
                    isUnique: (row.column("NON_UNIQUE")?.int ?? 1) == 0,
                    columns: []
                )

                if let columnName = row.column("COLUMN_NAME")?.string {
                    let ordinal = row.column("ORDINAL_POSITION")?.int ?? (entry.columns.count + 1)
                    let isDescending = row.column("ASC_OR_DESC")?.string?.uppercased() == "D"
                    let column = IndexColumnMetadata(
                        column: columnName,
                        ordinal: ordinal,
                        isDescending: isDescending,
                        isIncluded: false
                    )
                    entry.columns.append(column)
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
                IndexMetadata(
                    schema: entry.schema,
                    table: entry.table,
                    name: entry.name,
                    isUnique: entry.isUnique,
                    isClustered: false,
                    isPrimaryKey: false,
                    isUniqueConstraint: false,
                    filterDefinition: nil,
                    columns: entry.columns.sorted { $0.ordinal < $1.ordinal }
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

        return connection.rawSql(sql).map { rows in
            var grouped: [String: (schema: String, table: String, name: String, referencedSchema: String, referencedTable: String, deleteAction: Int, updateAction: Int, columns: [ForeignKeyColumnMetadata])] = [:]

            for row in rows {
                guard
                    let schemaName = row.column("FKTABLE_SCHEM")?.string,
                    let tableName = row.column("FKTABLE_NAME")?.string,
                    let fkName = row.column("FK_NAME")?.string,
                    let referencedSchema = row.column("PKTABLE_SCHEM")?.string,
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

        return connection.rawSql(sql).map { rows in
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

        return connection.rawSql(sql).map { rows in
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

        let groups = Dictionary(grouping: identifiers) { identifier -> String? in
            identifier.database
        }

        let futures = groups.map { (database, items) -> EventLoopFuture<[ObjectDefinition]> in
            let dbPrefix: String = {
                guard let database else { return "" }
                let escaped = SQLServerMetadataClient.escapeIdentifier(database)
                return "[\(escaped)]."
            }()

            let predicates = items.map { identifier -> String in
                let escapedSchema = SQLServerMetadataClient.escapeLiteral(identifier.schema)
                let escapedName = SQLServerMetadataClient.escapeLiteral(identifier.name)
                return "(s.name = N'\(escapedSchema)' AND o.name = N'\(escapedName)')"
            }.joined(separator: " OR ")

            let sql = """
            SELECT
                schema_name = s.name,
                object_name = o.name,
                type_desc = o.type_desc,
                definition = m.definition,
                is_ms_shipped = o.is_ms_shipped,
                create_date = o.create_date,
                modify_date = o.modify_date
            FROM \(dbPrefix)sys.objects AS o
            JOIN \(dbPrefix)sys.schemas AS s ON o.schema_id = s.schema_id
            LEFT JOIN \(dbPrefix)sys.sql_modules AS m ON o.object_id = m.object_id
            WHERE \(predicates)
            ORDER BY s.name, o.name
            """

            return self.connection.rawSql(sql).map { rows in
                rows.compactMap { row in
                    guard
                        let schemaName = row.column("schema_name")?.string,
                        let objectName = row.column("object_name")?.string,
                        let typeDesc = row.column("type_desc")?.string
                    else {
                        return nil
                    }

                    return ObjectDefinition(
                        schema: schemaName,
                        name: objectName,
                        type: ObjectDefinition.ObjectType.from(typeDesc: typeDesc),
                        definition: row.column("definition")?.string,
                        isSystemObject: row.column("is_ms_shipped")?.bool ?? false,
                        createDate: row.column("create_date")?.date,
                        modifyDate: row.column("modify_date")?.date
                    )
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

        return connection.rawSql(sql).flatMap { rows in
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
        var parameters: [String] = []
        if let qualifier = effectiveDatabase(database) {
            parameters.append("@sp_qualifier = N'\(SQLServerMetadataClient.escapeLiteral(qualifier))'")
        }
        if let schema {
            parameters.append("@sp_owner = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        }

        let sql = "EXEC sp_stored_procedures\(parameters.isEmpty ? "" : " " + parameters.joined(separator: ", "))"

        return connection.rawSql(sql).flatMap { rows in
            let functions: [RoutineMetadata] = rows.compactMap { row -> RoutineMetadata? in
                guard
                    let schemaName = row.column("PROCEDURE_OWNER")?.string,
                    let rawName = row.column("PROCEDURE_NAME")?.string,
                    let typeValue = row.column("PROCEDURE_TYPE")?.int
                else {
                    return nil
                }

                // PROCEDURE_TYPE: 1=procedure, 2=function
                guard typeValue == 2 else { return nil }

                if !self.configuration.includeSystemSchemas,
                   schemaName.caseInsensitiveCompare("sys") == .orderedSame ||
                   schemaName.caseInsensitiveCompare("INFORMATION_SCHEMA") == .orderedSame {
                    return nil
                }

                if let schema,
                   schemaName.caseInsensitiveCompare(schema) != .orderedSame {
                    return nil
                }

                let name = SQLServerMetadataClient.normalizeRoutineName(rawName)
                if name.hasPrefix("meta_client_") {
                    return nil
                }

                return RoutineMetadata(
                    schema: schemaName,
                    name: name,
                    type: .scalarFunction,
                    definition: nil,
                    isSystemObject: false
                )
            }

            guard !functions.isEmpty else {
                return self.connection.eventLoop.makeSucceededFuture([])
            }

            guard self.configuration.includeRoutineDefinitions else {
                return self.connection.eventLoop.makeSucceededFuture(functions)
            }

            let db = self.effectiveDatabase(database)
            let identifiers = functions.map { function in
                SQLServerMetadataObjectIdentifier(database: db, schema: function.schema, name: function.name, kind: .function)
            }

            return self.fetchObjectDefinitions(identifiers).map { definitions in
                let lookup = Dictionary(uniqueKeysWithValues: definitions.map { definition in
                    let key = "\(definition.schema.lowercased())|\(definition.name.lowercased())"
                    return (key, definition)
                })

                return functions.map { function in
                    let key = "\(function.schema.lowercased())|\(function.name.lowercased())"
                    guard let definition = lookup[key] else { return function }
                    let type: RoutineMetadata.RoutineType = definition.type == .tableFunction ? .tableFunction : .scalarFunction
                    return RoutineMetadata(
                        schema: function.schema,
                        name: function.name,
                        type: type,
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

        return connection.rawSql(sql).map { rows in
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
