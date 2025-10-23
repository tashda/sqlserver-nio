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
    public let type: String
}

public struct ColumnMetadata: Sendable {
    public let schema: String
    public let table: String
    public let name: String
    public let typeName: String
    public let maxLength: Int?
    public let precision: Int?
    public let scale: Int?
    public let isNullable: Bool
    public let isIdentity: Bool
    public let isComputed: Bool
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
    public struct Configuration {
        public var includeSystemSchemas: Bool
        public var enableColumnCache: Bool

        public init(includeSystemSchemas: Bool = false, enableColumnCache: Bool = true) {
            self.includeSystemSchemas = includeSystemSchemas
            self.enableColumnCache = enableColumnCache
        }
    }

    private let connection: TDSConnection
    private let cache: MetadataCache<[ColumnMetadata]>?
    private let configuration: Configuration

    public convenience init(
        connection: SQLServerConnection,
        configuration: Configuration = Configuration()
    ) {
        self.init(connection: connection.underlying, configuration: configuration, sharedCache: nil)
    }

    @available(*, deprecated, message: "Pass SQLServerConnection instead")
    public convenience init(
        connection: TDSConnection,
        configuration: Configuration = Configuration()
    ) {
        self.init(connection: connection, configuration: configuration, sharedCache: nil)
    }

    internal init(
        connection: TDSConnection,
        configuration: Configuration,
        sharedCache: MetadataCache<[ColumnMetadata]>?
    ) {
        self.connection = connection
        self.configuration = configuration
        if configuration.enableColumnCache {
            self.cache = sharedCache ?? MetadataCache<[ColumnMetadata]>()
        } else {
            self.cache = nil
        }
    }

    // MARK: - Databases

    public func listDatabases() -> EventLoopFuture<[DatabaseMetadata]> {
        let sql = """
        SELECT name
        FROM sys.databases
        WHERE state = 0
        ORDER BY name;
        """
        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return DatabaseMetadata(name: name)
            }
        }
    }

    // MARK: - Schemas

    public func listSchemas(in database: String? = nil) -> EventLoopFuture<[SchemaMetadata]> {
        var sql = """
        SELECT name
        FROM \(qualified(database, object: "sys.schemas"))
        """
        if !configuration.includeSystemSchemas {
            sql += " WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA')"
        }
        sql += " ORDER BY name;"
        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard let name = row.column("name")?.string else { return nil }
                return SchemaMetadata(name: name)
            }
        }
    }

    // MARK: - Tables

    public func listTables(database: String? = nil, schema: String? = nil) -> EventLoopFuture<[TableMetadata]> {
        var sql = """
        SELECT
            schema_name = s.name,
            table_name = o.name,
            table_type = o.type_desc
        FROM \(qualified(database, object: "sys.objects")) AS o
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON o.schema_id = s.schema_id
        WHERE o.type IN ('U', 'V') -- tables + views
        """
        var filters: [String] = []
        if let schema {
            filters.append("s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'")
        }
        if !configuration.includeSystemSchemas {
            filters.append("s.name NOT IN ('sys', 'INFORMATION_SCHEMA')")
        }
        if !filters.isEmpty {
            sql += " AND " + filters.joined(separator: " AND ")
        }
        sql += " ORDER BY s.name, o.name;"

        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let tableType = row.column("table_type")?.string
                else { return nil }
                return TableMetadata(schema: schemaName, name: tableName, type: tableType)
            }
        }
    }

    // MARK: - Columns

    public func listColumns(database: String? = nil, schema: String, table: String) -> EventLoopFuture<[ColumnMetadata]> {
        let cacheKey = "\(database ?? "").\(schema).\(table)"
        if let cache, let cached = cache.value(forKey: cacheKey) {
            return connection.eventLoop.makeSucceededFuture(cached)
        }

        let sql = """
        SELECT
            schema_name = s.name,
            table_name = t.name,
            column_name = c.name,
            type_name = ty.name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            c.is_computed
        FROM \(qualified(database, object: "sys.columns")) AS c
        JOIN \(qualified(database, object: "sys.tables")) AS t ON c.object_id = t.object_id
        JOIN \(qualified(database, object: "sys.schemas")) AS s ON t.schema_id = s.schema_id
        JOIN \(qualified(database, object: "sys.types")) AS ty ON c.user_type_id = ty.user_type_id
        WHERE s.name = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND t.name = N'\(SQLServerMetadataClient.escapeLiteral(table))'
        ORDER BY c.column_id;
        """

        return connection.rawSql(sql).flatMap { rows in
            let columns: [ColumnMetadata] = rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let columnName = row.column("column_name")?.string,
                    let typeName = row.column("type_name")?.string,
                    let isNullable = row.column("is_nullable")?.bool,
                    let isIdentity = row.column("is_identity")?.bool,
                    let isComputed = row.column("is_computed")?.bool
                else {
                    return nil
                }

                let maxLength = row.column("max_length")?.int
                let precision = row.column("precision")?.int
                let scale = row.column("scale")?.int
                return ColumnMetadata(
                    schema: schemaName,
                    table: tableName,
                    name: columnName,
                    typeName: typeName,
                    maxLength: maxLength,
                    precision: precision,
                    scale: scale,
                    isNullable: isNullable,
                    isIdentity: isIdentity,
                    isComputed: isComputed
                )
            }

            if !columns.isEmpty {
                if let cache = self.cache {
                    cache.setValue(columns, forKey: cacheKey)
                }
                return self.connection.eventLoop.makeSucceededFuture(columns)
            }

            self.connection.logger.info("Metadata:listColumns returned no rows from sys catalog; falling back to INFORMATION_SCHEMA for \(schema).\(table)")
            return self.fetchColumnsFromInformationSchema(database: database, schema: schema, table: table).map { fallback in
                if let cache = self.cache {
                    cache.setValue(fallback, forKey: cacheKey)
                }
                return fallback
            }
        }
    }

    private func fetchColumnsFromInformationSchema(database: String?, schema: String, table: String) -> EventLoopFuture<[ColumnMetadata]> {
        let sql = """
        SELECT
            schema_name = c.TABLE_SCHEMA,
            table_name = c.TABLE_NAME,
            column_name = c.COLUMN_NAME,
            type_name = c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE
        FROM \(qualified(database, object: "INFORMATION_SCHEMA.COLUMNS")) AS c
        WHERE c.TABLE_SCHEMA = N'\(SQLServerMetadataClient.escapeLiteral(schema))'
          AND c.TABLE_NAME = N'\(SQLServerMetadataClient.escapeLiteral(table))'
        ORDER BY c.ORDINAL_POSITION;
        """

        return connection.rawSql(sql).map { rows in
            rows.compactMap { row in
                guard
                    let schemaName = row.column("schema_name")?.string,
                    let tableName = row.column("table_name")?.string,
                    let columnName = row.column("column_name")?.string,
                    let typeName = row.column("type_name")?.string
                else {
                    return nil
                }
                let nullableValue = row.column("IS_NULLABLE")?.string ?? "YES"
                let maxLength = row.column("CHARACTER_MAXIMUM_LENGTH")?.int
                let precision = row.column("NUMERIC_PRECISION")?.int
                let scale = row.column("NUMERIC_SCALE")?.int

                return ColumnMetadata(
                    schema: schemaName,
                    table: tableName,
                    name: columnName,
                    typeName: typeName,
                    maxLength: maxLength,
                    precision: precision,
                    scale: scale,
                    isNullable: nullableValue.uppercased() == "YES",
                    isIdentity: false,
                    isComputed: false
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

    private func qualified(_ database: String?, object: String) -> String {
        if let database {
            return "[\(SQLServerMetadataClient.escapeIdentifier(database))].\(object)"
        } else {
            return object
        }
    }
}
