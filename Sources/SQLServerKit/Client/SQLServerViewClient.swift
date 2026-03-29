import NIO
import SQLServerTDS

// MARK: - View Options

public struct ViewOptions: Sendable {
    @available(*, deprecated, message: "Pass schema as a direct parameter instead")
    public var schemaValue: String { schema }
    internal let schema: String
    public let withEncryption: Bool
    public let withSchemaBinding: Bool
    public let withViewMetadata: Bool
    public let withCheckOption: Bool

    public init(
        schema: String = "dbo",
        withEncryption: Bool = false,
        withSchemaBinding: Bool = false,
        withViewMetadata: Bool = false,
        withCheckOption: Bool = false
    ) {
        self.schema = schema
        self.withEncryption = withEncryption
        self.withSchemaBinding = withSchemaBinding
        self.withViewMetadata = withViewMetadata
        self.withCheckOption = withCheckOption
    }
}

// MARK: - SQLServerViewClient

public final class SQLServerViewClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - View Management

    @discardableResult
    public func createView(
        name: String,
        query: String,
        schema: String = "dbo",
        options: ViewOptions = ViewOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createView(name: name, query: query, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func createView(
        name: String,
        query: String,
        schema: String = "dbo",
        options: ViewOptions = ViewOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "CREATE VIEW \(fullName)"

        // Add options
        var optionParts: [String] = []
        if options.withEncryption {
            optionParts.append("ENCRYPTION")
        }
        if options.withSchemaBinding {
            optionParts.append("SCHEMABINDING")
        }
        if options.withViewMetadata {
            optionParts.append("VIEW_METADATA")
        }

        if !optionParts.isEmpty {
            sql += "\nWITH \(optionParts.joined(separator: ", "))"
        }

        sql += "\nAS\n\(query)"

        if options.withCheckOption {
            sql += "\nWITH CHECK OPTION"
        }

        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    internal func dropView(name: String, schema: String = "dbo") -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropView(name: name, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func dropView(name: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        let sql = "DROP VIEW \(fullName)"
        let result = try await client.execute(sql)
        return result.messages
    }

    @discardableResult
    public func alterView(
        name: String,
        query: String,
        schema: String = "dbo",
        options: ViewOptions = ViewOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterView(name: name, query: query, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func alterView(
        name: String,
        query: String,
        schema: String = "dbo",
        options: ViewOptions = ViewOptions()
    ) async throws -> [SQLServerStreamMessage] {
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        var sql = "ALTER VIEW \(fullName)"

        // Add options
        var optionParts: [String] = []
        if options.withEncryption {
            optionParts.append("ENCRYPTION")
        }
        if options.withSchemaBinding {
            optionParts.append("SCHEMABINDING")
        }
        if options.withViewMetadata {
            optionParts.append("VIEW_METADATA")
        }

        if !optionParts.isEmpty {
            sql += "\nWITH \(optionParts.joined(separator: ", "))"
        }

        sql += "\nAS\n\(query)"

        if options.withCheckOption {
            sql += "\nWITH CHECK OPTION"
        }

        let result = try await client.execute(sql)
        return result.messages
    }

    // MARK: - Indexed Views (Materialized Views)

    @discardableResult
    public func createIndexedView(
        name: String,
        query: String,
        indexName: String,
        indexColumns: [String],
        schema: String = "dbo",
        options: ViewOptions = ViewOptions()
    ) -> EventLoopFuture<[SQLServerStreamMessage]> {
        let promise = client.eventLoopGroup.next().makePromise(of: [SQLServerStreamMessage].self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createIndexedView(name: name, query: query, indexName: indexName, indexColumns: indexColumns, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func createIndexedView(
        name: String,
        query: String,
        indexName: String,
        indexColumns: [String],
        schema: String = "dbo",
        options: ViewOptions = ViewOptions()
    ) async throws -> [SQLServerStreamMessage] {
        // First create the view with SCHEMABINDING (required for indexed views)
        let viewOptions = ViewOptions(
            schema: schema,
            withEncryption: options.withEncryption,
            withSchemaBinding: true, // Required for indexed views
            withViewMetadata: options.withViewMetadata,
            withCheckOption: options.withCheckOption
        )

        var allMessages = try await createView(name: name, query: query, schema: schema, options: viewOptions)

        // Then create the clustered index
        let escapedViewName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullViewName = "\(schemaPrefix)\(escapedViewName)"
        let escapedIndexName = SQLServerSQL.escapeIdentifier(indexName)

        let columnList = indexColumns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")
        let indexSql = """
        CREATE UNIQUE CLUSTERED INDEX \(escapedIndexName)
        ON \(fullViewName) (\(columnList))
        """

        let indexResult = try await client.execute(indexSql)
        allMessages.append(contentsOf: indexResult.messages)
        return allMessages
    }

    // MARK: - List Views

    /// List views in the given database and schema by filtering table metadata.
    @available(macOS 12.0, *)
    public func listViews(database: String? = nil, schema: String? = nil) async throws -> [TableMetadata] {
        try await client.metadata.listTables(database: database, schema: schema, includeComments: false)
            .filter { $0.kind == .view }
    }

    // MARK: - Utility Methods

    @available(macOS 12.0, *)
    public func viewExists(name: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.views v
        INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE v.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """

        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }

    @available(macOS 12.0, *)
    public func getViewDefinition(name: String, schema: String = "dbo") async throws -> String? {
        let sql = """
        SELECT m.definition
        FROM sys.views v
        INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE v.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """

        let result = try await client.queryScalar(sql, as: String.self)
        return result
    }

    @available(macOS 12.0, *)
    public func isIndexedView(name: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.views v
        INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON v.object_id = i.object_id
        WHERE v.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        AND i.type = 1 -- Clustered index
        """

        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }

    @available(macOS 12.0, *)
    @discardableResult
    public func refreshIndexedView(name: String, schema: String = "dbo") async throws -> [SQLServerStreamMessage] {
        // In SQL Server, indexed views are automatically maintained
        // This method could be used for statistics updates or other maintenance
        let escapedName = SQLServerSQL.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(SQLServerSQL.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"

        let sql = "UPDATE STATISTICS \(fullName)"
        let result = try await client.execute(sql)
        return result.messages
    }
}
