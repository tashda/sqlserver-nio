import NIO
import SQLServerTDS

// MARK: - View Options

public struct ViewOptions: Sendable {
    public let schema: String
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

public final class SQLServerViewClient {
    private let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
    // MARK: - View Management
    
    public func createView(
        name: String,
        query: String,
        options: ViewOptions = ViewOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createView(name: name, query: query, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createView(
        name: String,
        query: String,
        options: ViewOptions = ViewOptions()
    ) async throws {
        let escapedName = Self.escapeIdentifier(name)
        let schemaPrefix = options.schema != "dbo" ? "\(Self.escapeIdentifier(options.schema))." : ""
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
        
        _ = try await client.execute(sql)
    }
    
    public func dropView(name: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
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
    public func dropView(name: String, schema: String = "dbo") async throws {
        let escapedName = Self.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"
        
        let sql = "DROP VIEW \(fullName)"
        _ = try await client.execute(sql)
    }
    
    public func alterView(
        name: String,
        query: String,
        options: ViewOptions = ViewOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.alterView(name: name, query: query, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func alterView(
        name: String,
        query: String,
        options: ViewOptions = ViewOptions()
    ) async throws {
        let escapedName = Self.escapeIdentifier(name)
        let schemaPrefix = options.schema != "dbo" ? "\(Self.escapeIdentifier(options.schema))." : ""
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
        
        _ = try await client.execute(sql)
    }
    
    // MARK: - Indexed Views (Materialized Views)
    
    public func createIndexedView(
        name: String,
        query: String,
        indexName: String,
        indexColumns: [String],
        options: ViewOptions = ViewOptions()
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createIndexedView(name: name, query: query, indexName: indexName, indexColumns: indexColumns, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createIndexedView(
        name: String,
        query: String,
        indexName: String,
        indexColumns: [String],
        options: ViewOptions = ViewOptions()
    ) async throws {
        // First create the view with SCHEMABINDING (required for indexed views)
        var viewOptions = options
        viewOptions = ViewOptions(
            schema: options.schema,
            withEncryption: options.withEncryption,
            withSchemaBinding: true, // Required for indexed views
            withViewMetadata: options.withViewMetadata,
            withCheckOption: options.withCheckOption
        )
        
        try await createView(name: name, query: query, options: viewOptions)
        
        // Then create the clustered index
        let escapedViewName = Self.escapeIdentifier(name)
        let schemaPrefix = options.schema != "dbo" ? "\(Self.escapeIdentifier(options.schema))." : ""
        let fullViewName = "\(schemaPrefix)\(escapedViewName)"
        let escapedIndexName = Self.escapeIdentifier(indexName)
        
        let columnList = indexColumns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
        let indexSql = """
        CREATE UNIQUE CLUSTERED INDEX \(escapedIndexName)
        ON \(fullViewName) (\(columnList))
        """
        
        _ = try await client.execute(indexSql)
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
    public func refreshIndexedView(name: String, schema: String = "dbo") async throws {
        // In SQL Server, indexed views are automatically maintained
        // This method could be used for statistics updates or other maintenance
        let escapedName = Self.escapeIdentifier(name)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullName = "\(schemaPrefix)\(escapedName)"
        
        let sql = "UPDATE STATISTICS \(fullName)"
        _ = try await client.execute(sql)
    }
    
    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}