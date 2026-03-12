import NIO
import SQLServerTDS

// MARK: - SQLServerIndexClient

public final class SQLServerIndexClient: @unchecked Sendable {
    internal let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
    // MARK: - Index Creation
    
    internal func dropIndexIfExistsSQL(name: String, table: String, schema: String) -> String {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        return """
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = N'\(name.replacingOccurrences(of: "'", with: "''"))'
              AND object_id = OBJECT_ID(N'\(fullTableName.replacingOccurrences(of: "'", with: "''"))')
        )
        DROP INDEX \(escapedIndexName) ON \(fullTableName);
        """
    }

    public func createIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil,
        filter: String? = nil,
        dropIfExists: Bool = false
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createIndex(name: name, table: table, columns: columns, schema: schema, options: options, filter: filter, dropIfExists: dropIfExists)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil,
        filter: String? = nil,
        dropIfExists: Bool = false
    ) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        if dropIfExists {
            let dropSql = dropIndexIfExistsSQL(name: name, table: table, schema: schema)
            _ = try await client.execute(dropSql)
        }
        
        let keyColumns = columns.filter { !$0.isIncluded }
        let includedColumns = columns.filter { $0.isIncluded }
        
        guard !keyColumns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one key column is required")
        }
        
        var sql = "CREATE NONCLUSTERED INDEX \(escapedIndexName) ON \(fullTableName)"
        
        let keyColumnList = keyColumns.map { column in
            "\(Self.escapeIdentifier(column.name)) \(column.sortDirection.rawValue)"
        }.joined(separator: ", ")
        sql += " (\(keyColumnList))"
        
        if !includedColumns.isEmpty {
            let includedColumnList = includedColumns.map { Self.escapeIdentifier($0.name) }.joined(separator: ", ")
            sql += " INCLUDE (\(includedColumnList))"
        }
        if let f = filter, !f.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            sql += " WHERE \(f)"
        }

        if let options = options {
            sql += try applyOptions(options)
        }
        
        _ = try await client.execute(sql)
    }
    
    public func createUniqueIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil,
        filter: String? = nil,
        dropIfExists: Bool = false
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createUniqueIndex(name: name, table: table, columns: columns, schema: schema, options: options, filter: filter, dropIfExists: dropIfExists)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func createUniqueIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil,
        filter: String? = nil,
        dropIfExists: Bool = false
    ) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        if dropIfExists {
            let dropSql = dropIndexIfExistsSQL(name: name, table: table, schema: schema)
            _ = try await client.execute(dropSql)
        }
        
        let keyColumns = columns.filter { !$0.isIncluded }
        let includedColumns = columns.filter { $0.isIncluded }
        
        guard !keyColumns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one key column is required")
        }
        
        var sql = "CREATE UNIQUE NONCLUSTERED INDEX \(escapedIndexName) ON \(fullTableName)"
        
        let keyColumnList = keyColumns.map { column in
            "\(Self.escapeIdentifier(column.name)) \(column.sortDirection.rawValue)"
        }.joined(separator: ", ")
        sql += " (\(keyColumnList))"
        
        if !includedColumns.isEmpty {
            let includedColumnList = includedColumns.map { Self.escapeIdentifier($0.name) }.joined(separator: ", ")
            sql += " INCLUDE (\(includedColumnList))"
        }
        if let f = filter, !f.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            sql += " WHERE \(f)"
        }

        if let options = options {
            sql += try applyOptions(options)
        }
        
        _ = try await client.execute(sql)
    }

    @available(macOS 12.0, *)
    public func createColumnstoreIndex(
        name: String,
        table: String,
        clustered: Bool,
        columns: [String] = [],
        schema: String = "dbo",
        dropIfExists: Bool = false
    ) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"

        if dropIfExists {
            let dropSql = dropIndexIfExistsSQL(name: name, table: table, schema: schema)
            _ = try await client.execute(dropSql)
        }

        let kind = clustered ? "CLUSTERED COLUMNSTORE" : "NONCLUSTERED COLUMNSTORE"
        var sql = "CREATE \(kind) INDEX \(escapedIndexName) ON \(fullTableName)"
        if !clustered && !columns.isEmpty {
            let list = columns.map { Self.escapeIdentifier($0) }.joined(separator: ", ")
            sql += " (\(list))"
        }
        sql += ";"
        _ = try await client.execute(sql)
    }
    
    @available(macOS 12.0, *)
    public func createClusteredIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil
    ) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let keyColumns = columns.filter { !$0.isIncluded }
        
        guard !keyColumns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one key column is required")
        }
        
        if columns.contains(where: { $0.isIncluded }) {
            throw SQLServerError.invalidArgument("Clustered indexes cannot have included columns")
        }
        
        var sql = "CREATE CLUSTERED INDEX \(escapedIndexName) ON \(fullTableName)"
        
        let keyColumnList = keyColumns.map { column in
            "\(Self.escapeIdentifier(column.name)) \(column.sortDirection.rawValue)"
        }.joined(separator: ", ")
        sql += " (\(keyColumnList))"
        
        if let options = options {
            sql += try applyOptions(options)
        }
        
        _ = try await client.execute(sql)
    }

    internal func applyOptions(_ options: IndexOptions) throws -> String {
        var sql = ""
        var optionParts: [String] = []
        
        if let fillFactor = options.fillFactor {
            optionParts.append("FILLFACTOR = \(fillFactor)")
        }
        if options.padIndex {
            optionParts.append("PAD_INDEX = ON")
        }
        if options.ignoreDuplicateKey {
            optionParts.append("IGNORE_DUP_KEY = ON")
        }
        if options.statisticsNoRecompute {
            optionParts.append("STATISTICS_NORECOMPUTE = ON")
        }
        if !options.allowRowLocks {
            optionParts.append("ALLOW_ROW_LOCKS = OFF")
        }
        if !options.allowPageLocks {
            optionParts.append("ALLOW_PAGE_LOCKS = OFF")
        }
        if options.online {
            optionParts.append("ONLINE = ON")
        }
        if let maxDop = options.maxDop {
            optionParts.append("MAXDOP = \(maxDop)")
        }
        if let compression = options.dataCompression {
            optionParts.append("DATA_COMPRESSION = \(compression.rawValue)")
        }
        
        if !optionParts.isEmpty {
            sql += " WITH (\(optionParts.joined(separator: ", ")))"
        }
        
        if let partitionScheme = options.partitionScheme {
            let escapedPartitionScheme = Self.escapeIdentifier(partitionScheme)
            if options.partitionColumns.isEmpty {
                sql += " ON \(escapedPartitionScheme)"
            } else {
                let partitionColumns = options.partitionColumns
                    .map(Self.escapeIdentifier)
                    .joined(separator: ", ")
                sql += " ON \(escapedPartitionScheme)(\(partitionColumns))"
            }
        } else if let fileGroup = options.fileGroup {
            sql += " ON \(Self.escapeIdentifier(fileGroup))"
        }
        return sql
    }
    
    internal static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}
