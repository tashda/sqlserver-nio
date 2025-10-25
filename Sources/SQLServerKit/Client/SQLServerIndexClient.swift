import NIO
import SQLServerTDS

// MARK: - Index Types

public struct IndexColumn: Sendable {
    public let name: String
    public let sortDirection: SortDirection
    public let isIncluded: Bool
    
    public init(name: String, sortDirection: SortDirection = .ascending, isIncluded: Bool = false) {
        self.name = name
        self.sortDirection = sortDirection
        self.isIncluded = isIncluded
    }
    
    public enum SortDirection: String, Sendable {
        case ascending = "ASC"
        case descending = "DESC"
    }
}

public struct IndexOptions: Sendable {
    public let fillFactor: Int?
    public let padIndex: Bool
    public let ignoreDuplicateKey: Bool
    public let statisticsNoRecompute: Bool
    public let allowRowLocks: Bool
    public let allowPageLocks: Bool
    public let online: Bool
    public let maxDop: Int?
    public let dataCompression: DataCompression?
    public let fileGroup: String?
    
    public init(
        fillFactor: Int? = nil,
        padIndex: Bool = false,
        ignoreDuplicateKey: Bool = false,
        statisticsNoRecompute: Bool = false,
        allowRowLocks: Bool = true,
        allowPageLocks: Bool = true,
        online: Bool = false,
        maxDop: Int? = nil,
        dataCompression: DataCompression? = nil,
        fileGroup: String? = nil
    ) {
        self.fillFactor = fillFactor
        self.padIndex = padIndex
        self.ignoreDuplicateKey = ignoreDuplicateKey
        self.statisticsNoRecompute = statisticsNoRecompute
        self.allowRowLocks = allowRowLocks
        self.allowPageLocks = allowPageLocks
        self.online = online
        self.maxDop = maxDop
        self.dataCompression = dataCompression
        self.fileGroup = fileGroup
    }
    
    public enum DataCompression: String, Sendable {
        case none = "NONE"
        case row = "ROW"
        case page = "PAGE"
    }
}

public struct IndexInfo: Sendable {
    public let name: String
    public let tableName: String
    public let schemaName: String
    public let indexType: IndexType
    public let isUnique: Bool
    public let isPrimaryKey: Bool
    public let columns: [IndexColumnInfo]
    
    public enum IndexType: String, Sendable {
        case clustered = "CLUSTERED"
        case nonclustered = "NONCLUSTERED"
        case heap = "HEAP"
    }
}

public struct IndexColumnInfo: Sendable {
    public let name: String
    public let keyOrdinal: Int
    public let isDescending: Bool
    public let isIncluded: Bool
}

// MARK: - SQLServerIndexClient

public final class SQLServerIndexClient {
    private let client: SQLServerClient
    
    public init(client: SQLServerClient) {
        self.client = client
    }
    
    // MARK: - Index Creation
    
    public func createIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createIndex(name: name, table: table, columns: columns, schema: schema, options: options)
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
        options: IndexOptions? = nil
    ) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        // Separate key columns from included columns
        let keyColumns = columns.filter { !$0.isIncluded }
        let includedColumns = columns.filter { $0.isIncluded }
        
        guard !keyColumns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one key column is required")
        }
        
        var sql = "CREATE NONCLUSTERED INDEX \(escapedIndexName) ON \(fullTableName)"
        
        // Add key columns
        let keyColumnList = keyColumns.map { column in
            "\(Self.escapeIdentifier(column.name)) \(column.sortDirection.rawValue)"
        }.joined(separator: ", ")
        sql += " (\(keyColumnList))"
        
        // Add included columns if any
        if !includedColumns.isEmpty {
            let includedColumnList = includedColumns.map { Self.escapeIdentifier($0.name) }.joined(separator: ", ")
            sql += " INCLUDE (\(includedColumnList))"
        }
        
        // Add options
        if let options = options {
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
            
            if let fileGroup = options.fileGroup {
                sql += " ON \(Self.escapeIdentifier(fileGroup))"
            }
        }
        
        _ = try await client.execute(sql)
    }
    
    public func createUniqueIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createUniqueIndex(name: name, table: table, columns: columns, schema: schema, options: options)
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
        options: IndexOptions? = nil
    ) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        // Separate key columns from included columns
        let keyColumns = columns.filter { !$0.isIncluded }
        let includedColumns = columns.filter { $0.isIncluded }
        
        guard !keyColumns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one key column is required")
        }
        
        var sql = "CREATE UNIQUE NONCLUSTERED INDEX \(escapedIndexName) ON \(fullTableName)"
        
        // Add key columns
        let keyColumnList = keyColumns.map { column in
            "\(Self.escapeIdentifier(column.name)) \(column.sortDirection.rawValue)"
        }.joined(separator: ", ")
        sql += " (\(keyColumnList))"
        
        // Add included columns if any
        if !includedColumns.isEmpty {
            let includedColumnList = includedColumns.map { Self.escapeIdentifier($0.name) }.joined(separator: ", ")
            sql += " INCLUDE (\(includedColumnList))"
        }
        
        // Add options (same as regular index)
        if let options = options {
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
            
            if let fileGroup = options.fileGroup {
                sql += " ON \(Self.escapeIdentifier(fileGroup))"
            }
        }
        
        _ = try await client.execute(sql)
    }
    
    public func createClusteredIndex(
        name: String,
        table: String,
        columns: [IndexColumn],
        schema: String = "dbo",
        options: IndexOptions? = nil
    ) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.createClusteredIndex(name: name, table: table, columns: columns, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
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
        
        // Clustered indexes cannot have included columns
        let keyColumns = columns.filter { !$0.isIncluded }
        
        guard !keyColumns.isEmpty else {
            throw SQLServerError.invalidArgument("At least one key column is required")
        }
        
        if columns.contains(where: { $0.isIncluded }) {
            throw SQLServerError.invalidArgument("Clustered indexes cannot have included columns")
        }
        
        var sql = "CREATE CLUSTERED INDEX \(escapedIndexName) ON \(fullTableName)"
        
        // Add key columns
        let keyColumnList = keyColumns.map { column in
            "\(Self.escapeIdentifier(column.name)) \(column.sortDirection.rawValue)"
        }.joined(separator: ", ")
        sql += " (\(keyColumnList))"
        
        // Add options
        if let options = options {
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
            
            if let fileGroup = options.fileGroup {
                sql += " ON \(Self.escapeIdentifier(fileGroup))"
            }
        }
        
        _ = try await client.execute(sql)
    }
    
    // MARK: - Index Management
    
    public func dropIndex(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.dropIndex(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func dropIndex(name: String, table: String, schema: String = "dbo") async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "DROP INDEX \(escapedIndexName) ON \(fullTableName)"
        _ = try await client.execute(sql)
    }
    
    public func rebuildIndex(name: String, table: String, schema: String = "dbo", options: IndexOptions? = nil) -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.rebuildIndex(name: name, table: table, schema: schema, options: options)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func rebuildIndex(name: String, table: String, schema: String = "dbo", options: IndexOptions? = nil) async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        var sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) REBUILD"
        
        // Add rebuild options
        if let options = options {
            var optionParts: [String] = []
            
            if let fillFactor = options.fillFactor {
                optionParts.append("FILLFACTOR = \(fillFactor)")
            }
            if options.padIndex {
                optionParts.append("PAD_INDEX = ON")
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
        }
        
        _ = try await client.execute(sql)
    }
    
    public func reorganizeIndex(name: String, table: String, schema: String = "dbo") -> EventLoopFuture<Void> {
        let promise = client.eventLoopGroup.next().makePromise(of: Void.self)
        if #available(macOS 12.0, *) {
            promise.completeWithTask {
                try await self.reorganizeIndex(name: name, table: table, schema: schema)
            }
        } else {
            promise.fail(SQLServerError.unsupportedPlatform)
        }
        return promise.futureResult
    }
    
    @available(macOS 12.0, *)
    public func reorganizeIndex(name: String, table: String, schema: String = "dbo") async throws {
        let escapedIndexName = Self.escapeIdentifier(name)
        let escapedTableName = Self.escapeIdentifier(table)
        let schemaPrefix = schema != "dbo" ? "\(Self.escapeIdentifier(schema))." : ""
        let fullTableName = "\(schemaPrefix)\(escapedTableName)"
        
        let sql = "ALTER INDEX \(escapedIndexName) ON \(fullTableName) REORGANIZE"
        _ = try await client.execute(sql)
    }
    
    // MARK: - Index Information
    
    @available(macOS 12.0, *)
    public func indexExists(name: String, table: String, schema: String = "dbo") async throws -> Bool {
        let sql = """
        SELECT COUNT(*) as count
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE i.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        """
        
        let result = try await client.queryScalar(sql, as: Int.self)
        return (result ?? 0) > 0
    }
    
    @available(macOS 12.0, *)
    public func getIndexInfo(name: String, table: String, schema: String = "dbo") async throws -> IndexInfo? {
        let sql = """
        SELECT 
            i.name as index_name,
            o.name as table_name,
            s.name as schema_name,
            i.type_desc as index_type,
            i.is_unique,
            i.is_primary_key,
            ic.key_ordinal,
            c.name as column_name,
            ic.is_descending_key,
            ic.is_included_column
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.name = '\(name.replacingOccurrences(of: "'", with: "''"))'
        AND o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        ORDER BY ic.key_ordinal, ic.index_column_id
        """
        
        let rows = try await client.query(sql)
        guard !rows.isEmpty else { return nil }
        
        let firstRow = rows[0]
        let indexName = firstRow.column("index_name")?.string ?? ""
        let tableName = firstRow.column("table_name")?.string ?? ""
        let schemaName = firstRow.column("schema_name")?.string ?? ""
        let indexTypeString = firstRow.column("index_type")?.string ?? ""
        let isUnique = (firstRow.column("is_unique")?.int ?? 0) != 0
        let isPrimaryKey = (firstRow.column("is_primary_key")?.int ?? 0) != 0
        
        let indexType: IndexInfo.IndexType
        switch indexTypeString {
        case "CLUSTERED":
            indexType = .clustered
        case "NONCLUSTERED":
            indexType = .nonclustered
        case "HEAP":
            indexType = .heap
        default:
            indexType = .nonclustered
        }
        
        let columns = rows.map { row in
            IndexColumnInfo(
                name: row.column("column_name")?.string ?? "",
                keyOrdinal: row.column("key_ordinal")?.int ?? 0,
                isDescending: (row.column("is_descending_key")?.int ?? 0) != 0,
                isIncluded: (row.column("is_included_column")?.int ?? 0) != 0
            )
        }
        
        return IndexInfo(
            name: indexName,
            tableName: tableName,
            schemaName: schemaName,
            indexType: indexType,
            isUnique: isUnique,
            isPrimaryKey: isPrimaryKey,
            columns: columns
        )
    }
    
    @available(macOS 12.0, *)
    public func listTableIndexes(table: String, schema: String = "dbo") async throws -> [IndexInfo] {
        let sql = """
        SELECT DISTINCT
            i.name as index_name,
            o.name as table_name,
            s.name as schema_name,
            i.type_desc as index_type,
            i.is_unique,
            i.is_primary_key
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.name = '\(table.replacingOccurrences(of: "'", with: "''"))'
        AND s.name = '\(schema.replacingOccurrences(of: "'", with: "''"))'
        AND i.name IS NOT NULL
        ORDER BY i.name
        """
        
        let rows = try await client.query(sql)
        var indexes: [IndexInfo] = []
        
        for row in rows {
            let indexName = row.column("index_name")?.string ?? ""
            if let indexInfo = try await getIndexInfo(name: indexName, table: table, schema: schema) {
                indexes.append(indexInfo)
            }
        }
        
        return indexes
    }
    
    private static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }
}