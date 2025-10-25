import Foundation
import Logging

public enum SQLServerBulkCopyError: Error {
    case columnCountMismatch(expected: Int, actual: Int)
}

public struct SQLServerBulkCopyRow: Sendable {
    public var values: [SQLServerLiteralValue]
    
    public init(values: [SQLServerLiteralValue]) {
        self.values = values
    }
}

public struct SQLServerBulkCopyOptions: Sendable {
    public var schema: String
    public var table: String
    public var columns: [String]
    public var batchSize: Int
    public var identityInsert: Bool
    
    public init(
        table: String,
        schema: String = "dbo",
        columns: [String],
        batchSize: Int = 1_000,
        identityInsert: Bool = false
    ) {
        self.schema = schema
        self.table = table
        self.columns = columns
        self.batchSize = max(1, batchSize)
        self.identityInsert = identityInsert
    }
    
    internal var qualifiedTableName: String {
        let escapedSchema = SQLServerBulkCopyClient.escapeIdentifier(schema)
        let escapedTable = SQLServerBulkCopyClient.escapeIdentifier(table)
        return "[\(escapedSchema)].[\(escapedTable)]"
    }
    
    internal var columnList: String {
        columns.map { "[\(SQLServerBulkCopyClient.escapeIdentifier($0))]" }.joined(separator: ", ")
    }
}

public struct SQLServerBulkCopySummary: Sendable {
    public let schema: String
    public let table: String
    public let totalRows: Int
    public let batchesExecuted: Int
    public let batchSize: Int
    public let identityInsert: Bool
    public let duration: TimeInterval
}

public final class SQLServerBulkCopyClient {
    private let client: SQLServerClient
    private let logger: Logger
    
    public init(client: SQLServerClient, logger: Logger? = nil) {
        self.client = client
        self.logger = logger ?? client.logger
    }
    
    @available(macOS 12.0, *)
    public func copy(
        rows: [SQLServerBulkCopyRow],
        options: SQLServerBulkCopyOptions,
        afterBatch: ((SQLServerConnection, Int) async throws -> Void)? = nil
    ) async throws -> SQLServerBulkCopySummary {
        guard !rows.isEmpty else {
            logger.debug("SQLServerBulkCopyClient skipping copy because no rows were provided.")
            return SQLServerBulkCopySummary(
                schema: options.schema,
                table: options.table,
                totalRows: 0,
                batchesExecuted: 0,
                batchSize: options.batchSize,
                identityInsert: options.identityInsert,
                duration: 0
            )
        }
        
        let expectedColumnCount = options.columns.count
        guard expectedColumnCount > 0 else {
            logger.warning("SQLServerBulkCopyClient copy invoked without columns; nothing to do.")
            return SQLServerBulkCopySummary(
                schema: options.schema,
                table: options.table,
                totalRows: 0,
                batchesExecuted: 0,
                batchSize: options.batchSize,
                identityInsert: options.identityInsert,
                duration: 0
            )
        }
        
        for row in rows where row.values.count != expectedColumnCount {
            throw SQLServerBulkCopyError.columnCountMismatch(expected: expectedColumnCount, actual: row.values.count)
        }
        
        let start = Date()
        var batchesExecuted = 0
        var insertedRows: Int = 0
        
        try await client.withConnection { connection in
            for chunk in rows.chunked(into: options.batchSize) {
                batchesExecuted += 1
                let valuesClause = chunk.map { row in
                    let literals = row.values.map { $0.sqlLiteral() }.joined(separator: ", ")
                    return "(\(literals))"
                }.joined(separator: ",\n")
                
                var statement = """
                INSERT INTO \(options.qualifiedTableName) (\(options.columnList))
                VALUES
                \(valuesClause);
                """
                
                if options.identityInsert {
                    statement = """
                    SET IDENTITY_INSERT \(options.qualifiedTableName) ON;
                    \(statement)
                    SET IDENTITY_INSERT \(options.qualifiedTableName) OFF;
                    """
                }
                
                let result = try await connection.execute(statement)
                if let rowCount = result.rowCount, rowCount > 0 {
                    insertedRows += Int(rowCount)
                } else if result.totalRowCount > 0 {
                    insertedRows += Int(result.totalRowCount)
                } else {
                    insertedRows += chunk.count
                }
                
                if let afterBatch {
                    try await afterBatch(connection, batchesExecuted)
                }
            }
        }
        
        let duration = Date().timeIntervalSince(start)
        return SQLServerBulkCopySummary(
            schema: options.schema,
            table: options.table,
            totalRows: insertedRows,
            batchesExecuted: batchesExecuted,
            batchSize: options.batchSize,
            identityInsert: options.identityInsert,
            duration: duration
        )
    }
    
    static func escapeIdentifier(_ identifier: String) -> String {
        identifier.replacingOccurrences(of: "]", with: "]]")
    }
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        guard size > 0 else { return [self] }
        var start = 0
        var result: [[Element]] = []
        while start < count {
            let end = Swift.min(start + size, count)
            result.append(Array(self[start..<end]))
            start = end
        }
        return result
    }
}
