import Foundation
import Logging
import NIOConcurrencyHelpers

public enum SQLServerBulkCopyError: Error, LocalizedError {
    case columnCountMismatch(expected: Int, actual: Int)
    /// A value that cannot be converted to its destination column. `row` counts from 1.
    case invalidValue(row: Int, column: String, value: String, reason: String)

    public var errorDescription: String? {
        switch self {
        case .columnCountMismatch(let expected, let actual):
            return "A row has \(actual) values for \(expected) columns."
        case .invalidValue(let row, let column, let value, let reason):
            let shown = value.count > 60 ? String(value.prefix(60)) + "…" : value
            return "Row \(row), column \(column): '\(shown)' \(reason)."
        }
    }
}

public struct SQLServerBulkCopyRow: Sendable {
    public var values: [SQLServerLiteralValue]
    
    public init(values: [SQLServerLiteralValue]) {
        self.values = values
    }
}

/// How rows reach the server.
public enum SQLServerBulkCopyMethod: String, Sendable {
    /// The TDS bulk load (what `bcp` and SqlBulkCopy use): rows are streamed in the columns' own
    /// format, without SQL statements.
    case bulkLoad
    /// Multi-row `INSERT … VALUES` statements.
    case insertStatements
}

public struct SQLServerBulkCopyOptions: Sendable {
    public var schema: String
    public var table: String
    public var columns: [String]
    public var batchSize: Int
    /// Keep the identity values from the source. When false, a bulk load leaves identity columns
    /// out and the server assigns the values; INSERT statements fail on them.
    public var identityInsert: Bool
    /// `.bulkLoad` (the default) falls back to INSERT statements when a column or value cannot be
    /// bulk loaded: `text`, `ntext`, `image`, `sql_variant`, CLR types (`geometry`, `geography`,
    /// `hierarchyid`), `json`, `vector`, or `.raw` SQL values.
    public var method: SQLServerBulkCopyMethod
    /// Check CHECK and FOREIGN KEY constraints. Bulk load only; INSERT statements always check them.
    public var checkConstraints: Bool
    /// Fire the table's INSERT triggers. Bulk load only; INSERT statements always fire them.
    public var fireTriggers: Bool
    /// Store NULL as NULL; when false, columns with a default get the default instead.
    public var keepNulls: Bool
    /// Take a table lock for the duration of each batch (faster, blocks other writers).
    public var tableLock: Bool
    /// Always Encrypted: copy ciphertext into encrypted columns as it is, without the key (bulk load
    /// only), as SqlBulkCopy's AllowEncryptedValueModifications: for moving encrypted data between
    /// tables or databases. Use a connection without `columnEncryption`, on which encrypted columns
    /// read and write as `varbinary`; the database user needs ALLOW_ENCRYPTED_VALUE_MODIFICATIONS.
    /// Values copied this way are not checked: wrong bytes cannot be decrypted later.
    public var allowEncryptedValueModifications: Bool = false

    public init(
        table: String,
        schema: String = "dbo",
        columns: [String],
        batchSize: Int = 1_000,
        identityInsert: Bool = false,
        method: SQLServerBulkCopyMethod = .bulkLoad,
        checkConstraints: Bool = true,
        fireTriggers: Bool = true,
        keepNulls: Bool = true,
        tableLock: Bool = false
    ) {
        self.schema = schema
        self.table = table
        self.columns = columns
        self.batchSize = max(1, batchSize)
        self.identityInsert = identityInsert
        self.method = method
        self.checkConstraints = checkConstraints
        self.fireTriggers = fireTriggers
        self.keepNulls = keepNulls
        self.tableLock = tableLock
    }
    
    internal var qualifiedTableName: String {
        "\(SQLServerSQL.escapeIdentifier(schema)).\(SQLServerSQL.escapeIdentifier(table))"
    }

    internal var columnList: String {
        columns.map { SQLServerSQL.escapeIdentifier($0) }.joined(separator: ", ")
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
    /// The method used; `.insertStatements` when a bulk load was asked for but not possible.
    public let method: SQLServerBulkCopyMethod
}

public final class SQLServerBulkClient {
    private let client: SQLServerClient
    private let logger: Logger
    
    public init(client: SQLServerClient, logger: Logger? = nil) {
        self.client = client
        self.logger = logger ?? client.logger
    }
    
    /// Copies rows into a table on a pooled connection. See ``SQLServerConnection/bulkCopy(rows:options:afterBatch:)``.
    @available(macOS 12.0, *)
    public func copy(
        rows: [SQLServerBulkCopyRow],
        options: SQLServerBulkCopyOptions,
        afterBatch: (@Sendable (SQLServerConnection, Int) async throws -> Void)? = nil
    ) async throws -> SQLServerBulkCopySummary {
        try await client.withConnection { connection in
            try await connection.bulkCopy(rows: rows, options: options, afterBatch: afterBatch)
        }
    }
}

extension Array {
    internal func chunked(into size: Int) -> [[Element]] {
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
