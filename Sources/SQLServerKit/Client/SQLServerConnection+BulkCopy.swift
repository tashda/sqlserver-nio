import Foundation
import NIO
import NIOConcurrencyHelpers
import SQLServerTDS

extension SQLServerConnection {
    /// Copies rows into a table in batches of `options.batchSize`.
    ///
    /// With `.bulkLoad` (the default) each batch is one TDS bulk load, as `bcp` and SqlBulkCopy send
    /// it: values are converted on the client to each destination column's type, so text from a
    /// CSV file is read the way SQL Server would convert it (`yyyy-MM-dd HH:mm:ss` dates, `.`
    /// decimals). Every value is checked before the first batch is sent; a value that does not
    /// convert throws ``SQLServerBulkCopyError/invalidValue(row:column:value:reason:)`` and nothing
    /// is written. Batches are committed one by one unless the connection is in a transaction.
    @available(macOS 12.0, *)
    public func bulkCopy(
        rows: [SQLServerBulkCopyRow],
        options: SQLServerBulkCopyOptions,
        afterBatch: (@Sendable (SQLServerConnection, Int) async throws -> Void)? = nil
    ) async throws -> SQLServerBulkCopySummary {
        try checkClosed()
        let start = Date()
        func summary(rows copied: Int, batches: Int, method: SQLServerBulkCopyMethod) -> SQLServerBulkCopySummary {
            SQLServerBulkCopySummary(
                schema: options.schema, table: options.table, totalRows: copied, batchesExecuted: batches,
                batchSize: options.batchSize, identityInsert: options.identityInsert,
                duration: Date().timeIntervalSince(start), method: method
            )
        }
        guard !rows.isEmpty, !options.columns.isEmpty else {
            return summary(rows: 0, batches: 0, method: options.method)
        }
        for row in rows where row.values.count != options.columns.count {
            throw SQLServerBulkCopyError.columnCountMismatch(expected: options.columns.count, actual: row.values.count)
        }

        if options.method == .bulkLoad, let destination = try await bulkLoadDestination(options, rows: rows) {
            try Self.validateBulkLoadValues(rows.map { SQLServerBulkCopyRow(values: destination.values(of: $0)) }, columns: destination.columns)
            let statement = Self.insertBulkStatement(options, declarations: destination.declarations)
            var copied = 0
            var batches = 0
            for chunk in rows.chunked(into: options.batchSize) {
                var writer = TDSBulkLoadWriter(columns: destination.columns)
                for row in chunk {
                    try writer.appendRow(zip(destination.values(of: row), destination.columns).map { try SQLServerBulkValueEncoder.encode($0, for: $1) })
                }
                _ = try await execute(statement)
                copied += try await sendBulkLoad(writer.finished(), rowCount: chunk.count)
                batches += 1
                if let afterBatch { try await afterBatch(self, batches) }
            }
            return summary(rows: copied, batches: batches, method: .bulkLoad)
        }

        var copied = 0
        var batches = 0
        let table = options.qualifiedTableName + (options.tableLock ? " WITH (TABLOCK)" : "")
        for chunk in rows.chunked(into: options.batchSize) {
            let valuesClause = chunk.map { row in
                let literals = row.values.map { value -> String in
                    if case .null = value, !options.keepNulls { return "DEFAULT" }
                    return value.sqlLiteral()
                }.joined(separator: ", ")
                return "(\(literals))"
            }.joined(separator: ",\n")
            var statement = """
            INSERT INTO \(table) (\(options.columnList))
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
            let result = try await execute(statement)
            if let rowCount = result.rowCount, rowCount > 0 {
                copied += Int(rowCount)
            } else if result.totalRowCount > 0 {
                copied += Int(result.totalRowCount)
            } else {
                copied += chunk.count
            }
            batches += 1
            if let afterBatch { try await afterBatch(self, batches) }
        }
        return summary(rows: copied, batches: batches, method: .insertStatements)
    }

    // MARK: - Destination

    struct BulkLoadDestination {
        var columns: [TDSColumnMetadata]
        /// `[name] type [COLLATE name]` for each column, for INSERT BULK.
        var declarations: [String]
        /// The positions in each row of the values sent, one per column.
        var valueIndexes: [Int]

        func values(of row: SQLServerBulkCopyRow) -> [SQLServerLiteralValue] {
            valueIndexes.map { row.values[$0] }
        }
    }

    /// The destination columns as the server describes them, or nil when a column or value cannot be
    /// bulk loaded (the copy then uses INSERT statements).
    @available(macOS 12.0, *)
    func bulkLoadDestination(_ options: SQLServerBulkCopyOptions, rows: [SQLServerBulkCopyRow]) async throws -> BulkLoadDestination? {
        for row in rows {
            for value in row.values where !Self.canBulkLoad(value) { return nil }
        }
        let described = try await columnMetadata(of: "SELECT TOP (0) \(options.columnList) FROM \(options.qualifiedTableName)")
        guard described.count == options.columns.count,
              described.allSatisfy({ TDSBulkLoadWriter.supports($0) && Self.bulkTypeName($0) != nil }) else {
            return nil
        }
        // A bulk load keeps the identity values of an identity column it lists. Without
        // identityInsert the column is left out and the server assigns them, as SqlBulkCopy does.
        let valueIndexes = described.indices.filter { options.identityInsert || described[$0].flags & 0x0010 == 0 }
        guard !valueIndexes.isEmpty else { return nil }
        let columns = valueIndexes.map { described[$0] }

        let isTemporary = options.table.hasPrefix("#")
        let catalog = isTemporary ? "tempdb.sys.columns" : "sys.columns"
        let objectName = (isTemporary ? "tempdb.." + SQLServerSQL.escapeIdentifier(options.table) : options.qualifiedTableName)
            .replacingOccurrences(of: "'", with: "''")
        var collations: [String: String] = [:]
        for row in try await query("SELECT name, collation_name FROM \(catalog) WHERE object_id = OBJECT_ID(N'\(objectName)') AND collation_name IS NOT NULL") {
            if let name = row.column("name")?.string, let collation = row.column("collation_name")?.string {
                collations[name.lowercased()] = collation
            }
        }

        let declarations = columns.map { column -> String in
            var declaration = "\(SQLServerSQL.escapeIdentifier(column.colName)) \(Self.bulkTypeName(column)!)"
            if let collation = collations[column.colName.lowercased()], collation.allSatisfy({ $0.isLetter || $0.isNumber || $0 == "_" }) {
                declaration += " COLLATE \(collation)"
            }
            return declaration
        }
        return BulkLoadDestination(columns: columns, declarations: declarations, valueIndexes: valueIndexes)
    }

    static func canBulkLoad(_ value: SQLServerLiteralValue) -> Bool {
        switch value {
        case .raw, .geometry, .geography, .hierarchyID: return false
        case .variant(let inner): return canBulkLoad(inner)
        default: return true
        }
    }

    static func validateBulkLoadValues(_ rows: [SQLServerBulkCopyRow], columns: [TDSColumnMetadata]) throws {
        for (index, row) in rows.enumerated() {
            for (value, column) in zip(row.values, columns) {
                do {
                    _ = try SQLServerBulkValueEncoder.encode(value, for: column)
                } catch let error as SQLServerBulkValueEncoder.ConversionError {
                    throw SQLServerBulkCopyError.invalidValue(row: index + 1, column: column.colName, value: value.displayText, reason: error.reason)
                }
            }
        }
    }

    static func insertBulkStatement(_ options: SQLServerBulkCopyOptions, declarations: [String]) -> String {
        var hints: [String] = []
        if options.tableLock { hints.append("TABLOCK") }
        if options.checkConstraints { hints.append("CHECK_CONSTRAINTS") }
        if options.fireTriggers { hints.append("FIRE_TRIGGERS") }
        if options.keepNulls { hints.append("KEEP_NULLS") }
        let with = hints.isEmpty ? "" : " WITH (\(hints.joined(separator: ", ")))"
        return "INSERT BULK \(options.qualifiedTableName) (\(declarations.joined(separator: ", ")))\(with)"
    }

    /// The column's type as INSERT BULK declares it, or nil for types bulk load does not carry.
    static func bulkTypeName(_ column: TDSColumnMetadata) -> String? {
        let isMax = column.length == 0xFFFF || column.length == -1
        func sized(_ name: String, _ length: Int) -> String { isMax ? "\(name)(max)" : "\(name)(\(max(length, 1)))" }
        switch column.dataType {
        case .tinyInt: return "tinyint"
        case .smallInt: return "smallint"
        case .int: return "int"
        case .bigInt: return "bigint"
        case .intn:
            switch column.length {
            case 1: return "tinyint"
            case 2: return "smallint"
            case 4: return "int"
            default: return "bigint"
            }
        case .bit, .bitn: return "bit"
        case .real: return "real"
        case .float: return "float"
        case .floatn: return column.length == 4 ? "real" : "float"
        case .money: return "money"
        case .smallMoney: return "smallmoney"
        case .moneyn: return column.length == 4 ? "smallmoney" : "money"
        case .datetime: return "datetime"
        case .smallDateTime: return "smalldatetime"
        case .datetimen: return column.length == 4 ? "smalldatetime" : "datetime"
        case .date: return "date"
        case .time: return "time(\(column.scale))"
        case .datetime2: return "datetime2(\(column.scale))"
        case .datetimeOffset: return "datetimeoffset(\(column.scale))"
        case .decimal, .decimalLegacy: return "decimal(\(column.precision), \(column.scale))"
        case .numeric, .numericLegacy: return "numeric(\(column.precision), \(column.scale))"
        case .guid: return "uniqueidentifier"
        case .char, .charLegacy: return sized("char", Int(column.length))
        case .varchar, .varcharLegacy: return sized("varchar", Int(column.length))
        case .nchar: return sized("nchar", Int(column.length) / 2)
        case .nvarchar: return sized("nvarchar", Int(column.length) / 2)
        case .binary, .binaryLegacy: return sized("binary", Int(column.length))
        case .varbinary, .varbinaryLegacy: return sized("varbinary", Int(column.length))
        case .xml: return "xml"
        default: return nil
        }
    }

    // MARK: - Requests

    /// The column metadata of a query's first result set.
    @available(macOS 12.0, *)
    func columnMetadata(of sql: String) async throws -> [TDSColumnMetadata] {
        struct Accumulator: Sendable {
            var columns: [TDSColumnMetadata]?
            var messages: [SQLServerStreamMessage] = []
        }
        let accumulator = NIOLockedValueBox(Accumulator())
        let request = RawSqlRequest(
            sql: sql,
            onMetadata: { columns in
                accumulator.withLockedValue { if $0.columns == nil { $0.columns = columns } }
            },
            onMessage: { token, isError in
                accumulator.withLockedValue { $0.messages.append(SQLServerStreamMessage(token: token, isError: isError)) }
            }
        )
        let handle = base.start(request, timeout: configuration.sessionOptions.defaultQueryTimeout.flatMap(Self.timeAmount))
        let result = handle.future.flatMapThrowing { _ -> [TDSColumnMetadata] in
            let snapshot = accumulator.withLockedValue { $0 }
            if let error = SQLServerError.fromServerMessages(snapshot.messages) { throw error }
            return snapshot.columns ?? []
        }
        return try await Self.awaitCancellable(handle: handle, result: result)
    }

    /// Sends the data of one bulk load (after its INSERT BULK) and returns the rows the server copied.
    @available(macOS 12.0, *)
    func sendBulkLoad(_ payload: ByteBuffer, rowCount: Int) async throws -> Int {
        struct Accumulator: Sendable {
            var copied: UInt64 = 0
            var messages: [SQLServerStreamMessage] = []
        }
        let accumulator = NIOLockedValueBox(Accumulator())
        let request = BulkLoadRequest(
            payload: payload,
            rowCount: rowCount,
            onDone: { token in
                // SQL Server reports the rows copied without setting DONE_COUNT.
                accumulator.withLockedValue { $0.copied += token.doneRowCount }
            },
            onMessage: { token, isError in
                accumulator.withLockedValue { $0.messages.append(SQLServerStreamMessage(token: token, isError: isError)) }
            }
        )
        let handle = base.start(request, timeout: configuration.sessionOptions.defaultQueryTimeout.flatMap(Self.timeAmount))
        let result = handle.future.flatMapThrowing { _ -> Int in
            let snapshot = accumulator.withLockedValue { $0 }
            if let error = SQLServerError.fromServerMessages(snapshot.messages) { throw error }
            return Int(snapshot.copied)
        }.flatMapErrorThrowing { error -> Int in
            throw SQLServerError.normalize(error)
        }
        return try await Self.awaitCancellable(handle: handle, result: result)
    }
}

extension SQLServerLiteralValue {
    /// The value as shown in a conversion error.
    var displayText: String {
        switch self {
        case .null: return "NULL"
        case .string(let s), .nString(let s), .decimal(let s), .raw(let s): return s
        case .int(let n): return String(n)
        case .int64(let n): return String(n)
        case .double(let d): return String(d)
        case .bool(let b): return b ? "true" : "false"
        case .date(let d): return ISO8601DateFormatter().string(from: d)
        case .uuid(let u): return u.uuidString
        case .bytes(let b): return "0x" + b.prefix(32).map { String(format: "%02X", $0) }.joined()
        case .variant(let inner): return inner.displayText
        default: return sqlLiteral()
        }
    }
}
