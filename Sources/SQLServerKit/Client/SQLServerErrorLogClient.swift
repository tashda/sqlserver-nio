import Foundation

// MARK: - Error Log Types

/// Which product's error log to read.
public enum SQLServerErrorLogProduct: Int, Sendable, CaseIterable {
    case sqlServer = 1
    case agent = 2
}

/// A SQL Server error log archive entry from xp_enumerrorlogs.
public struct SQLServerErrorLogArchive: Sendable, Hashable, Identifiable {
    public var id: Int { archiveNumber }
    public let archiveNumber: Int
    public let date: String
    public let sizeBytes: Int?

    public init(archiveNumber: Int, date: String, sizeBytes: Int?) {
        self.archiveNumber = archiveNumber
        self.date = date
        self.sizeBytes = sizeBytes
    }
}

/// A single entry from the SQL Server error log.
public struct SQLServerErrorLogEntry: Sendable, Identifiable {
    public let id: UUID
    public let logDate: String?
    public let processInfo: String?
    public let text: String

    public init(logDate: String?, processInfo: String?, text: String) {
        self.id = UUID()
        self.logDate = logDate
        self.processInfo = processInfo
        self.text = text
    }
}

// MARK: - SQLServerErrorLogClient

/// Namespace client for SQL Server error log operations.
///
/// Provides access to SQL Server and SQL Agent error logs, including
/// listing available log archives, reading log entries with optional
/// filters, and cycling the current error log.
///
/// Usage:
/// ```swift
/// let archives = try await client.errorLog.listErrorLogs()
/// let entries = try await client.errorLog.getErrorLogEntries(archiveNumber: 0, filter1: "Login")
/// try await client.errorLog.cycleErrorLog()
/// ```
public final class SQLServerErrorLogClient: @unchecked Sendable {
    private let client: SQLServerClient

    internal init(client: SQLServerClient) {
        self.client = client
    }

    // MARK: - List Error Logs

    /// Lists available error log archives for the specified product.
    ///
    /// - Parameter product: Which product's logs to enumerate (SQL Server or Agent).
    /// - Returns: An array of error log archive entries sorted by archive number.
    @available(macOS 12.0, *)
    public func listErrorLogs(
        product: SQLServerErrorLogProduct = .sqlServer
    ) async throws -> [SQLServerErrorLogArchive] {
        let sql = "EXEC xp_enumerrorlogs \(product.rawValue)"
        let rows = try await client.query(sql)

        return rows.compactMap { row in
            guard let archiveNumber = row.column("Archive #")?.int else { return nil }
            guard let date = row.column("Date")?.string else { return nil }
            let sizeBytes = row.column("Log File Size (Byte)")?.int

            return SQLServerErrorLogArchive(
                archiveNumber: archiveNumber,
                date: date,
                sizeBytes: sizeBytes
            )
        }
    }

    // MARK: - Read Error Log

    /// Returns entries from a specific error log archive with optional text filters.
    ///
    /// - Parameters:
    ///   - archiveNumber: The archive number to read (0 = current log).
    ///   - product: Which product's log to read (SQL Server or Agent).
    ///   - filter1: Optional first search string — only entries containing this text are returned.
    ///   - filter2: Optional second search string — further narrows results to entries also containing this text.
    /// - Returns: An array of error log entries matching the filters.
    @available(macOS 12.0, *)
    public func getErrorLogEntries(
        archiveNumber: Int = 0,
        product: SQLServerErrorLogProduct = .sqlServer,
        filter1: String? = nil,
        filter2: String? = nil
    ) async throws -> [SQLServerErrorLogEntry] {
        var sql = "EXEC sp_readerrorlog \(archiveNumber), \(product.rawValue)"

        if let filter1 {
            let escaped1 = filter1.replacingOccurrences(of: "'", with: "''")
            sql += ", N'\(escaped1)'"

            if let filter2 {
                let escaped2 = filter2.replacingOccurrences(of: "'", with: "''")
                sql += ", N'\(escaped2)'"
            }
        }

        let rows = try await client.query(sql)

        return rows.map { row in
            SQLServerErrorLogEntry(
                logDate: row.column("LogDate")?.string,
                processInfo: row.column("ProcessInfo")?.string,
                text: row.column("Text")?.string ?? ""
            )
        }
    }

    @available(*, deprecated, renamed: "getErrorLogEntries(archiveNumber:product:filter1:filter2:)")
    @available(macOS 12.0, *)
    public func readErrorLog(
        archiveNumber: Int = 0,
        product: SQLServerErrorLogProduct = .sqlServer,
        filter1: String? = nil,
        filter2: String? = nil
    ) async throws -> [SQLServerErrorLogEntry] {
        try await getErrorLogEntries(archiveNumber: archiveNumber, product: product, filter1: filter1, filter2: filter2)
    }

    // MARK: - Cycle Error Log

    /// Cycles the current SQL Server error log, closing the current log and opening a new one.
    @available(macOS 12.0, *)
    public func cycleErrorLog() async throws {
        _ = try await client.execute("EXEC sp_cycle_errorlog")
    }
}
