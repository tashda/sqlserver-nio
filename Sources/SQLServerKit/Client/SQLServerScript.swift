import Foundation

/// One batch of a script: the text between `GO` separators, run `repeatCount` times.
public struct SQLServerScriptBatch: Sendable, Equatable {
    public let sql: String
    public let repeatCount: Int
    /// 1-based line of the batch's first line in the script.
    public let line: Int
}

/// Splits T-SQL scripts into batches the way sqlcmd and SSMS do: a line holding only `GO` (optionally
/// `GO n`, optionally followed by a `--` comment) ends a batch. `GO` inside strings, quoted or
/// bracketed identifiers and comments does not count.
public enum SQLServerScript {
    public static func batches(in script: String) -> [SQLServerScriptBatch] {
        var batches: [SQLServerScriptBatch] = []
        var current: [Substring] = []
        var currentStart = 1
        var state = LexState()
        // "\r\n" is one Character in Swift, so normalise line endings before splitting.
        let normalized = script.replacingOccurrences(of: "\r\n", with: "\n")
        let lines = normalized.split(separator: "\n", omittingEmptySubsequences: false)

        for (index, line) in lines.enumerated() {
            if state.isAtTopLevel, let count = separatorCount(line) {
                append(current, start: currentStart, repeatCount: count, to: &batches)
                current = []
                currentStart = index + 2
                continue
            }
            state.scan(line)
            current.append(line)
        }
        append(current, start: currentStart, repeatCount: 1, to: &batches)
        return batches
    }

    private static func append(_ lines: [Substring], start: Int, repeatCount: Int, to batches: inout [SQLServerScriptBatch]) {
        let sql = lines.joined(separator: "\n")
        guard !sql.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        batches.append(SQLServerScriptBatch(sql: sql, repeatCount: repeatCount, line: start))
    }

    /// The repeat count if the line is a batch separator.
    static func separatorCount(_ line: Substring) -> Int? {
        var text = line.trimmingCharacters(in: .whitespaces)
        if let comment = text.range(of: "--") { text = String(text[..<comment.lowerBound]).trimmingCharacters(in: .whitespaces) }
        let parts = text.split(separator: " ", omittingEmptySubsequences: true)
        guard let first = parts.first, first.uppercased() == "GO", parts.count <= 2 else { return nil }
        if parts.count == 2 {
            guard let count = Int(parts[1]), count > 0 else { return nil }
            return count
        }
        return 1
    }

    /// Where a line ends: inside a string, a quoted or bracketed identifier, or a block comment.
    struct LexState {
        var blockCommentDepth = 0
        var closingQuote: Character?

        var isAtTopLevel: Bool { blockCommentDepth == 0 && closingQuote == nil }

        mutating func scan(_ line: Substring) {
            var index = line.startIndex
            while index < line.endIndex {
                let character = line[index]
                let next = line.index(after: index)
                let following: Character? = next < line.endIndex ? line[next] : nil
                if let quote = closingQuote {
                    if character == quote {
                        // A doubled closing character is an escaped one.
                        if following == quote { index = line.index(after: next); continue }
                        closingQuote = nil
                    }
                } else if blockCommentDepth > 0 {
                    if character == "*", following == "/" { blockCommentDepth -= 1; index = line.index(after: next); continue }
                    if character == "/", following == "*" { blockCommentDepth += 1; index = line.index(after: next); continue }
                } else {
                    switch character {
                    case "-" where following == "-":
                        return
                    case "/" where following == "*":
                        blockCommentDepth = 1
                        index = line.index(after: next)
                        continue
                    case "'": closingQuote = "'"
                    case "\"": closingQuote = "\""
                    case "[": closingQuote = "]"
                    default: break
                    }
                }
                index = next
            }
        }
    }
}

/// What running a script did.
public struct SQLServerScriptSummary: Sendable {
    public let batchesRun: Int
    public let messages: [SQLServerStreamMessage]
}

public struct SQLServerScriptError: Error, CustomStringConvertible, Sendable {
    public let batchIndex: Int
    public let line: Int
    public let underlying: String
    public var description: String { "Batch \(batchIndex + 1) (line \(line)) failed: \(underlying)" }
}

/// Runs T-SQL scripts (with `GO` separators) through the driver on one connection.
public final class SQLServerScriptClient: @unchecked Sendable {
    private let client: SQLServerClient

    public init(client: SQLServerClient) {
        self.client = client
    }

    /// Runs every batch in order on one connection, starting in `database` when given, and
    /// returns the connection to the database it started in. Stops at the first failing batch.
    @available(macOS 12.0, *)
    @discardableResult
    public func run(
        _ script: String,
        database: String? = nil,
        progress: (@Sendable (_ batch: Int, _ of: Int) -> Void)? = nil
    ) async throws -> SQLServerScriptSummary {
        let batches = SQLServerScript.batches(in: script)
        return try await client.withConnection { connection in
            let original = connection.currentDatabase
            if let database { try await connection.changeDatabase(database) }
            var messages: [SQLServerStreamMessage] = []
            do {
                for (index, batch) in batches.enumerated() {
                    progress?(index + 1, batches.count)
                    for _ in 0..<batch.repeatCount {
                        do {
                            messages += try await connection.execute(batch.sql).messages
                        } catch {
                            throw SQLServerScriptError(batchIndex: index, line: batch.line, underlying: String(describing: error))
                        }
                    }
                }
            } catch {
                try? await connection.changeDatabase(original)
                throw error
            }
            if connection.currentDatabase.caseInsensitiveCompare(original) != .orderedSame {
                try await connection.changeDatabase(original)
            }
            return SQLServerScriptSummary(batchesRun: batches.count, messages: messages)
        }
    }

    /// Runs a script file (UTF-8, or UTF-16 with a byte-order mark as SSMS saves them).
    @available(macOS 12.0, *)
    @discardableResult
    public func run(
        contentsOf file: URL,
        database: String? = nil,
        progress: (@Sendable (_ batch: Int, _ of: Int) -> Void)? = nil
    ) async throws -> SQLServerScriptSummary {
        let data = try Data(contentsOf: file)
        let text: String
        if data.starts(with: [0xFF, 0xFE]) || data.starts(with: [0xFE, 0xFF]) {
            // .utf16 reads the byte-order mark and drops it.
            text = String(data: data, encoding: .utf16) ?? ""
        } else {
            text = String(decoding: data.starts(with: [0xEF, 0xBB, 0xBF]) ? data.dropFirst(3) : data, as: UTF8.self)
        }
        return try await run(text, database: database, progress: progress)
    }
}
