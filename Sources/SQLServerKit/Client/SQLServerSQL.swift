import Foundation

/// Shared SQL escaping utilities for SQL Server identifier and literal handling.
///
/// All namespace clients should use these instead of defining their own copies.
internal enum SQLServerSQL {
    /// Escapes a SQL Server identifier by wrapping it in brackets and doubling any existing `]` characters.
    ///
    /// Returns a bracket-delimited identifier safe for use in SQL statements.
    /// Example: `escapeIdentifier("my]table")` returns `[my]]table]`
    static func escapeIdentifier(_ identifier: String) -> String {
        "[\(identifier.replacingOccurrences(of: "]", with: "]]"))]"
    }

    /// Escapes a SQL Server string literal by doubling any single-quote characters.
    ///
    /// The result does NOT include the surrounding single quotes — the caller adds those.
    /// Example: `escapeLiteral("it's")` returns `it''s`
    static func escapeLiteral(_ literal: String) -> String {
        literal.replacingOccurrences(of: "'", with: "''")
    }
}
