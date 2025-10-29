import Foundation

public enum SqlServerExecutionMode: Sendable {
    case auto
    case simple
    case cursor
}

public struct SqlServerExecutionOptions: Sendable, Equatable {
    // Engine routing for this query. Defaults to .auto.
    public var mode: SqlServerExecutionMode

    // Advisory: future rowset size (e.g., via server cursors/rowsets); currently a no-op.
    public var rowsetFetchSize: Int?

    // Advisory: progress sampling throttle for package-level progress events (future).
    public var progressThrottleMs: Int?

    public init(
        mode: SqlServerExecutionMode = .auto,
        rowsetFetchSize: Int? = nil,
        progressThrottleMs: Int? = nil
    ) {
        self.mode = mode
        self.rowsetFetchSize = rowsetFetchSize
        self.progressThrottleMs = progressThrottleMs
    }
}

