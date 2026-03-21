import Foundation
import SQLServerTDS

public struct SQLServerConnectionPoolStatus: Sendable {
    public let active: Int
    public let idle: Int
    public let waiting: Int
    public let isShuttingDown: Bool
}

extension SQLServerClient {
    public var poolStatus: SQLServerConnectionPoolStatus {
        pool.statusSnapshot()
    }
}

@available(macOS 12.0, *)
extension SQLServerConnection {
    public func streamQuery(
        _ sql: String,
        options: SqlServerExecutionOptions
    ) -> SQLServerStreamSequence {
        let _ = options
        return streamQuery(sql)
    }
}
