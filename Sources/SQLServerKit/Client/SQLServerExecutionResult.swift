import Foundation
import SQLServerTDS

public struct SQLServerExecutionResult: Sendable {
    public let rows: [TDSRow]
    public let done: [SQLServerStreamDone]
    public let messages: [SQLServerStreamMessage]
    public let returnValues: [SQLServerReturnValue]

    public init(rows: [TDSRow], done: [SQLServerStreamDone], messages: [SQLServerStreamMessage], returnValues: [SQLServerReturnValue] = []) {
        self.rows = rows
        self.done = done
        self.messages = messages
        self.returnValues = returnValues
    }

    public var rowCount: UInt64? {
        done.last?.rowCount
    }

    public var totalRowCount: UInt64 {
        done.reduce(0) { $0 + $1.rowCount }
    }

    public var rowCounts: [UInt64] {
        done.map { $0.rowCount }
    }
}
