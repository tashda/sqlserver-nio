import Foundation
import SQLServerTDS

public struct SQLServerExecutionResult: Sendable {
    internal let rawRows: [TDSRow]
    public let done: [SQLServerStreamDone]
    public let messages: [SQLServerStreamMessage]
    public let returnValues: [SQLServerReturnValue]

    public init(rows: [SQLServerRow], done: [SQLServerStreamDone], messages: [SQLServerStreamMessage], returnValues: [SQLServerReturnValue] = []) {
        self.rawRows = rows.map(\.base)
        self.done = done
        self.messages = messages
        self.returnValues = returnValues
    }

    internal init(rows: [TDSRow], done: [SQLServerStreamDone], messages: [SQLServerStreamMessage], returnValues: [SQLServerReturnValue] = []) {
        self.rawRows = rows
        self.done = done
        self.messages = messages
        self.returnValues = returnValues
    }

    public var rows: [SQLServerRow] {
        rawRows.map(SQLServerRow.init(base:))
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
