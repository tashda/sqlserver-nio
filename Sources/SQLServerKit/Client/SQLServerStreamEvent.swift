import struct Foundation.Decimal
import SQLServerTDS

public struct SQLServerColumnDescription: Sendable {
    public let name: String
    public let type: TDSDataType
    public let length: Int
    public let precision: Int?
    public let scale: Int?
    public let flags: UInt16
}

public struct SQLServerStreamDone: Sendable {
    public let status: UInt16
    public let rowCount: UInt64
}

public struct SQLServerStreamMessage: Sendable {
    public enum Kind: Sendable {
        case info
        case error
    }

    public let kind: Kind
    public let number: Int32
    public let message: String
    public let state: UInt8
    public let severity: UInt8
}

public enum SQLServerStreamEvent: Sendable {
    case metadata([SQLServerColumnDescription])
    case row(TDSRow)
    case done(SQLServerStreamDone)
    case message(SQLServerStreamMessage)
}

