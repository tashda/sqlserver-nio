import struct Foundation.Decimal
import SQLServerTDS

public struct SQLServerColumnDescription: Sendable {
    public let name: String
    public let type: SQLServerDataType
    public let typeName: String
    public let length: Int
    public let precision: Int?
    public let scale: Int?
    public let flags: UInt16
}

public struct SQLServerStreamDone: Sendable {
    public enum Kind: String, Sendable {
        case done
        case doneProc
        case doneInProc
    }

    public let kind: Kind
    public let status: UInt16
    public let curCmd: UInt16
    public let rowCount: UInt64

    public init(kind: Kind, status: UInt16, curCmd: UInt16, rowCount: UInt64) {
        self.kind = kind
        self.status = status
        self.curCmd = curCmd
        self.rowCount = rowCount
    }
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
    public let serverName: String
    public let procedureName: String
    public let lineNumber: Int32

    public init(
        kind: Kind,
        number: Int32,
        message: String,
        state: UInt8,
        severity: UInt8,
        serverName: String,
        procedureName: String,
        lineNumber: Int32
    ) {
        self.kind = kind
        self.number = number
        self.message = message
        self.state = state
        self.severity = severity
        self.serverName = serverName
        self.procedureName = procedureName
        self.lineNumber = lineNumber
    }
}

extension SQLServerStreamDone.Kind {
    init(tokenType: TDSTokens.TokenType) {
        switch tokenType {
        case .done:
            self = .done
        case .doneProc:
            self = .doneProc
        case .doneInProc:
            self = .doneInProc
        default:
            self = .done
        }
    }
}

public enum SQLServerStreamEvent: Sendable {
    case metadata([SQLServerColumnDescription])
    case row(SQLServerRow)
    case done(SQLServerStreamDone)
    case message(SQLServerStreamMessage)
}
