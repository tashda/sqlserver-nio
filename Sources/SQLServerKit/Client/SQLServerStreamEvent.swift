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
    /// The column's wire type, for formatting spooled cells with
    /// `SQLServerCellFormatter` (store `cellType.encoded`).
    public let cellType: SQLServerCellType
    /// Always Encrypted: how the column is encrypted, on a connection with `columnEncryption`.
    /// Its values are ciphertext (`varbinary`); the driver cannot decrypt them.
    public var encryption: SQLServerColumnEncryption? = nil
}

/// An Always Encrypted column, as SQL Server describes it to a client that negotiated column
/// encryption.
public struct SQLServerColumnEncryption: Sendable, Hashable {
    public enum Kind: String, Sendable {
        /// The same plaintext always gives the same ciphertext (equality lookups, joins).
        case deterministic
        /// Different ciphertext each time.
        case randomized
    }

    public let kind: Kind?
    /// `AEAD_AES_256_CBC_HMAC_SHA_256` today.
    public let algorithm: String
    /// The plaintext type.
    public let type: SQLServerDataType
    /// The plaintext type as declared, for example `nvarchar(11)` or `decimal(10, 2)`.
    public let typeName: String
    /// Where the column master key lives (`MSSQL_CERTIFICATE_STORE`, `AZURE_KEY_VAULT`, …).
    public let keyStoreName: String?
    /// The column master key's path in that store.
    public let keyPath: String?

    init(_ encryption: TDSTokens.ColMetadataToken.ColumnData.ColumnEncryption) {
        kind = encryption.kind.map { $0 == .deterministic ? .deterministic : .randomized }
        algorithm = encryption.algorithm
        let base = encryption.baseType
        type = SQLServerDataType(base: base.dataType)
        typeName = Self.declaredName(base)
        keyStoreName = encryption.keyStoreName
        keyPath = encryption.keyPath
    }

    static func declaredName(_ base: TDSTokens.ColMetadataToken.ColumnData.TypeInfo) -> String {
        let name = SQLServerDataType(base: base.dataType).name
        let isMax = base.length == 0xFFFF || base.length == -1
        switch base.dataType {
        case .char, .varchar, .binary, .varbinary, .charLegacy, .varcharLegacy, .binaryLegacy, .varbinaryLegacy:
            return isMax ? "\(name)(max)" : "\(name)(\(base.length))"
        case .nchar, .nvarchar:
            return isMax ? "\(name)(max)" : "\(name)(\(base.length / 2))"
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            return "\(name)(\(base.precision), \(base.scale))"
        case .time, .datetime2, .datetimeOffset:
            return "\(name)(\(base.scale))"
        default:
            return name
        }
    }
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

extension SQLServerStreamMessage {
    internal init(token: TDSTokens.ErrorInfoToken, isError: Bool) {
        self.init(
            kind: isError ? .error : .info,
            number: Int32(token.number),
            message: token.messageText,
            state: token.state,
            severity: token.classValue,
            serverName: token.serverName,
            procedureName: token.procName,
            lineNumber: token.lineNumber
        )
    }
}
