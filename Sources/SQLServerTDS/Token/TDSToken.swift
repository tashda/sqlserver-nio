import Foundation
import NIOCore

public protocol TDSToken: Sendable {
    var type: TDSTokens.TokenType { get }
}

public enum TDSTokens {
    public enum EnvChangeType: UInt8, Sendable {
        case database = 0x01
        case language = 0x02
        case charset = 0x03
        case packetSize = 0x04
        case sqlCollation = 0x07
        case beginTransaction = 0x08
        case commitTransaction = 0x09
        case rollbackTransaction = 0x0A
        case enlistDTCTransaction = 0x0B
        case defectTransaction = 0x0C
        case promoteTransaction = 0x0F
        case transactionManagerAddress = 0x10
        case transactionEnded = 0x11
        case resetConnectionAck = 0x12
        case userInstance = 0x13
        case routing = 0x14
        case enhancedRouting = 0x15
    }

    public enum TokenType: UInt8, Sendable {
        /// ALTMETADATA
        case altMetadata = 0x88
        /// ALTROW
        case altRow = 0xD3
        /// COLINFO
        case colInfo = 0xA5
        /// COLMETADATA
        case colMetadata = 0x81
        /// DONE
        case done = 0xFD
        /// DONEINPROC
        case doneInProc = 0xFF
        /// DONEPROC
        case doneProc = 0xFE
        /// ENVCHANGE
        case envchange = 0xE3
        /// ERROR
        case error = 0xAA
        /// FEATUREEXTACK
        case featureExtAck = 0xAE
        /// FEDAUTHINFO
        case fedAuthInfo = 0xEE
        /// INFO
        case info = 0xAB
        /// LOGINACK
        case loginAck = 0xAD
        /// NBCROW
        case nbcRow = 0xD2
        /// OFFSET
        case offset = 0x78
        /// ORDER
        case order = 0xA9
        /// RETURNSTATUS
        case returnStatus = 0x79
        /// RETURNVALUE
        case returnValue = 0xAC
        /// ROW
        case row = 0xD1
        /// SESSIONSTATE
        case sessionState = 0xE4
        /// SSPI
        case sspi = 0xED
        /// TABNAME
        case tabName = 0xA4
        /// DATA_CLASSIFICATION
        case dataClassification = 0xA3
        /// SQL_RESULT_COLUMN_SOURCES
        case sqlResultColumnSources = 0xAF // Adjusted value
        
        // Extended/Unknown types from tracing
        case unknown0x04 = 0x04
        case unknown0x61 = 0x61
        case unknown0x74 = 0x74
        case unknown0xc1 = 0xC1
        case columnStatus = 0x12 // Placeholder
        case tvpRow = 0xD4 // Placeholder for TVP rows
    }

    public struct DoneToken: TDSToken {
        public typealias Status = UInt16
        public var type: TokenType = .done
        public var status: Status
        public var curCmd: UInt16
        public var doneRowCount: UInt64

        public init(status: Status, curCmd: UInt16, doneRowCount: UInt64) {
            self.status = status
            self.curCmd = curCmd
            self.doneRowCount = doneRowCount
        }

        static func parse(from buffer: inout ByteBuffer) throws -> DoneToken {
            guard let status: UInt16 = buffer.readInteger(endianness: .little),
                  let curCmd: UInt16 = buffer.readInteger(endianness: .little),
                  let rowCount: UInt64 = buffer.readInteger(endianness: .little) else {
                throw TDSError.needMoreData
            }
            return DoneToken(status: status, curCmd: curCmd, doneRowCount: rowCount)
        }
    }

    public struct ColMetadataToken: TDSToken {
        public var type: TokenType = .colMetadata
        public var colData: [ColumnData]
        public var count: Int { colData.count }

        public struct ColumnData: Metadata, Sendable {
            public var userType: UInt32
            public var flags: UInt16
            public var dataType: TDSDataType
            // Type-specific metadata
            public var length: Int32
            public var precision: UInt8
            public var scale: UInt8
            public var collation: [UInt8]
            public var colName: String
            public var isView: Bool = false
            
            public init(userType: UInt32, flags: UInt16, dataType: TDSDataType, length: Int32, precision: UInt8, scale: UInt8, collation: [UInt8] = [], colName: String) {
                self.userType = userType
                self.flags = flags
                self.dataType = dataType
                self.length = length
                self.precision = precision
                self.scale = scale
                self.collation = collation
                self.colName = colName
            }

            public init(
                userType: UInt32,
                flags: UInt16,
                dataType: TDSDataType,
                length: Int32,
                collation: [UInt8] = [],
                tableName: String? = nil,
                colName: String,
                precision: UInt8? = nil,
                scale: UInt8? = nil
            ) {
                let _ = tableName
                self.init(
                    userType: userType,
                    flags: flags,
                    dataType: dataType,
                    length: length,
                    precision: precision ?? 0,
                    scale: scale ?? 0,
                    collation: collation,
                    colName: colName
                )
            }
        }
        
        public init(colData: [ColumnData]) {
            self.colData = colData
        }

        public init(count: Int, colData: [ColumnData]) {
            let _ = count
            self.init(colData: colData)
        }
    }

    public struct RowToken: TDSToken {
        public var type: TokenType = .row
        public var colMetadata: [ColMetadataToken.ColumnData]
        public var colData: [ColumnData]

        public struct ColumnData: Sendable {
            public var textPointer: [Byte]
            public var timestamp: [Byte]
            public var data: ByteBuffer?
            
            public init(textPointer: [Byte] = [], timestamp: [Byte] = [], data: ByteBuffer?) {
                self.textPointer = textPointer
                self.timestamp = timestamp
                self.data = data
            }
        }
        
        public init(colMetadata: [ColMetadataToken.ColumnData], colData: [ColumnData]) {
            self.colMetadata = colMetadata
            self.colData = colData
        }
    }

    public struct NbcRowToken: TDSToken {
        public var type: TokenType = .nbcRow
        public var nullBitmap: [Byte]
        public var colMetadata: [ColMetadataToken.ColumnData]
        public var colData: [RowToken.ColumnData]
        
        public init(nullBitmap: [Byte] = [], colMetadata: [ColMetadataToken.ColumnData], colData: [RowToken.ColumnData]) {
            self.nullBitmap = nullBitmap
            self.colMetadata = colMetadata
            self.colData = colData
        }
    }

    public struct TvpRowToken: TDSToken {
        public var type: TokenType = .tvpRow
        public var colMetadata: [ColMetadataToken.ColumnData]
        public var colData: [RowToken.ColumnData]

        public init(
            colMetadata: [ColMetadataToken.ColumnData] = [],
            colData: [RowToken.ColumnData] = []
        ) {
            self.colMetadata = colMetadata
            self.colData = colData
        }
    }

    public typealias TVPRowToken = TvpRowToken

    public struct EnvchangeToken<T: Sendable>: TDSToken {
        public var type: TokenType = .envchange
        public var envType: UInt8
        public var newValue: T
        public var oldValue: T
        
        public init(envType: UInt8, newValue: T, oldValue: T) {
            self.envType = envType
            self.newValue = newValue
            self.oldValue = oldValue
        }

        public var envchangeType: EnvChangeType? {
            EnvChangeType(rawValue: envType)
        }
    }

    public struct ErrorInfoToken: TDSToken {
        public var type: TokenType
        public var number: Int32
        public var state: UInt8
        public var classValue: UInt8
        public var messageText: String
        public var serverName: String
        public var procName: String
        public var lineNumber: Int32
        
        public init(type: TokenType, number: Int32, state: UInt8, classValue: UInt8, messageText: String, serverName: String, procName: String, lineNumber: Int32) {
            self.type = type
            self.number = number
            self.state = state
            self.classValue = classValue
            self.messageText = messageText
            self.serverName = serverName
            self.procName = procName
            self.lineNumber = lineNumber
        }

        public var procedureName: String {
            procName
        }
    }

    public struct LoginAckToken: TDSToken {
        public var type: TokenType = .loginAck
        public var interface: UInt8
        public var tdsVersion: UInt32
        public var progName: String
        public var version: UInt32
        
        public init(interface: UInt8, tdsVersion: UInt32, progName: String, version: UInt32) {
            self.interface = interface
            self.tdsVersion = tdsVersion
            self.progName = progName
            self.version = version
        }

        public var majorVer: UInt8 {
            UInt8((version >> 24) & 0xFF)
        }

        public var minorVer: UInt8 {
            UInt8((version >> 16) & 0xFF)
        }
    }

    public struct FeatureExtAckToken: TDSToken {
        public var type: TokenType = .featureExtAck
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct FedAuthInfoToken: TDSToken {
        public var type: TokenType = .fedAuthInfo
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct SessionStateToken: TDSToken {
        public var type: TokenType = .sessionState
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct TabNameToken: TDSToken {
        public var type: TokenType = .tabName
        public var data: [Byte]
        public init(data: [Byte]) { self.data = data }
    }

    public struct ColInfoToken: TDSToken {
        public var type: TokenType = .colInfo
        public var data: [Byte]
        public init(data: [Byte]) { self.data = data }
    }

    public struct OffsetToken: TDSToken {
        public var type: TokenType = .offset
        public var data: [Byte]
        public init(data: [Byte]) { self.data = data }
    }

    public struct DataClassificationToken: TDSToken {
        public var type: TokenType = .dataClassification
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct SQLResultColumnSourcesToken: TDSToken {
        public var type: TokenType = .sqlResultColumnSources
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct Unknown0x61Token: TDSToken {
        public var type: TokenType = .unknown0x61
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct Unknown0x74Token: TDSToken {
        public var type: TokenType = .unknown0x74
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct Unknown0xC1Token: TDSToken {
        public var type: TokenType = .unknown0xc1
        public var payload: ByteBuffer
        public init(payload: ByteBuffer) { self.payload = payload }
    }

    public struct ReturnStatusToken: TDSToken {
        public var type: TokenType = .returnStatus
        public var value: Int32
        public init(value: Int32) { self.value = value }
    }

    public struct ReturnValueToken: TDSToken {
        public var type: TokenType = .returnValue
        public var ordinal: UInt16
        public var name: String
        public var status: UInt8
        public var userType: UInt32
        public var flags: UInt16
        public var metadata: TDSTokens.ColMetadataToken.ColumnData
        public var value: ByteBuffer?
        
        public init(ordinal: UInt16, name: String, status: UInt8, userType: UInt32, flags: UInt16, metadata: TDSTokens.ColMetadataToken.ColumnData, value: ByteBuffer?) {
            self.ordinal = ordinal
            self.name = name
            self.status = status
            self.userType = userType
            self.flags = flags
            self.metadata = metadata
            self.value = value
        }

        public var paramOrdinal: UInt16 {
            ordinal
        }
    }

    public struct ColumnStatusToken: TDSToken {
        public var type: TokenType = .columnStatus
        public var status: UInt16
        public var data: [Byte]
        public init(status: UInt16, data: [Byte]) { self.status = status; self.data = data }
    }

    public struct SSPIToken: TDSToken {
        public var type: TokenType = .sspi
        public var data: Data
        public init(data: Data) { self.data = data }
    }

    public struct OrderToken: TDSToken {
        public var type: TokenType = .order
        public var columns: [UInt16]
        public init(columns: [UInt16]) { self.columns = columns }
        
        static func parse(from buffer: inout ByteBuffer) throws -> OrderToken {
            guard let length: UInt16 = buffer.readInteger(endianness: .little) else {
                throw TDSError.needMoreData
            }
            let count = Int(length) / 2
            var cols: [UInt16] = []
            for _ in 0..<count {
                guard let col: UInt16 = buffer.readInteger(endianness: .little) else {
                    throw TDSError.needMoreData
                }
                cols.append(col)
            }
            return OrderToken(columns: cols)
        }
    }
}
