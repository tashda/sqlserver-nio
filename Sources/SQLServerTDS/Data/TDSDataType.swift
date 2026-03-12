import NIOCore

public protocol Metadata: Sendable {
    var userType: UInt32 { get }
    var flags: UInt16 { get }
    var dataType: TDSDataType { get }
    var length: Int32 { get }
    var precision: UInt8 { get }
    var scale: UInt8 { get }
    var collation: [UInt8] { get }
}

public struct TypeMetadata: Metadata, Sendable {
    public var userType: UInt32
    public var flags: UInt16
    public var dataType: TDSDataType
    public var length: Int32
    public var precision: UInt8
    public var scale: UInt8
    public var collation: [UInt8]
    
    public init(userType: UInt32 = 0, flags: UInt16 = 0, dataType: TDSDataType, length: Int32 = 0, precision: UInt8 = 0, scale: UInt8 = 0, collation: [UInt8] = []) {
        self.userType = userType
        self.flags = flags
        self.dataType = dataType
        self.length = length
        self.precision = precision
        self.scale = scale
        self.collation = collation
    }
}

public enum TDSDataType: UInt8, Sendable {
    /// Fixed-Length Data Types
    case null = 0x1F
    case tinyInt = 0x30
    case bit = 0x32
    case smallInt = 0x34
    case int = 0x38
    case smallDateTime = 0x3A
    case real = 0x3B
    case money = 0x3C
    case datetime = 0x3D
    case float = 0x3E
    case smallMoney = 0x7A
    case bigInt = 0x7F

    /// Variable-Length Data Types
    case guid = 0x24
    case intn = 0x26
    case decimalLegacy = 0x37
    case numericLegacy = 0x3F
    case bitn = 0x68
    case decimal = 0x6A
    case numeric = 0x6C
    case floatn = 0x6D
    case moneyn = 0x6E
    case datetimen = 0x6F
    case date = 0x28
    case time = 0x29
    case datetime2 = 0x2A
    case datetimeOffset = 0x2B
    case charLegacy = 0x2F
    case varcharLegacy = 0x27
    case binaryLegacy = 0x2D
    case varbinaryLegacy = 0x25
    case varbinary = 0xA5
    case varchar = 0xA7
    case binary = 0xAD
    case char = 0xAF
    case nvarchar = 0xE7
    case nchar = 0xEF
    case xml = 0xF1
    case clrUdt = 0xF0
    case text = 0x23
    case nText = 0x63
    case image = 0x22
    case sqlVariant = 0x62
    case json = 0xF3
    case vector = 0xF4

    public func isCollationType() -> Bool {
        switch self {
        case .char, .varchar, .text, .nchar, .nvarchar, .nText: return true
        default: return false
        }
    }

    public func isPrecisionType() -> Bool {
        switch self {
        case .decimal, .numeric, .decimalLegacy, .numericLegacy: return true
        default: return false
        }
    }

    public func isScaleType() -> Bool {
        switch self {
        case .decimal, .numeric, .decimalLegacy, .numericLegacy, .time, .datetime2, .datetimeOffset: return true
        default: return false
        }
    }
}
