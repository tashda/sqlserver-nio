import NIOCore
import Foundation

extension TDSData {
    private init<I: FixedWidthInteger>(fwi: I) {
        let capacity: Int
        switch I.bitWidth {
        case 8:
            capacity = 1
        case 16:
            capacity = 2
        case 32:
            capacity = 4
        case 64:
            capacity = 8
        default:
            fatalError("Cannot encode \(I.self) to TDSData")
        }

        var buffer = ByteBufferAllocator().buffer(capacity: capacity)
        buffer.writeInteger(fwi, endianness: .little)
        self.init(metadata: I.tdsMetadata, value: buffer)
    }

    public init(int value: Int) {
        self.init(fwi: value)
    }

    public init(int8 value: Int8) {
        self.init(fwi: value)
    }

    public init(int16 value: Int16) {
        self.init(fwi: value)
    }

    public init(int32 value: Int32) {
        self.init(fwi: value)
    }

    public init(int64 value: Int64) {
        self.init(fwi: value)
    }

    public init(uint value: UInt) {
        self.init(fwi: value)
    }

    public init(uint8 value: UInt8) {
        self.init(fwi: value)
    }

    public init(uint16 value: UInt16) {
        self.init(fwi: value)
    }

    public init(uint32 value: UInt32) {
        self.init(fwi: value)
    }

    public init(uint64 value: UInt64) {
        self.init(fwi: value)
    }

    public var int: Int? {
        return fwi()
    }

    public var int64: Int64? {
        return fwi()
    }

    public var int8: Int8? {
        return fwi()
    }

    public var int16: Int16? {
        return fwi()
    }

    public var int32: Int32? {
        return fwi()
    }

    public var uint: UInt? {
        return fwi()
    }

    public var uint8: UInt8? {
        return fwi()
    }

    public var uint16: UInt16? {
        return fwi()
    }

    public var uint32: UInt32? {
        return fwi()
    }

    public var uint64: UInt64? {
        return fwi()
    }

    private func fwi<I>(_ type: I.Type = I.self) -> I?
        where I: FixedWidthInteger
    {
        if self.metadata.dataType == .sqlVariant {
            return self.sqlVariantResolved()?.fwi(type)
        }
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .bit:
            guard value.readableBytes == 1,
                  let byte = value.getInteger(at: value.readerIndex, as: UInt8.self) else {
                return nil
            }
            return I(byte)
        case .bitn:
            switch value.readableBytes {
            case 0:
                return nil
            case 1:
                guard let byte = value.getInteger(at: value.readerIndex, as: UInt8.self) else {
                    return nil
                }
                return I(byte)
            default:
                return nil
            }
        case .tinyInt:
            guard value.readableBytes == 1,
                  let uint8 = value.getInteger(at: value.readerIndex, as: UInt8.self) else {
                return nil
            }
            return I(exactly: uint8)
        case .smallInt:
            guard value.readableBytes == 2,
                  let int16 = value.readInteger(endianness: .little, as: Int16.self) else {
                return nil
            }
            return I(exactly: int16)
        case .int:
            guard value.readableBytes == 4,
                  let int32 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int32.self) else {
                return nil
            }
            return I(exactly: int32)
        case .bigInt:
            guard value.readableBytes == 8,
                  let int64 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int64.self) else {
                return nil
            }
            return I(exactly: int64)
        case .intn:
            switch value.readableBytes {
            case 0:
                return nil
            case 1:
                guard let uint8 = value.getInteger(at: value.readerIndex, as: UInt8.self) else {
                    return nil
                }
                return I(exactly: uint8)
            case 2:
                guard let int16 = value.readInteger(endianness: .little, as: Int16.self) else {
                    return nil
                }
                return I(exactly: int16)
            case 4:
                guard let int32 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int32.self) else {
                    return nil
                }
                return I(exactly: int32)
            case 8:
                guard let int64 = value.getInteger(at: value.readerIndex, endianness: .little, as: Int64.self) else {
                    return nil
                }
                return I(exactly: int64)
            default:
                return nil
            }
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            guard let decimal = self.decimal else {
                return nil
            }
            var integral = Decimal()
            var working = decimal
            NSDecimalRound(&integral, &working, 0, .plain)
            guard integral == decimal else {
                return nil
            }
            return I(exactly: NSDecimalNumber(decimal: integral).int64Value)
        default:
            return nil
        }
    }
}

extension Int: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .int)
    }

    public init?(tdsData: TDSData) {
        guard let int = tdsData.int else {
            return nil
        }
        self = int
    }

    public var tdsData: TDSData? {
        return .init(int: self)
    }
}

extension Int8: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .tinyInt)
    }

    public init?(tdsData: TDSData) {
        guard let int = tdsData.int else { return nil }
        self = Int8(int)
    }

    public var tdsData: TDSData? {
        return .init(int8: self)
    }
}

extension Int16: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .smallInt)
    }

    public init?(tdsData: TDSData) {
        guard let int = tdsData.int else { return nil }
        self = Int16(int)
    }

    public var tdsData: TDSData? {
        return .init(int16: self)
    }
}

extension Int32: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .int)
    }

    public init?(tdsData: TDSData) {
        guard let int = tdsData.int else { return nil }
        self = Int32(int)
    }

    public var tdsData: TDSData? {
        return .init(int32: self)
    }
}

extension Int64: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .bigInt)
    }

    public init?(tdsData: TDSData) {
        guard let int = tdsData.int else { return nil }
        self = Int64(int)
    }

    public var tdsData: TDSData? {
        return .init(int64: self)
    }
}

extension UInt8: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .tinyInt)
    }

    public init?(tdsData: TDSData) {
        guard let int = tdsData.int else { return nil }
        self = UInt8(int)
    }

    public var tdsData: TDSData? {
        return .init(uint8: self)
    }
}

extension FixedWidthInteger {
    public static var tdsMetadata: any Metadata {
        let dataType: TDSDataType
        switch self.bitWidth {
        case 8:
            dataType = .tinyInt
        case 16:
            dataType = .smallInt
        case 32:
            dataType = .int
        case 64:
            dataType = .bigInt
        default:
            fatalError("\(self.bitWidth) not supported")
        }
        return TypeMetadata(dataType: dataType)
    }
}

extension TDSData: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int) {
        self.init(int: value)
    }
}
