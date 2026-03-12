import NIOCore
import Foundation

extension TDSData {
    public init(double: Double) {
        var buffer = ByteBufferAllocator().buffer(capacity: 8)
        buffer.writeInteger(double.bitPattern, endianness: .little)
        self.init(metadata: Double.tdsMetadata, value: buffer)
    }

    public var double: Double? {
        if self.metadata.dataType == .sqlVariant {
            return self.sqlVariantResolved()?.double
        }

        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .float:
            return value.getInteger(at: value.readerIndex, endianness: .little, as: UInt64.self).map(Double.init(bitPattern:))
        case .real:
            return value.getInteger(at: value.readerIndex, endianness: .little, as: UInt32.self).map { Double(Float(bitPattern: $0)) }
        case .floatn:
            switch value.readableBytes {
            case 0:
                return nil
            case 4:
                return value.getInteger(at: value.readerIndex, endianness: .little, as: UInt32.self).map { Double(Float(bitPattern: $0)) }
            case 8:
                return value.getInteger(at: value.readerIndex, endianness: .little, as: UInt64.self).map(Double.init(bitPattern:))
            default:
                return nil
            }
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            return self.decimal.map { ($0 as NSDecimalNumber).doubleValue }
        case .smallMoney:
            guard let raw = value.readInteger(endianness: .little, as: Int32.self) else {
                return nil
            }
            return Double(raw) / 10_000.0
        case .money:
            guard let high = value.readInteger(endianness: .little, as: Int32.self),
                  let low = value.readInteger(endianness: .little, as: UInt32.self) else {
                return nil
            }
            let combined = (Int64(high) << 32) | Int64(low)
            return Double(combined) / 10_000.0
        case .moneyn:
            switch value.readableBytes {
            case 0:
                return nil
            case 4:
                guard let raw = value.readInteger(endianness: .little, as: Int32.self) else {
                    return nil
                }
                return Double(raw) / 10_000.0
            case 8:
                guard let high = value.readInteger(endianness: .little, as: Int32.self),
                      let low = value.readInteger(endianness: .little, as: UInt32.self) else {
                    return nil
                }
                let combined = (Int64(high) << 32) | Int64(low)
                return Double(combined) / 10_000.0
            default:
                return nil
            }
        case .int, .bigInt, .smallInt, .tinyInt:
            return self.int.map { Double($0) }
        default:
            return nil
        }
    }
}

extension Double: TDSDataConvertible {
    public static var tdsMetadata: any Metadata {
        return TypeMetadata(dataType: .float)
    }

    public init?(tdsData: TDSData) {
        guard let double = tdsData.double else {
            return nil
        }
        self = double
    }

    public var tdsData: TDSData? {
        return .init(double: self)
    }
}
