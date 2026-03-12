import NIOCore
import Foundation

private struct TDSSqlVariantComponents: Sendable {
    var baseMetadata: TypeMetadata
    var value: ByteBuffer?
}

extension TDSData {
    internal func sqlVariantResolved() -> TDSData? {
        guard self.metadata.dataType == .sqlVariant, var payload = self.value else {
            return nil
        }

        guard let typeByte: UInt8 = payload.readInteger() else {
            return nil
        }

        guard let dataType = TDSDataType(rawValue: typeByte) else {
            return nil
        }

        guard let propLen: UInt8 = payload.readInteger() else {
            return nil
        }

        guard var properties = payload.readSlice(length: Int(propLen)) else {
            return nil
        }

        var typeMetadata = TypeMetadata(dataType: dataType)

        switch dataType {
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            guard let precisionByte: UInt8 = properties.readInteger(),
                  let scaleByte: UInt8 = properties.readInteger() else {
                return nil
            }
            typeMetadata.precision = precisionByte
            typeMetadata.scale = scaleByte

        case .datetime2, .datetimeOffset, .time:
            guard let scaleByte: UInt8 = properties.readInteger() else {
                return nil
            }
            typeMetadata.scale = scaleByte

        case .char, .varchar, .charLegacy, .varcharLegacy,
             .nchar, .nvarchar, .text, .nText:
            guard let collation = properties.readBytes(length: 5) else {
                return nil
            }
            typeMetadata.collation = collation
            if properties.readableBytes >= 2 {
                if let len: Int16 = properties.readInteger(endianness: .little) {
                    typeMetadata.length = Int32(len)
                }
            }

        case .binary, .varbinary, .binaryLegacy, .varbinaryLegacy, .image:
            if properties.readableBytes >= 2 {
                if let len: Int16 = properties.readInteger(endianness: .little) {
                    typeMetadata.length = Int32(len)
                }
            }

        default:
            break
        }

        return TDSData(metadata: typeMetadata, value: payload)
    }
}
