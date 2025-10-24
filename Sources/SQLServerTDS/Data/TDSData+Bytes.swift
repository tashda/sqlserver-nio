import Foundation
import NIO

extension TDSData {
    public var bytes: [UInt8]? {
        switch self.metadata.dataType {
        case .binaryLegacy, .varbinaryLegacy, .varbinary, .binary, .image:
            guard let buffer = self.value else {
                return nil
            }
            return buffer.getBytes(at: buffer.readerIndex, length: buffer.readableBytes)
        case .sqlVariant:
            return self.sqlVariantResolved()?.bytes
        default:
            return nil
        }
    }

    public var uuid: UUID? {
        switch self.metadata.dataType {
        case .guid:
            guard let buffer = self.value,
                  let raw = buffer.getBytes(at: buffer.readerIndex, length: 16)
            else {
                return nil
            }
            return raw.withUnsafeBytes { bytes -> UUID? in
                guard let base = bytes.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                    return nil
                }
                return UUID(uuid: (
                    base[0], base[1], base[2], base[3],
                    base[4], base[5], base[6], base[7],
                    base[8], base[9], base[10], base[11],
                    base[12], base[13], base[14], base[15]
                ))
            }
        case .sqlVariant:
            return self.sqlVariantResolved()?.uuid
        default:
            return nil
        }
    }
}
