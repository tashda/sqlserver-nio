import Foundation
import NIO

extension TDSData {
    public init(decimal value: Decimal, precision: Int, scale: Int) throws {
        // Support up to 18 digits safely using Int64 packing for now
        guard precision >= 1 && precision <= 38 else { throw TDSError.protocolError("Decimal precision out of range: \(precision)") }
        guard scale >= 0 && scale <= precision else { throw TDSError.protocolError("Decimal scale out of range: \(scale)") }

        // Scale the decimal by 10^scale to get an integer payload
        var src = value
        var scaled = Decimal()
        NSDecimalMultiplyByPowerOf10(&scaled, &src, Int16(scale), .plain)

        // Extract sign and magnitude
        let isNegative = (scaled as NSDecimalNumber).compare(0) == .orderedAscending
        let absScaled = (scaled as NSDecimalNumber).multiplying(by: isNegative ? -1 : 1)

        // For now, restrict to 64-bit magnitude; extend later to full 38-digit packing
        guard let int64 = Int64(exactly: absScaled) else {
            throw TDSError.protocolError("Decimal magnitude exceeds supported range (<= 18 digits)")
        }

        // Determine storage size (without sign byte) based on precision
        let dataBytes: Int
        switch precision {
        case 1...9: dataBytes = 4
        case 10...19: dataBytes = 8
        case 20...28: dataBytes = 12
        default: dataBytes = 16
        }

        var buf = ByteBufferAllocator().buffer(capacity: dataBytes + 1)
        // Value ByteLen written by caller (RPC writer); here we only store payload (sign + magnitude)
        buf.writeInteger(UInt8(isNegative ? 0 : 1)) // sign: 0 = negative, 1 = positive
        // Write magnitude little-endian into dataBytes bytes
        var tmp = int64
        var written = 0
        while written < dataBytes {
            buf.writeInteger(UInt8(truncatingIfNeeded: tmp & 0xFF))
            tmp >>= 8
            written += 1
        }

        let meta = TypeMetadata(userType: 0, flags: 0, dataType: .decimal, collation: [], precision: precision, scale: scale)
        self.init(metadata: meta, value: buf)
    }

    public var decimal: Decimal? {
        guard var value = self.value else { return nil }
        switch self.metadata.dataType {
        case .decimal, .numeric, .decimalLegacy, .numericLegacy:
            // Read sign + magnitude; infer bytes by remaining length
            guard let sign = value.readInteger(as: UInt8.self) else { return nil }
            let bytes = value.readBytes(length: value.readableBytes) ?? []
            // Accumulate magnitude as Decimal: sum(b[i] * 256^i)
            var magnitude = Decimal(0)
            var factor = Decimal(1)
            for b in bytes {
                magnitude += Decimal(Int(b)) * factor
                factor *= 256
            }
            let negative = (sign == 0)
            var result = magnitude
            let scale = self.metadata.scale ?? 0
            var input = magnitude
            NSDecimalMultiplyByPowerOf10(&result, &input, Int16(-scale), .plain)
            if negative { result = -result }
            return result
        default:
            return nil
        }
    }
}

// A minimal 128-bit unsigned to aid magnitude math; Swift has no std UInt128
// No fixed UInt128 needed with Decimal accumulation
