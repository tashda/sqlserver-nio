
import NIOCore

extension ByteBuffer {
    public func getFloat(at index: Int, endianness: Endianness = .big) -> Float? {
        return self.getInteger(at: index, endianness: endianness, as: UInt32.self).map { Float(bitPattern: $0) }
    }

    public func getDouble(at index: Int, endianness: Endianness = .big) -> Double? {
        return self.getInteger(at: index, endianness: endianness, as: UInt64.self).map { Double(bitPattern: $0) }
    }
}
