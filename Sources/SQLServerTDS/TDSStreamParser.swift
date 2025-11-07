
import NIOCore

public class TDSStreamParser {
    public var buffer: ByteBuffer
    public var position: Int

    public init() {
        self.buffer = ByteBufferAllocator().buffer(capacity: 0)
        self.position = 0
    }

    public func waitForChunk() {
        // This will be implemented later
    }

    public func readUInt8() -> UInt8? {
        guard buffer.readableBytes >= 1 else {
            return nil
        }
        let value = buffer.getInteger(at: position, as: UInt8.self)
        position += 1
        return value
    }

    public func peekUInt8() -> UInt8? {
        guard buffer.readableBytes >= 1 else {
            return nil
        }
        return buffer.getInteger(at: position, as: UInt8.self)
    }

    public func readUInt16LE() -> UInt16? {
        guard buffer.readableBytes >= 2 else {
            return nil
        }
        let value = buffer.getInteger(at: position, endianness: .little, as: UInt16.self)
        position += 2
        return value
    }

    public func readBVarChar() -> String? {
        guard let length = readUInt8() else {
            return nil
        }

        guard buffer.readableBytes >= Int(length) * 2 else {
            return nil
        }

        var result = ""
        for _ in 0..<length {
            if let charCode = readUInt16LE() {
                result.append(Character(UnicodeScalar(charCode)!))
            } else {
                return nil
            }
        }
        return result
    }

    public func readUsVarChar() -> String? {
        guard let length = readUInt16LE() else {
            return nil
        }

        guard buffer.readableBytes >= Int(length) * 2 else {
            return nil
        }

        var result = ""
        for _ in 0..<length {
            if let charCode = readUInt16LE() {
                result.append(Character(UnicodeScalar(charCode)!))
            } else {
                return nil
            }
        }
        return result
    }

    public func readUInt32LE() -> UInt32? {
        guard buffer.readableBytes >= 4 else {
            return nil
        }
        let value = buffer.getInteger(at: position, endianness: .little, as: UInt32.self)
        position += 4
        return value
    }

    public func readUInt64LE() -> UInt64? {
        guard buffer.readableBytes >= 8 else {
            return nil
        }
        let value = buffer.getInteger(at: position, endianness: .little, as: UInt64.self)
        position += 8
        return value
    }

    public func readInt32LE() -> Int32? {
        guard buffer.readableBytes >= 4 else {
            return nil
        }
        let value = buffer.getInteger(at: position, endianness: .little, as: Int32.self)
        position += 4
        return value
    }

    public func readFloatLE() -> Float? {
        guard buffer.readableBytes >= 4 else {
            return nil
        }
        let value = buffer.getFloat(at: position, endianness: .little)
        position += 4
        return value
    }

    public func readDoubleLE() -> Double? {
        guard buffer.readableBytes >= 8 else {
            return nil
        }
        let value = buffer.getDouble(at: position, endianness: .little)
        position += 8
        return value
    }

    public func readBytes(count: Int) -> [UInt8]? {
        guard buffer.readableBytes >= count else {
            return nil
        }
        let value = buffer.getBytes(at: position, length: count)
        position += count
        return value
    }
}
