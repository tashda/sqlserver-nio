import Foundation
import NIOCore

extension ByteBuffer {
    mutating func readUByte() throws -> UInt8 {
        guard let value: UInt8 = self.readInteger() else {
            throw TDSError.needMoreData
        }
        return value
    }

    mutating func readUShort() throws -> UInt16 {
        guard let value: UInt16 = self.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }
        return value
    }

    mutating func readULong() throws -> UInt32 {
        guard let value: UInt32 = self.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }
        return value
    }

    mutating func readByte() -> UInt8? {
        return self.readInteger()
    }

    mutating func writeUSVarChar(_ string: String) {
        self.writeInteger(UInt16(string.utf16.count), endianness: .little)
        self.writeUTF16String(string)
    }

    mutating func writeBVarChar(_ string: String) {
        self.writeInteger(UInt8(string.utf16.count))
        self.writeUTF16String(string)
    }

    mutating func writeUTF16String(_ string: String) {
        for codePoint in string.utf16 {
            self.writeInteger(codePoint, endianness: .little)
        }
    }

    mutating func readUTF16String(length: Int) -> String? {
        guard
            let bytes = self.readBytes(length: length),
            let utf16 = String(bytes: bytes, encoding: .utf16LittleEndian)
        else {
            return nil
        }
        return utf16
    }

    func getUTF16String(at position: Int, length: Int) -> String? {
        guard
            let bytes = self.getBytes(at: position, length: length)
        else {
            return nil
        }
        return String(bytes: bytes, encoding: .utf16LittleEndian)
    }

    mutating func writePLPBuffer(_ buffer: ByteBuffer) {
        self.writeInteger(UInt64(buffer.readableBytes), endianness: .little)
        var copy = buffer
        self.writeBuffer(&copy)
        self.writeInteger(UInt32(0), endianness: .little)
    }

    mutating func readPLPBytes() throws -> ByteBuffer? {
        guard let totalLength: UInt64 = self.readInteger(endianness: .little) else {
            throw TDSError.needMoreData
        }

        if totalLength == UInt64.max {
            return nil
        }

        let initialCapacity: Int
        if totalLength == UInt64.max - 1 {
            initialCapacity = 0
        } else if totalLength <= UInt64(Int.max) {
            initialCapacity = Int(totalLength)
        } else {
            throw TDSError.protocolError("PLP payload length exceeds supported buffer size")
        }

        var result = ByteBufferAllocator().buffer(capacity: initialCapacity)
        while true {
            guard let chunkLength: UInt32 = self.readInteger(endianness: .little) else {
                throw TDSError.needMoreData
            }

            if chunkLength == 0 || chunkLength == UInt32.max {
                break
            }

            guard var chunk = self.readSlice(length: Int(chunkLength)) else {
                throw TDSError.needMoreData
            }

            result.writeBuffer(&chunk)
        }

        return result
    }
}
