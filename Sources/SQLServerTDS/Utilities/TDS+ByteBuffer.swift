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
        guard let totalLength: UInt64 = self.getInteger(at: self.readerIndex, endianness: .little) else {
            throw TDSError.needMoreData
        }

        if totalLength == UInt64.max {
            self.moveReaderIndex(forwardBy: 8)
            return nil
        }
        if totalLength != UInt64.max - 1, totalLength > UInt64(Int32.max) {
            throw TDSError.protocolError("PLP payload length \(totalLength) exceeds the supported size")
        }

        // Verify every chunk has arrived before copying anything. A large
        // value spans many packets, and it is re-examined each time a packet
        // arrives, so copying on every incomplete attempt would be quadratic.
        var index = self.readerIndex + 8
        var dataLength = 0
        while true {
            guard let chunkLength: UInt32 = self.getInteger(at: index, endianness: .little) else {
                throw TDSError.needMoreData
            }
            index += 4
            if chunkLength == 0 || chunkLength == UInt32.max {
                break
            }
            index += Int(chunkLength)
            dataLength += Int(chunkLength)
            guard index <= self.writerIndex else {
                throw TDSError.needMoreData
            }
        }

        self.moveReaderIndex(forwardBy: 8)
        var result = ByteBufferAllocator().buffer(capacity: dataLength)
        while true {
            let chunkLength: UInt32 = self.readInteger(endianness: .little)!
            if chunkLength == 0 || chunkLength == UInt32.max {
                break
            }
            var chunk = self.readSlice(length: Int(chunkLength))!
            result.writeBuffer(&chunk)
        }
        return result
    }
}
