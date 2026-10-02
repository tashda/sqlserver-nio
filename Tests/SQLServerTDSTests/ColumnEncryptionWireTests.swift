import XCTest
import NIOCore
@testable import SQLServerTDS

/// Always Encrypted on the wire (MS-TDS 2.2.7.4): with COLUMNENCRYPTION negotiated, COLMETADATA starts
/// with a CekTable and an encrypted column carries CryptoMetaData after its TYPE_INFO.
final class ColumnEncryptionWireTests: XCTestCase {
    private func writeBVarchar(_ text: String, into buffer: inout ByteBuffer) {
        let units = Array(text.utf16)
        buffer.writeInteger(UInt8(units.count))
        for unit in units { buffer.writeInteger(unit, endianness: .little) }
    }

    private func writeUSVarchar(_ text: String, into buffer: inout ByteBuffer) {
        let units = Array(text.utf16)
        buffer.writeInteger(UInt16(units.count), endianness: .little)
        for unit in units { buffer.writeInteger(unit, endianness: .little) }
    }

    /// Id int, then SSN nvarchar(11) encrypted deterministically (ciphertext varbinary(81)).
    private func colMetadata(cekEntries: Int = 1) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: 256)
        buffer.writeInteger(TDSTokens.TokenType.colMetadata.rawValue)
        buffer.writeInteger(UInt16(2), endianness: .little)
        // CekTable
        buffer.writeInteger(UInt16(cekEntries), endianness: .little)
        for _ in 0..<cekEntries {
            buffer.writeInteger(UInt32(5), endianness: .little)  // DatabaseId
            buffer.writeInteger(UInt32(1), endianness: .little)  // CekId
            buffer.writeInteger(UInt32(1), endianness: .little)  // CekVersion
            buffer.writeBytes([UInt8](repeating: 0x11, count: 8)) // CekMDVersion
            buffer.writeInteger(UInt8(1))                         // one encrypted value
            buffer.writeInteger(UInt16(3), endianness: .little)
            buffer.writeBytes([0xAB, 0xCD, 0xEF])                 // EncryptedKey
            writeBVarchar("MSSQL_CERTIFICATE_STORE", into: &buffer)
            writeUSVarchar("CurrentUser/My/0123456789ABCDEF", into: &buffer)
            writeBVarchar("RSA_OAEP", into: &buffer)
        }
        // Id int (INTN 4)
        buffer.writeInteger(UInt32(0), endianness: .little)
        buffer.writeInteger(UInt16(0x0008), endianness: .little)
        buffer.writeInteger(TDSDataType.intn.rawValue)
        buffer.writeInteger(UInt8(4))
        writeBVarchar("Id", into: &buffer)
        // SSN: ciphertext varbinary(81), fEncrypted
        buffer.writeInteger(UInt32(0), endianness: .little)
        buffer.writeInteger(UInt16(0x0809), endianness: .little)
        buffer.writeInteger(TDSDataType.varbinary.rawValue)
        buffer.writeInteger(UInt16(81), endianness: .little)
        // CryptoMetaData
        buffer.writeInteger(UInt16(0), endianness: .little)  // CekTable ordinal
        buffer.writeInteger(UInt32(0), endianness: .little)  // base UserType
        buffer.writeInteger(TDSDataType.nvarchar.rawValue)    // base TYPE_INFO: nvarchar(11), BIN2
        buffer.writeInteger(UInt16(22), endianness: .little)
        buffer.writeBytes([0x09, 0x04, 0xD0, 0x00, 0x00])
        buffer.writeInteger(UInt8(2))                         // AEAD_AES_256_CBC_HMAC_SHA_256
        buffer.writeInteger(UInt8(1))                         // deterministic
        buffer.writeInteger(UInt8(1))                         // normalization version
        writeBVarchar("SSN", into: &buffer)
        return buffer
    }

    func testEncryptedColumnIsDescribedAndItsRowReads() throws {
        var buffer = colMetadata()
        let ciphertext = [UInt8(0x01)] + [UInt8](repeating: 0x5A, count: 80)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt8(4)); buffer.writeInteger(UInt32(7), endianness: .little)
        buffer.writeInteger(UInt16(ciphertext.count), endianness: .little); buffer.writeBytes(ciphertext)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenOperations(streamParser: stream, logger: .init(label: "test"))
        parser.columnEncryption = true
        let tokens = try parser.parse()

        let metadata = try XCTUnwrap(tokens.first as? TDSTokens.ColMetadataToken)
        XCTAssertEqual(metadata.colData.map(\.colName), ["Id", "SSN"])
        XCTAssertNil(metadata.colData[0].encryption)
        let ssn = try XCTUnwrap(metadata.colData[1].encryption)
        XCTAssertEqual(metadata.colData[1].dataType, .varbinary)
        XCTAssertEqual(ssn.baseType.dataType, .nvarchar)
        XCTAssertEqual(ssn.baseType.length, 22)
        XCTAssertEqual(ssn.algorithm, "AEAD_AES_256_CBC_HMAC_SHA_256")
        XCTAssertEqual(ssn.kind, .deterministic)
        XCTAssertEqual(ssn.keyStoreName, "MSSQL_CERTIFICATE_STORE")
        XCTAssertEqual(ssn.keyPath, "CurrentUser/My/0123456789ABCDEF")

        let row = try XCTUnwrap(tokens.last as? TDSTokens.RowToken)
        XCTAssertEqual(row.colData[1].data.map { Array($0.readableBytesView) }, ciphertext)
    }

    func testAnEmptyCekTableBeforePlainColumns() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeInteger(TDSTokens.TokenType.colMetadata.rawValue)
        buffer.writeInteger(UInt16(1), endianness: .little)
        buffer.writeInteger(UInt16(0), endianness: .little) // CekTable: no keys
        buffer.writeInteger(UInt32(0), endianness: .little)
        buffer.writeInteger(UInt16(0x0008), endianness: .little)
        buffer.writeInteger(TDSDataType.intn.rawValue)
        buffer.writeInteger(UInt8(4))
        writeBVarchar("n", into: &buffer)
        let metadata = try TDSTokenOperations.parseColMetadataToken(from: &buffer, columnEncryption: true)
        XCTAssertEqual(metadata.colData.map(\.colName), ["n"])
        XCTAssertEqual(buffer.readableBytes, 0)
    }

    func testLoginAsksForColumnEncryptionOnlyWhenConfigured() throws {
        func featureExt(_ request: Bool) throws -> (flags3: UInt8, features: [UInt8]?) {
            var message = TDSMessages.Login7Message(username: "sa", password: "p", serverName: "s", database: "master")
            message.requestColumnEncryption = request
            var buffer = ByteBufferAllocator().buffer(capacity: 512)
            try message.serialize(into: &buffer)
            let flags3 = try XCTUnwrap(buffer.getInteger(at: 27, as: UInt8.self))
            // ibExtension is the sixth offset/length pair from offset 36; it points at a DWORD
            // holding the FeatureExt block's offset.
            let pointer = try XCTUnwrap(buffer.getInteger(at: 36 + 5 * 4, endianness: .little, as: UInt16.self))
            let length = try XCTUnwrap(buffer.getInteger(at: 36 + 5 * 4 + 2, endianness: .little, as: UInt16.self))
            guard length == 4 else { return (flags3, nil) }
            let start = try XCTUnwrap(buffer.getInteger(at: Int(pointer), endianness: .little, as: UInt32.self))
            return (flags3, buffer.getBytes(at: Int(start), length: buffer.writerIndex - Int(start)))
        }
        let on = try featureExt(true)
        XCTAssertEqual(on.flags3 & 0x10, 0x10, "fExtension")
        XCTAssertEqual(on.features, [0x04, 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF])
        let off = try featureExt(false)
        XCTAssertEqual(off.flags3 & 0x10, 0)
        XCTAssertNil(off.features)
    }

    func testBulkLoadMetadataCarriesAnEmptyCekTable() throws {
        let column = TDSColumnMetadata(userType: 0, flags: 0x0009, dataType: .intn, length: 4, precision: 0, scale: 0, colName: "id")
        var writer = TDSBulkLoadWriter(columns: [column], columnEncryption: true)
        try writer.appendRow([[1, 0, 0, 0]])
        var payload = writer.finished()
        let metadata = try TDSTokenOperations.parseColMetadataToken(from: &payload, columnEncryption: true)
        XCTAssertEqual(metadata.colData.map(\.colName), ["id"])
        XCTAssertEqual(payload.getInteger(at: payload.readerIndex, as: UInt8.self), TDSTokens.TokenType.row.rawValue)
    }

    func testEncryptedDestinationColumnsAreNotBulkLoaded() {
        var column = TDSColumnMetadata(userType: 0, flags: 0x0809, dataType: .varbinary, length: 81, precision: 0, scale: 0, colName: "SSN")
        column.encryption = .init(baseType: .init(dataType: .nvarchar, length: 22), algorithm: "AEAD_AES_256_CBC_HMAC_SHA_256", kind: .deterministic)
        XCTAssertFalse(TDSBulkLoadWriter.supports(column))
    }
}
