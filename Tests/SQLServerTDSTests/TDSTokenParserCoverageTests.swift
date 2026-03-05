import XCTest
@testable import SQLServerTDS
import NIO

/// Tests token types and row formats that were previously uncovered.
/// All byte sequences are crafted to match the TDS wire format exactly.
final class TDSTokenParserCoverageTests: XCTestCase {

    // MARK: - EnvChange

    func testEnvChangeDatabaseSwitch() throws {
        // ENVCHANGE: type=database(1), new="master"(6 chars), old=""(0 chars)
        // Content = 1(type) + 1+12(new BVarchar) + 1(old BVarchar) = 15 bytes
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeInteger(TDSTokens.TokenType.envchange.rawValue)
        buffer.writeInteger(UInt16(15), endianness: .little)    // content length
        buffer.writeInteger(UInt8(1))                           // type: database
        buffer.writeInteger(UInt8(6))                           // new: 6 chars
        buffer.writeUTF16String("master")
        buffer.writeInteger(UInt8(0))                           // old: 0 chars

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let token = try XCTUnwrap(tokens[0] as? TDSTokens.EnvchangeToken<String>)
        XCTAssertEqual(token.envchangeType, .database)
        XCTAssertEqual(token.newValue, "master")
        XCTAssertEqual(token.oldValue, "")
    }

    func testEnvChangePacketSize() throws {
        // ENVCHANGE: type=packetSize(4), new="8192", old="4096"
        let newVal = "8192", oldVal = "4096"
        let contentLen = 1 + (1 + newVal.count * 2) + (1 + oldVal.count * 2)

        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeInteger(TDSTokens.TokenType.envchange.rawValue)
        buffer.writeInteger(UInt16(contentLen), endianness: .little)
        buffer.writeInteger(UInt8(4))                   // type: packetSize
        buffer.writeInteger(UInt8(newVal.count))
        buffer.writeUTF16String(newVal)
        buffer.writeInteger(UInt8(oldVal.count))
        buffer.writeUTF16String(oldVal)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let token = try XCTUnwrap(tokens[0] as? TDSTokens.EnvchangeToken<String>)
        XCTAssertEqual(token.envchangeType, .packetSize)
        XCTAssertEqual(token.newValue, "8192")
        XCTAssertEqual(token.oldValue, "4096")
    }

    // MARK: - Error / Info tokens

    func testErrorTokenAllFields() throws {
        // ERROR token: number=515, state=2, class=16, msg="err", server="S", proc="", line=1
        var content = ByteBufferAllocator().buffer(capacity: 32)
        content.writeInteger(Int32(515), endianness: .little)   // number (Long = Int32)
        content.writeInteger(UInt8(2))                          // state
        content.writeInteger(UInt8(16))                         // class (severity)
        content.writeInteger(UInt16(3), endianness: .little)    // msgText: 3 chars (USVarchar)
        content.writeUTF16String("err")
        content.writeInteger(UInt8(1))                          // serverName: 1 char (BVarchar)
        content.writeUTF16String("S")
        content.writeInteger(UInt8(0))                          // procName: 0 chars
        content.writeInteger(Int32(1), endianness: .little)     // lineNumber (Long = Int32)

        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        buffer.writeInteger(TDSTokens.TokenType.error.rawValue)
        buffer.writeInteger(UInt16(content.readableBytes), endianness: .little)
        buffer.writeBuffer(&content)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let error = try XCTUnwrap(tokens[0] as? TDSTokens.ErrorInfoToken)
        XCTAssertEqual(error.type, .error)
        XCTAssertEqual(error.number, 515)
        XCTAssertEqual(error.state, 2)
        XCTAssertEqual(error.classValue, 16)
        XCTAssertEqual(error.messageText, "err")
        XCTAssertEqual(error.serverName, "S")
        XCTAssertEqual(error.procedureName, "")
        XCTAssertEqual(error.lineNumber, 1)
    }

    func testInfoTokenParsing() throws {
        // INFO token: same wire format as ERROR, different token type
        var content = ByteBufferAllocator().buffer(capacity: 32)
        content.writeInteger(Int32(5701), endianness: .little)  // number (changed database)
        content.writeInteger(UInt8(1))                          // state
        content.writeInteger(UInt8(0))                          // class (info = 0)
        content.writeInteger(UInt16(2), endianness: .little)    // msgText: 2 chars
        content.writeUTF16String("ok")
        content.writeInteger(UInt8(0))                          // serverName: 0 chars
        content.writeInteger(UInt8(0))                          // procName: 0 chars
        content.writeInteger(Int32(0), endianness: .little)     // lineNumber

        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        buffer.writeInteger(TDSTokens.TokenType.info.rawValue)
        buffer.writeInteger(UInt16(content.readableBytes), endianness: .little)
        buffer.writeBuffer(&content)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let info = try XCTUnwrap(tokens[0] as? TDSTokens.ErrorInfoToken)
        XCTAssertEqual(info.type, .info)
        XCTAssertEqual(info.number, 5701)
        XCTAssertEqual(info.messageText, "ok")
        XCTAssertEqual(info.classValue, 0)
    }

    // MARK: - LoginAck

    func testLoginAckTokenParsing() throws {
        // LoginAck: interface=1, tdsVersion=TDS7.4(big-endian), progName="SQL"(3 chars), ver=15.0.0.0
        var content = ByteBufferAllocator().buffer(capacity: 32)
        content.writeInteger(UInt8(1))              // interface: SQL Server
        content.writeInteger(UInt32(0x74000004))    // tdsVersion TDS 7.4 (DWord = big-endian)
        content.writeInteger(UInt8(3))              // progName: 3 chars
        content.writeUTF16String("SQL")
        content.writeInteger(UInt8(15))             // majorVer
        content.writeInteger(UInt8(0))              // minorVer
        content.writeInteger(UInt8(0))              // buildNumHi
        content.writeInteger(UInt8(0))              // buildNumLow

        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        buffer.writeInteger(TDSTokens.TokenType.loginAck.rawValue)
        buffer.writeInteger(UInt16(content.readableBytes), endianness: .little)
        buffer.writeBuffer(&content)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let ack = try XCTUnwrap(tokens[0] as? TDSTokens.LoginAckToken)
        XCTAssertEqual(ack.interface, 1)
        XCTAssertEqual(ack.tdsVersion, 0x74000004)
        XCTAssertEqual(ack.progName, "SQL")
        XCTAssertEqual(ack.majorVer, 15)
        XCTAssertEqual(ack.minorVer, 0)
    }

    // MARK: - DoneInProc / DoneProc

    func testDoneInProcToken() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 13)
        buffer.writeInteger(TDSTokens.TokenType.doneInProc.rawValue)
        buffer.writeInteger(UInt16(0x20), endianness: .little)  // status: row count valid
        buffer.writeInteger(UInt16(0xC1), endianness: .little)  // curCmd: SELECT
        buffer.writeInteger(UInt64(10), endianness: .little)    // rowcount: 10

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let token = try XCTUnwrap(parser.parseDoneToken())
        XCTAssertEqual(token.type, .doneInProc)
        XCTAssertEqual(token.status, 0x20)
        XCTAssertEqual(token.curCmd, 0xC1)
        XCTAssertEqual(token.doneRowCount, 10)
    }

    func testDoneProcToken() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 13)
        buffer.writeInteger(TDSTokens.TokenType.doneProc.rawValue)
        buffer.writeInteger(UInt16(0), endianness: .little)
        buffer.writeInteger(UInt16(0), endianness: .little)
        buffer.writeInteger(UInt64(0), endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))

        let token = try XCTUnwrap(parser.parseDoneToken())
        XCTAssertEqual(token.type, .doneProc)
        XCTAssertEqual(token.doneRowCount, 0)
    }

    func testParseDoneTokenRejectsNonDoneTokenType() throws {
        // Feeding a ROW token to parseDoneToken must return nil and not consume bytes.
        var buffer = ByteBufferAllocator().buffer(capacity: 13)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeBytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        let positionBefore = stream.position

        let result = try parser.parseDoneToken()
        XCTAssertNil(result)
        XCTAssertEqual(stream.position, positionBefore, "Position must not advance on rejection")
    }

    // MARK: - Multi-column NBC row

    func testNbcRowThreeColumnsWithMiddleNull() throws {
        // 3 columns: nvarchar "Hi" | int NULL | varchar "bye"
        // Null bitmap: bit0=0 (col0 present), bit1=1 (col1 null), bit2=0 (col2 present) → 0x02
        let cols: [TDSTokens.ColMetadataToken.ColumnData] = [
            .init(userType: 0, flags: 0, dataType: .nvarchar, length: 100, collation: [], tableName: nil, colName: "name", precision: nil, scale: nil),
            .init(userType: 0, flags: 0, dataType: .int,      length: 4,   collation: [], tableName: nil, colName: "age",  precision: nil, scale: nil),
            .init(userType: 0, flags: 0, dataType: .varchar,  length: 50,  collation: [], tableName: nil, colName: "code", precision: nil, scale: nil),
        ]
        let meta = TDSTokens.ColMetadataToken(count: 3, colData: cols)

        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeInteger(TDSTokens.TokenType.nbcRow.rawValue)
        buffer.writeInteger(UInt8(0x02))                         // null bitmap
        buffer.writeInteger(UInt16(4), endianness: .little)      // col0: "Hi" = 4 bytes UTF-16LE
        buffer.writeUTF16String("Hi")
        // col1 omitted (null)
        buffer.writeInteger(UInt16(3), endianness: .little)      // col2: "bye" = 3 bytes
        buffer.writeBytes([0x62, 0x79, 0x65])

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let row = try XCTUnwrap(tokens[0] as? TDSTokens.NbcRowToken)
        XCTAssertEqual(row.colData.count, 3)
        XCTAssertEqual(row.nullBitmap, [0x02])

        // col0: "Hi"
        var col0 = try XCTUnwrap(row.colData[0].data)
        XCTAssertEqual(col0.readUTF16String(length: col0.readableBytes), "Hi")

        // col1: null
        XCTAssertNil(row.colData[1].data)

        // col2: "bye"
        XCTAssertEqual(
            row.colData[2].data?.getBytes(at: row.colData[2].data!.readerIndex, length: 3),
            [0x62, 0x79, 0x65]
        )
    }

    func testNbcRowEightColumnsNullBitmapSpansTwoBytes() throws {
        // 9 columns, all null: bitmap = 2 bytes, both 0xFF
        let cols: [TDSTokens.ColMetadataToken.ColumnData] = (0..<9).map { i in
            .init(userType: 0, flags: 0, dataType: .intn, length: 4, collation: [], tableName: nil, colName: "c\(i)", precision: nil, scale: nil)
        }
        let meta = TDSTokens.ColMetadataToken(count: 9, colData: cols)

        var buffer = ByteBufferAllocator().buffer(capacity: 4)
        buffer.writeInteger(TDSTokens.TokenType.nbcRow.rawValue)
        buffer.writeInteger(UInt8(0xFF))   // cols 0–7: all null
        buffer.writeInteger(UInt8(0x01))   // col 8: null (bit 0 of second byte)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta

        let tokens = try parser.parse()
        XCTAssertEqual(tokens.count, 1)
        let row = try XCTUnwrap(tokens[0] as? TDSTokens.NbcRowToken)
        XCTAssertEqual(row.colData.count, 9)
        XCTAssertTrue(row.colData.allSatisfy { $0.data == nil }, "All 9 columns must be null")
    }

    // MARK: - floatn column through row parser

    func testFloatnFourByteRowColumn() throws {
        // floatn(4): ByteLen=4, then 4-byte LE IEEE 754 float
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .floatn, length: 4,
            collation: [], tableName: nil, colName: "val", precision: nil, scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [col])

        var buffer = ByteBufferAllocator().buffer(capacity: 8)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt8(4))
        buffer.writeInteger(Float(1.5).bitPattern, endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta

        let row = try XCTUnwrap(parser.parseRowToken())
        let colData = try XCTUnwrap(row.colData[0].data)
        let tdsData = TDSData(metadata: TypeMetadata(dataType: .floatn), value: colData)
        XCTAssertEqual(tdsData.double!, Double(Float(1.5)), accuracy: 1e-6)
    }

    func testFloatnEightByteRowColumn() throws {
        // floatn(8): ByteLen=8, then 8-byte LE IEEE 754 double
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .floatn, length: 8,
            collation: [], tableName: nil, colName: "val", precision: nil, scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [col])

        var buffer = ByteBufferAllocator().buffer(capacity: 12)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt8(8))
        buffer.writeInteger(Double(2.718281828).bitPattern, endianness: .little)

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta

        let row = try XCTUnwrap(parser.parseRowToken())
        let colData = try XCTUnwrap(row.colData[0].data)
        let tdsData = TDSData(metadata: TypeMetadata(dataType: .floatn), value: colData)
        XCTAssertEqual(tdsData.double!, 2.718281828, accuracy: 1e-9)
    }

    func testFloatnNullByteLen() throws {
        // floatn with ByteLen=0 signals NULL
        let col = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0, flags: 0, dataType: .floatn, length: 8,
            collation: [], tableName: nil, colName: "val", precision: nil, scale: nil
        )
        let meta = TDSTokens.ColMetadataToken(count: 1, colData: [col])

        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeInteger(TDSTokens.TokenType.row.rawValue)
        buffer.writeInteger(UInt8(0))   // ByteLen=0 → NULL

        let stream = TDSStreamParser()
        stream.buffer.writeBuffer(&buffer)
        let parser = TDSTokenParser(streamParser: stream, logger: .init(label: "test"))
        parser.colMetadata = meta

        let row = try XCTUnwrap(parser.parseRowToken())
        XCTAssertNil(row.colData[0].data)
    }
}
