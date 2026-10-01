import XCTest
@testable import SQLServerTDS
import NIOCore

/// LOGIN7's variable fields sit in MS-TDS order: HostName, UserName, Password, AppName, ServerName,
/// Extension, CltIntName, Language, Database. Each has an offset and a character count from offset 36.
final class Login7FieldTests: XCTestCase {
    private func field(_ index: Int, of buffer: ByteBuffer) throws -> String {
        let entry = 36 + index * 4
        let offset = try XCTUnwrap(buffer.getInteger(at: entry, endianness: .little, as: UInt16.self))
        let count = try XCTUnwrap(buffer.getInteger(at: entry + 2, endianness: .little, as: UInt16.self))
        let bytes = try XCTUnwrap(buffer.getBytes(at: Int(offset), length: Int(count) * 2))
        return String(decoding: stride(from: 0, to: bytes.count, by: 2).map { UInt16(bytes[$0]) | UInt16(bytes[$0 + 1]) << 8 }, as: UTF16.self)
    }

    func testApplicationNameIsAppNameAndTheDriverIsTheClientInterface() throws {
        var message = TDSMessages.Login7Message(username: "sa", password: "p", serverName: "sql01", database: "Sales")
        message.applicationName = "Echo"
        var buffer = ByteBufferAllocator().buffer(capacity: 512)
        try message.serialize(into: &buffer)

        XCTAssertEqual(try field(1, of: buffer), "sa")
        XCTAssertEqual(try field(3, of: buffer), "Echo")          // AppName: APP_NAME(), program_name
        XCTAssertEqual(try field(4, of: buffer), "sql01")         // ServerName
        XCTAssertEqual(try field(6, of: buffer), "sqlserver-nio") // CltIntName: client_interface_name
        XCTAssertEqual(try field(7, of: buffer), "")              // Language
        XCTAssertEqual(try field(8, of: buffer), "Sales")         // Database
    }
}
