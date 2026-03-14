import XCTest
@testable import SQLServerTDS
import NIOCore

final class ReadOnlyIntentTests: XCTestCase {

    // MARK: - Login7Message Serialization

    func testLogin7TypeFlagsSetWhenReadOnly() throws {
        let message = TDSMessages.Login7Message(
            username: "sa",
            password: "pass",
            serverName: "localhost",
            database: "master",
            readOnlyIntent: true
        )

        var buffer = ByteBufferAllocator().buffer(capacity: 512)
        try message.serialize(into: &buffer)

        // Fixed header layout before flags:
        //   4 bytes  - Length
        //   4 bytes  - TDS version
        //   4 bytes  - Packet length negotiation
        //   4 bytes  - Client version
        //   4 bytes  - Client PID
        //   4 bytes  - Connection ID
        //   1 byte   - Flags1 (0xE0)
        //   1 byte   - Flags2 (optionFlags2)
        //   1 byte   - TypeFlags  <-- offset 26
        let typeFlagsOffset = 26
        let typeFlags = buffer.getInteger(at: typeFlagsOffset, as: UInt8.self)
        XCTAssertEqual(typeFlags, 0x20, "TypeFlags should have bit 5 set for readOnlyIntent")
    }

    func testLogin7TypeFlagsClearWhenNotReadOnly() throws {
        let message = TDSMessages.Login7Message(
            username: "sa",
            password: "pass",
            serverName: "localhost",
            database: "master",
            readOnlyIntent: false
        )

        var buffer = ByteBufferAllocator().buffer(capacity: 512)
        try message.serialize(into: &buffer)

        let typeFlagsOffset = 26
        let typeFlags = buffer.getInteger(at: typeFlagsOffset, as: UInt8.self)
        XCTAssertEqual(typeFlags, 0x00, "TypeFlags should be 0x00 when readOnlyIntent is false")
    }

    // MARK: - TDSLoginConfiguration

    func testConfigurationStoresReadOnlyIntent() {
        let config = TDSLoginConfiguration(
            serverName: "localhost",
            port: 1433,
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "pass"),
            readOnlyIntent: true
        )
        XCTAssertTrue(config.readOnlyIntent)
    }

    func testConfigurationDefaultsReadOnlyIntentToFalse() {
        let config = TDSLoginConfiguration(
            serverName: "localhost",
            port: 1433,
            database: "master",
            authentication: .sqlPassword(username: "sa", password: "pass")
        )
        XCTAssertFalse(config.readOnlyIntent)
    }
}
