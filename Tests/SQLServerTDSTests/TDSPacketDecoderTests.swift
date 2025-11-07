@testable import SQLServerTDS
import XCTest
import NIO
import NIOEmbedded
import Logging

final class TDSPacketDecoderTests: XCTestCase, @unchecked Sendable {
    func makeChannelWithDecoder() throws -> EmbeddedChannel {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds.decoder.tests")
        try channel.pipeline.addHandler(ByteToMessageHandler(TDSPacketDecoder(logger: logger)) as! (any Sendable & ChannelHandler)).wait()
        return channel
    }

    func testDecodeLastOnEmptyBufferDoesNotLoop() throws {
        let channel = try makeChannelWithDecoder()
        // If decodeLast spins when there is no data, this would hang.
        XCTAssertNoThrow(_ = try channel.finish())
    }

    func testDecodeLastOnNonPacketRemainderDoesNotLoop() throws {
        let channel = try makeChannelWithDecoder()
        // Write a few bytes that don't make a complete packet header.
        var buffer = ByteBufferAllocator().buffer(capacity: 3)
        buffer.writeBytes([0x12, 0x34, 0x56])
        XCTAssertNoThrow(try channel.writeInbound(buffer))

        // Closing the channel triggers decodeLast; should not spin.
        XCTAssertNoThrow(_ = try channel.finish())
    }
}

