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


final class TDSPacketDecoderFramingTests: XCTestCase, @unchecked Sendable {
    func testInvalidPacketLengthFailsInsteadOfStalling() throws {
        let channel = EmbeddedChannel()
        try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(TDSPacketDecoder(logger: Logger(label: "t"))))
        var buffer = ByteBufferAllocator().buffer(capacity: 8)
        buffer.writeBytes([0x04, 0x01, 0x00, 0x03, 0x00, 0x00, 0x01, 0x00]) // length 3 < header
        XCTAssertThrowsError(try channel.writeInbound(buffer))
    }

    func testEachPacketIsDeliveredWithoutWaitingForEndOfMessage() throws {
        let channel = EmbeddedChannel()
        try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(TDSPacketDecoder(logger: Logger(label: "t"))))
        var buffer = ByteBufferAllocator().buffer(capacity: 32)
        buffer.writeBytes([0x04, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x01, 0x00, 0xAA, 0xBB]) // not EOM
        buffer.writeBytes([0x04, 0x01, 0x00, 0x09, 0x00, 0x00, 0x02, 0x00, 0xCC])       // EOM
        try channel.writeInbound(buffer)
        let first = try XCTUnwrap(channel.readInbound(as: TDSPacketChunk.self))
        XCTAssertEqual(first.payload.readableBytes, 2)
        XCTAssertFalse(first.isEndOfMessage)
        let second = try XCTUnwrap(channel.readInbound(as: TDSPacketChunk.self))
        XCTAssertEqual(second.payload.readableBytes, 1)
        XCTAssertTrue(second.isEndOfMessage)
    }
}
