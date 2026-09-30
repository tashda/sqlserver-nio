@testable import SQLServerTDS
import Logging
import NIOCore
import NIOEmbedded
import XCTest

/// Deterministic tests of the request pipeline state machine. They drive the
/// handler with hand-built server responses, so the ordering races that only
/// show up intermittently against a real server are reproduced exactly.
final class TDSRequestPipelineTests: XCTestCase, @unchecked Sendable {
    private var channel: EmbeddedChannel!
    private var handler: TDSRequestHandler!
    private var reads: ReadCounter!

    override func setUp() async throws {
        let logger = Logger(label: "tds.pipeline.tests")
        channel = EmbeddedChannel()
        reads = ReadCounter()
        handler = TDSRequestHandler(
            logger: logger,
            firstDecoder: ByteToMessageHandler(TDSPacketDecoder(logger: logger)),
            firstEncoder: MessageToByteHandler(TDSPacketEncoder(logger: logger)),
            firstDecoderName: "decoder",
            firstEncoderName: "encoder",
            pipelineCoordinatorName: "coordinator"
        )
        try channel.pipeline.syncOperations.addHandler(reads)
        try channel.pipeline.syncOperations.addHandler(handler)
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 1433)).wait()
    }

    override func tearDown() async throws {
        _ = try? channel.finish()
    }

    // MARK: - Helpers

    private func submit(_ sql: String, timeout: TimeAmount? = nil) -> TDSRequestContext {
        let loop = channel.eventLoop
        let completion = loop.makePromise(of: Void.self)
        let context = TDSRequestContext(
            delegate: RawSqlRequest(sql: sql),
            completionPromise: completion,
            resultPromise: loop.makePromise(of: [TDSData].self),
            tokenHandler: RequestTokenHandler(promise: completion, onRow: nil, onMetadata: nil, onDone: nil, onMessage: nil, onReturnValue: nil)
        )
        context.timeout = timeout
        channel.write(context, promise: nil)
        channel.flush()
        return context
    }

    private func outboundPacketTypes() throws -> [TDSPacket.HeaderType] {
        var types: [TDSPacket.HeaderType] = []
        while let packet = try channel.readOutbound(as: TDSPacket.self) {
            types.append(packet.type)
        }
        return types
    }

    private static func done(status: UInt16, rows: UInt64 = 0) -> [UInt8] {
        var bytes: [UInt8] = [0xFD, UInt8(status & 0xFF), UInt8(status >> 8), 0xC1, 0x00]
        withUnsafeBytes(of: rows.littleEndian) { bytes.append(contentsOf: $0) }
        return bytes
    }

    private func receive(_ bytes: [UInt8], endOfMessage: Bool = true) throws {
        var buffer = channel.allocator.buffer(capacity: bytes.count)
        buffer.writeBytes(bytes)
        try channel.writeInbound(TDSPacketChunk(type: .tabularResult, payload: buffer, isEndOfMessage: endOfMessage))
    }

    private func result(of context: TDSRequestContext) -> Result<Void, Error>? {
        var outcome: Result<Void, Error>?
        context.completionPromise.futureResult.whenComplete { outcome = $0 }
        return outcome
    }

    // MARK: - Tests

    func testRequestsAreSentOneAtATime() throws {
        let first = submit("SELECT 1")
        let second = submit("SELECT 2")
        XCTAssertEqual(try outboundPacketTypes(), [.sqlBatch], "Only the head request may be in flight")

        try receive(Self.done(status: 0x0010, rows: 1))
        XCTAssertNotNil(try result(of: first)?.get())
        XCTAssertEqual(try outboundPacketTypes(), [.sqlBatch], "Second request starts after the first completes")

        try receive(Self.done(status: 0x0010, rows: 1))
        XCTAssertNotNil(try result(of: second)?.get())
    }

    func testLateAttentionAcknowledgementIsNotAttributedToNextRequest() throws {
        let first = submit("WAITFOR DELAY '00:01:00'")
        let second = submit("SELECT 2")
        XCTAssertEqual(try outboundPacketTypes(), [.sqlBatch])

        handler.cancel(first, reason: TDSError.cancelled)
        XCTAssertEqual(try outboundPacketTypes(), [.attentionSignal])

        // The first request finished before the server saw the ATTENTION.
        // Its final DONE arrives first; the acknowledgement follows alone.
        try receive(Self.done(status: 0x0000))
        guard case .failure(let error)? = result(of: first) else { return XCTFail("First request should fail") }
        XCTAssertEqual(error as? TDSError, .cancelled)
        XCTAssertEqual(try outboundPacketTypes(), [], "No request may start before the acknowledgement")
        XCTAssertNil(result(of: second))

        try receive(Self.done(status: 0x0020))
        XCTAssertEqual(try outboundPacketTypes(), [.sqlBatch])
        XCTAssertNil(result(of: second), "The acknowledgement must not complete the next request")

        try receive(Self.done(status: 0x0010, rows: 1))
        XCTAssertNotNil(try result(of: second)?.get())
    }

    func testCancellingQueuedRequestNeverSendsIt() throws {
        let first = submit("SELECT 1")
        let second = submit("SELECT 2")
        _ = try outboundPacketTypes()
        handler.cancel(second, reason: TDSError.cancelled)
        guard case .failure? = result(of: second) else { return XCTFail("Queued request should fail") }

        try receive(Self.done(status: 0x0000))
        XCTAssertNotNil(try result(of: first)?.get())
        XCTAssertEqual(try outboundPacketTypes(), [], "Cancelled request must not be sent")
    }

    func testStaleCancellationIsIgnored() throws {
        let first = submit("SELECT 1")
        _ = try outboundPacketTypes()
        try receive(Self.done(status: 0x0000))
        handler.cancel(first, reason: TDSError.cancelled)
        XCTAssertEqual(try outboundPacketTypes(), [], "No ATTENTION for a completed request")
    }

    func testTimeoutCancelsRequestOnServer() throws {
        let request = submit("WAITFOR DELAY '00:01:00'", timeout: .seconds(2))
        _ = try outboundPacketTypes()
        channel.embeddedEventLoop.advanceTime(by: .seconds(2))
        XCTAssertEqual(try outboundPacketTypes(), [.attentionSignal])
        XCTAssertNil(result(of: request), "Request completes when the server acknowledges")

        try receive(Self.done(status: 0x0020))
        guard case .failure(let error)? = result(of: request),
              case .requestTimeout = error as? TDSError else {
            return XCTFail("Expected a request timeout")
        }
        XCTAssertTrue(channel.isActive)
    }

    func testMissingAttentionAcknowledgementClosesConnection() throws {
        let request = submit("WAITFOR DELAY '00:01:00'")
        let queued = submit("SELECT 2")
        _ = try outboundPacketTypes()
        handler.cancel(request, reason: TDSError.cancelled)
        channel.embeddedEventLoop.advanceTime(by: TDSRequestHandler.defaultAttentionAcknowledgementTimeout)
        XCTAssertFalse(channel.isActive, "A connection with an unacknowledged cancellation cannot be reused")
        guard case .failure? = result(of: request), case .failure? = result(of: queued) else {
            return XCTFail("Requests should fail when the connection closes")
        }
    }

    func testDrainingAfterCancellationKeepsConnectionWhileDataArrives() throws {
        let request = submit("SELECT * FROM big")
        _ = try outboundPacketTypes()
        handler.cancel(request, reason: TDSError.cancelled)
        let timeout = TDSRequestHandler.defaultAttentionAcknowledgementTimeout
        // The server keeps sending the rows it had already produced for
        // longer than the timeout in total, but never goes silent.
        for _ in 0..<4 {
            channel.embeddedEventLoop.advanceTime(by: .nanoseconds(timeout.nanoseconds * 3 / 4))
            try receive(Self.done(status: 0x0011, rows: 1), endOfMessage: false)
            XCTAssertTrue(channel.isActive, "A draining connection is still responsive")
        }
        try receive(Self.done(status: 0x0020))
        guard case .failure(let error)? = result(of: request) else { return XCTFail("Request should be cancelled") }
        XCTAssertEqual(error as? TDSError, .cancelled)
        XCTAssertTrue(channel.isActive)
    }

    func testUnsolicitedDataClosesConnection() throws {
        try receive(Self.done(status: 0x0000))
        XCTAssertFalse(channel.isActive)
    }

    func testTruncatedMessageFailsRequestAndClosesConnection() throws {
        let request = submit("SELECT 1")
        _ = try outboundPacketTypes()
        try receive(Array(Self.done(status: 0x0000).prefix(5)), endOfMessage: true)
        guard case .failure(let error)? = result(of: request), case .protocolError = error as? TDSError else {
            return XCTFail("Expected a protocol error")
        }
        XCTAssertFalse(channel.isActive)
    }

    func testTokenSplitAcrossPacketsIsReassembled() throws {
        let request = submit("SELECT 1")
        _ = try outboundPacketTypes()
        let done = Self.done(status: 0x0010, rows: 3)
        for (index, byte) in done.enumerated() {
            XCTAssertNil(result(of: request))
            try receive([byte], endOfMessage: index == done.count - 1)
        }
        XCTAssertNotNil(try result(of: request)?.get())
        XCTAssertTrue(channel.isActive)
    }

    func testPausedReadIsDeferredUntilResumed() throws {
        let request = submit("SELECT * FROM big")
        _ = try outboundPacketTypes()
        let before = reads.count
        handler.setReadPaused(true, for: request)
        channel.read()
        XCTAssertEqual(reads.count, before, "Read must be held while the consumer is paused")
        handler.setReadPaused(false, for: request)
        XCTAssertEqual(reads.count, before + 1, "Held read is issued on resume")
    }

    func testCompletionReleasesHeldRead() throws {
        let request = submit("SELECT * FROM big")
        _ = try outboundPacketTypes()
        handler.setReadPaused(true, for: request)
        let before = reads.count
        channel.read()
        try receive(Self.done(status: 0x0000))
        XCTAssertGreaterThan(reads.count, before, "Reading must resume once the paused request completes")
    }
}

/// Counts `read()` calls that pass through the pipeline toward the socket.
private final class ReadCounter: ChannelOutboundHandler, @unchecked Sendable {
    typealias OutboundIn = Any
    private(set) var count = 0

    func read(context: ChannelHandlerContext) {
        count += 1
        context.read()
    }
}
