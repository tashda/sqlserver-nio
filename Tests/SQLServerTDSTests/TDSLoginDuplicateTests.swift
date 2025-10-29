@testable import SQLServerTDS
import XCTest
import NIO
import NIOEmbedded
import Logging

final class TDSLoginDuplicateTests: XCTestCase {
    private func makeChannelWithRequestHandler() throws -> EmbeddedChannel {
        let channel = EmbeddedChannel()
        let logger = Logger(label: "tds.login.dup.tests")
        let firstDecoder = ByteToMessageHandler(TDSPacketDecoder(logger: logger))
        let firstEncoder = MessageToByteHandler(TDSPacketEncoder(logger: logger))
        let handler = TDSRequestHandler(
            logger: logger,
            firstDecoder: firstDecoder,
            firstEncoder: firstEncoder,
            tlsConfiguration: nil,
            serverHostname: nil,
            firstDecoderName: "tds.firstDecoder",
            firstEncoderName: "tds.firstEncoder",
            pipelineCoordinatorName: "tds.pipelineCoordinator"
        )
        try channel.pipeline.addHandler(firstDecoder, name: "tds.firstDecoder").wait()
        try channel.pipeline.addHandler(firstEncoder, name: "tds.firstEncoder").wait()
        try channel.pipeline.addHandler(handler, name: "tds.requestHandler").wait()
        return channel
    }

    func drainOutbound(_ channel: EmbeddedChannel) throws -> [ByteBuffer] {
        var out: [ByteBuffer] = []
        while let buf: ByteBuffer = try channel.readOutbound(as: ByteBuffer.self) {
            out.append(buf)
        }
        return out
    }

    func makeLoginRequestContext(on loop: EventLoop) -> TDSRequestContext {
        let payload = TDSMessages.Login7Message(
            username: "sa",
            password: "password",
            serverName: "server",
            database: "master",
            useIntegratedSecurity: false,
            sspiData: nil
        )
        let logger = Logger(label: "tds.login.dup.tests")
        let request = LoginRequest(payload: payload, authenticator: nil, logger: logger, ring: nil)
        let promise: EventLoopPromise<Void> = PromiseTracker.makeTrackedPromise(on: loop, label: "TDSRequest.completion")
        return TDSRequestContext(delegate: request, promise: promise)
    }

    func testDuplicateLoginIsDroppedAndQueueProgresses() throws {
        let channel = try makeChannelWithRequestHandler()
        let ctx1 = makeLoginRequestContext(on: channel.eventLoop)
        let ctx2 = makeLoginRequestContext(on: channel.eventLoop)

        // Write first LOGIN
        try channel.writeAndFlush(ctx1).wait()
        let firstWrites = try drainOutbound(channel)
        XCTAssertFalse(firstWrites.isEmpty, "First LOGIN should emit packets")

        // Write duplicate LOGIN immediately; should be coalesced and not emit any additional outbound data
        try channel.writeAndFlush(ctx2).wait()
        let secondWrites = try drainOutbound(channel)
        XCTAssertTrue(secondWrites.isEmpty, "Duplicate LOGIN should not emit packets")

        // Duplicate request promise should be completed successfully
        XCTAssertNoThrow(try ctx2.promise.futureResult.wait())

        // Finish channel cleanly
        XCTAssertNoThrow(_ = try channel.finish())
    }
}
