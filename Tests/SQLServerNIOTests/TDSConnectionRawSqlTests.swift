@testable import SQLServerNIO
import XCTest
import NIOEmbedded
import Logging
import Foundation

final class TDSConnectionRawSqlTests: XCTestCase {
    private var channel: EmbeddedChannel!
    private var connection: TDSConnection!
    private var interceptor: TestRequestCaptureHandler!
    private var logger: Logger!

    override func setUpWithError() throws {
        try super.setUpWithError()
        channel = EmbeddedChannel()
        interceptor = TestRequestCaptureHandler()
        try channel.pipeline.addHandler(interceptor).wait()
        logger = Logger(label: "tds.rawsql.test")
        connection = TDSConnection(channel: channel, logger: logger)
    }

    override func tearDownWithError() throws {
        if let connection {
            var closed = false
            let closeFuture = connection.close()
            closeFuture.whenComplete { _ in closed = true }
            pumpEventLoop(until: { closed })
        }
        _ = try? channel.finish()
        channel = nil
        connection = nil
        interceptor = nil
        logger = nil
        try super.tearDownWithError()
    }

    func testRawSqlAggregatesRows() throws {
        let future = connection.rawSql("SELECT 1;")
        var isComplete = false
        future.whenComplete { _ in isComplete = true }

        guard let context = interceptor.lastRequest else {
            XCTFail("Expected RawSqlBatchRequest to be sent")
            return
        }
        guard let request = context.delegate as? RawSqlBatchRequest else {
            XCTFail("Expected RawSqlBatchRequest delegate")
            return
        }

        let (row, _) = Self.makeSampleRow(value: "agg-value")
        try request.onRow?(row)
        context.promise.futureResult.eventLoop.execute {
            context.promise.succeed(())
        }
        pumpEventLoop(until: { isComplete })

        XCTAssertTrue(isComplete, "Expected rawSql future to resolve")
        guard isComplete else { return }
        let rows = try future.wait()
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows.first?.column("Value")?.string, "agg-value")
    }

    func testRawSqlStreamingClosureReceivesRows() throws {
        var received: String?

        let future = connection.rawSql("SELECT 1;") { row in
            received = row.column("Value")?.string
        }
        var isComplete = false
        future.whenComplete { _ in isComplete = true }

        guard let context = interceptor.lastRequest else {
            XCTFail("Expected RawSqlBatchRequest to be sent")
            return
        }
        guard let request = context.delegate as? RawSqlBatchRequest else {
            XCTFail("Expected RawSqlBatchRequest delegate")
            return
        }

        let (row, _) = Self.makeSampleRow(value: "stream-value")
        try request.onRow?(row)
        context.promise.futureResult.eventLoop.execute {
            context.promise.succeed(())
        }
        pumpEventLoop(until: { isComplete })

        XCTAssertTrue(isComplete, "Expected streaming future to resolve")
        guard isComplete else { return }
        XCTAssertEqual(received, "stream-value")
        XCTAssertNoThrow(try future.wait())
    }

    func testQueryWithExplicitMessageUsesProvidedBatch() throws {
        let message = TDSMessages.RawSqlBatchMessage(sqlText: "SELECT 2;")
        let future = connection.query(message) { _ in }
        var isComplete = false
        future.whenComplete { _ in isComplete = true }

        guard let context = interceptor.lastRequest else {
            XCTFail("Expected RawSqlBatchRequest to be sent")
            return
        }
        guard let request = context.delegate as? RawSqlBatchRequest else {
            XCTFail("Expected RawSqlBatchRequest delegate")
            return
        }

        XCTAssertEqual(request.sqlBatch.sqlText, message.sqlText)

        context.promise.futureResult.eventLoop.execute {
            context.promise.succeed(())
        }
        pumpEventLoop(until: { isComplete })

        XCTAssertTrue(isComplete, "Expected query future to resolve")
        guard isComplete else { return }
        XCTAssertNoThrow(try future.wait())
    }

    // MARK: - Helpers

    private static func makeSampleRow(
        columnName: String = "Value",
        value: String
    ) -> (row: TDSRow, metadata: TDSTokens.ColMetadataToken) {
        let column = TDSTokens.ColMetadataToken.ColumnData(
            userType: 0,
            flags: 0,
            dataType: .nvarchar,
            length: value.utf16.count * 2,
            collation: [],
            tableName: nil,
            colName: columnName,
            precision: nil,
            scale: nil
        )
        let metadata = TDSTokens.ColMetadataToken(count: 1, colData: [column])

        var buffer = ByteBufferAllocator().buffer(capacity: value.utf16.count * 2)
        buffer.writeUTF16String(value)

        let columnData = TDSTokens.RowToken.ColumnData(
            textPointer: [],
            timestamp: [],
            data: buffer
        )

        let rowToken = TDSTokens.RowToken(colData: [columnData])
        let lookupTable = TDSRow.LookupTable(colMetadata: metadata)
        let row = TDSRow(dataRow: rowToken, lookupTable: lookupTable)

        return (row, metadata)
    }

    private func pumpEventLoop(
        until condition: @escaping () -> Bool,
        timeout: TimeInterval = 1.0
    ) {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() && Date() < deadline {
            channel.embeddedEventLoop.run()
        }
    }
}

private final class TestRequestCaptureHandler: ChannelDuplexHandler {
    typealias InboundIn = TDSPacket
    typealias OutboundIn = TDSRequestContext

    private(set) var requests: [TDSRequestContext] = []

    var lastRequest: TDSRequestContext? { requests.last }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = unwrapOutboundIn(data)
        requests.append(request)
        promise?.succeed(())
    }

    func flush(context: ChannelHandlerContext) {
        context.flush()
    }
}
