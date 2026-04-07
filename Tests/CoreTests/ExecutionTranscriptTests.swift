import SQLServerKit
import SQLServerKitTesting
import XCTest

final class ExecutionTranscriptTests: StandardTestBase, @unchecked Sendable {
    func testExecuteCapturesDoneTokenDetails() async throws {
        let connection = try await client.connection()
        let result = try await connection.execute("SELECT 1 AS value")

        let done = try XCTUnwrap(result.done.last)
        XCTAssertEqual(done.kind, .done)
        XCTAssertGreaterThan(done.curCmd, 0)
    }

    func testExecuteCapturesFullInfoMessageFields() async throws {
        let connection = try await client.connection()
        let result = try await connection.execute("PRINT N'hello from sqlserver-nio'")

        let message = try XCTUnwrap(result.messages.first(where: { $0.kind == .info }))
        XCTAssertEqual(message.message, "hello from sqlserver-nio")
        XCTAssertEqual(message.procedureName, "")
        XCTAssertFalse(message.serverName.isEmpty)
    }
}
