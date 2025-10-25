@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerAdventureWorksRoutineTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).wait()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testAdventureWorksUfnGetAccountingEndDateParameters() async throws {
        // Only run when explicitly enabled and AdventureWorks is available
        guard env("TDS_ENABLE_ADVENTUREWORKS") == "1" else { throw XCTSkip("TDS_ENABLE_ADVENTUREWORKS!=1") }
        let dbName = env("TDS_AW_DATABASE") ?? "AdventureWorks2022"
        try await client.withConnection { connection in
            _ = try await connection.changeDatabase(dbName).get()
            let parameters = try await connection.listParameters(schema: "dbo", object: "ufnGetAccountingEndDate").get()
            XCTAssertFalse(parameters.isEmpty, "Expected parameters for dbo.ufnGetAccountingEndDate")
        }
    }
}

