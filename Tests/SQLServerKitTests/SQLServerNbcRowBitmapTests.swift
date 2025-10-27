@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerNbcRowBitmapTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        try await client?.shutdownGracefully().get()
        try await group?.shutdownGracefully()
    }

    @available(macOS 12.0, *)
    func testNbcRowNullBitmapOverMultipleBytes() async throws {
        try await withTimeout(20) {
            try await withTemporaryDatabase(client: self.client, prefix: "nbc") { db in
            let table = "nbc_tbl_\(UUID().uuidString.prefix(6))"
            // 20 nullable columns to ensure the null bitmap spans multiple bytes
            var cols: [String] = []
            for i in 1...20 { cols.append("c\(i) INT NULL") }
            let create = "CREATE TABLE [dbo].[\(table)] (\n    \(cols.joined(separator: ",\n    "))\n);"
            _ = try await executeInDb(client: self.client, database: db, create)

            // Insert one row with alternating NULL/non-NULL pattern
            var values: [String] = []
            for i in 1...20 { values.append(i % 2 == 1 ? String(i) : "NULL") }
            let insert = "INSERT INTO [dbo].[\(table)] VALUES (\(values.joined(separator: ", ")));"
            _ = try await executeInDb(client: self.client, database: db, insert)

            let rows = try await queryInDb(client: self.client, database: db, "SELECT * FROM [dbo].[\(table)]")
            guard let row = rows.first else { XCTFail("Missing row"); return }
            for i in 1...20 {
                let val = row.column("c\(i)")?.int
                if i % 2 == 1 {
                    XCTAssertEqual(val, i, "Expected non-null at c\(i)")
                } else {
                    XCTAssertNil(val, "Expected NULL at c\(i)")
                }
            }
            }
        }
    }
}
