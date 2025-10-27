@testable import SQLServerKit
import NIO
import XCTest

final class SQLServerMetadataParameterLoadTests: XCTestCase {
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
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testRepeatedParameterIntrospectionStaysStable() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "paramload") { database in
            let statements: [String] = [
                """
                CREATE TYPE dbo.PhoneList AS TABLE (
                    phone NVARCHAR(32) NOT NULL,
                    extension NVARCHAR(12) NULL
                );
                """,
                """
                CREATE PROCEDURE dbo.InsertPhones
                    @customerId INT,
                    @phones dbo.PhoneList READONLY
                AS
                BEGIN
                    SET NOCOUNT ON;
                    SELECT @customerId AS customerId, COUNT(*) AS phoneCount FROM @phones;
                END;
                """,
                """
                CREATE PROCEDURE dbo.UpdateContact
                    @contactId UNIQUEIDENTIFIER,
                    @firstName NVARCHAR(50) = N'',
                    @lastName NVARCHAR(50) = N'',
                    @email NVARCHAR(128) = NULL,
                    @debug BIT = 0
                AS
                BEGIN
                    SELECT @contactId, @firstName, @lastName, @email, @debug;
                END;
                """,
                """
                CREATE FUNCTION dbo.FormatFullName (
                    @first NVARCHAR(50),
                    @middle NVARCHAR(50) = NULL,
                    @last NVARCHAR(50)
                )
                RETURNS NVARCHAR(200)
                AS
                BEGIN
                    RETURN CONCAT(@first, N' ', COALESCE(@middle + N' ', N''), @last);
                END;
                """
            ]

            for statement in statements {
                _ = try await executeInDb(client: self.client, database: database, statement)
            }

            let objects = [
                ("InsertPhones", 2),
                ("UpdateContact", 5),
                ("FormatFullName", 3)
            ]

            try await withDbConnection(client: self.client, database: database) { connection in
                for _ in 0..<3 {
                    for (name, expectedCount) in objects {
                        let metadata = try await connection.listParameters(database: database, schema: "dbo", object: name).get()
                        XCTAssertEqual(metadata.count, expectedCount, "Unexpected parameter count for \(name)")
                    }
                }
            }
        }
    }
}
