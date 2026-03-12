@testable import SQLServerKit
import SQLServerKitTesting
import NIO
import XCTest

final class SQLServerMetadataParameterLoadTests: XCTestCase, @unchecked Sendable {
    var group: EventLoopGroup!
    var client: SQLServerClient!

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
    }

    override func tearDown() async throws {
        do {
            try await client?.shutdownGracefully().get()
        } catch {
            // Silently ignore "Already closed" errors during shutdown - they're expected under stress
            if error.localizedDescription.contains("Already closed") ||
               error.localizedDescription.contains("ChannelError error 6") {
                // Both errors are expected during EventLoop shutdown
            } else {
                throw error
            }
        }

        if let g = group {
            _ = try? await SQLServerClient.shutdownEventLoopGroup(g).get()
        }
        client = nil
        group = nil
    }

    @available(macOS 12.0, *)
    func testRepeatedParameterIntrospectionStaysStable() async throws {
        try await withTemporaryDatabase(client: self.client, prefix: "paramload") { database in
            try await withDbClient(for: database, using: self.group) { dbClient in
                let typeClient = SQLServerTypeClient(client: dbClient)
                let routineClient = SQLServerRoutineClient(client: dbClient)

                try await typeClient.createUserDefinedTableType(.init(
                    name: "PhoneList",
                    columns: [
                        .init(name: "phone", dataType: .nvarchar(length: .length(32)), isNullable: false),
                        .init(name: "extension", dataType: .nvarchar(length: .length(12)), isNullable: true)
                    ]
                ))

                try await routineClient.createStoredProcedure(
                    name: "InsertPhones",
                    parameters: [
                        .init(name: "customerId", dataType: .int),
                        .init(name: "phones", dataType: .userDefinedTable(name: "PhoneList", schema: "dbo"))
                    ],
                    body: """
                    BEGIN
                        SET NOCOUNT ON;
                        SELECT @customerId AS customerId, COUNT(*) AS phoneCount FROM @phones;
                    END
                    """
                )

                try await routineClient.createStoredProcedure(
                    name: "UpdateContact",
                    parameters: [
                        .init(name: "contactId", dataType: .uniqueidentifier),
                        .init(name: "firstName", dataType: .nvarchar(length: .length(50)), defaultValue: "N''"),
                        .init(name: "lastName", dataType: .nvarchar(length: .length(50)), defaultValue: "N''"),
                        .init(name: "email", dataType: .nvarchar(length: .length(128)), defaultValue: "NULL"),
                        .init(name: "debug", dataType: .bit, defaultValue: "0")
                    ],
                    body: """
                    BEGIN
                        SELECT @contactId, @firstName, @lastName, @email, @debug;
                    END
                    """
                )

                try await routineClient.createFunction(
                    name: "FormatFullName",
                    parameters: [
                        .init(name: "first", dataType: .nvarchar(length: .length(50))),
                        .init(name: "middle", dataType: .nvarchar(length: .length(50)), defaultValue: "NULL"),
                        .init(name: "last", dataType: .nvarchar(length: .length(50)))
                    ],
                    returnType: .nvarchar(length: .length(200)),
                    body: """
                    BEGIN
                        RETURN CONCAT(@first, N' ', COALESCE(@middle + N' ', N''), @last);
                    END
                    """
                )
            }

            let objects = [
                ("InsertPhones", 2),
                ("UpdateContact", 5),
                ("FormatFullName", 3)
            ]

            try await withDbConnection(client: self.client, database: database) { connection in
                // Clear any connection cache/pool state by running a simple query first
                _ = try await connection.query("SELECT 1 AS test;").get()

                for iteration in 0..<3 {
                    for (name, expectedCount) in objects {
                        let metadata = try await connection.listParameters(database: database, schema: "dbo", object: name).get()
                        let inputParameters = metadata.filter { !$0.isReturnValue }
                        XCTAssertEqual(inputParameters.count, expectedCount, "Unexpected parameter count for \(name) on iteration \(iteration)")

                    }
                }
            }
        }
    }
}
