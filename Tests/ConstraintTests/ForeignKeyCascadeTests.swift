@testable import SQLServerKit
import XCTest
import NIO
import Logging

final class SQLServerForeignKeyCascadeMatrixTests: XCTestCase {
    var group: EventLoopGroup!
    var client: SQLServerClient!
    private var skipDueToEnv = false

    override func setUp() async throws {
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        client = try await SQLServerClient.connect(configuration: makeSQLServerClientConfiguration(), eventLoopGroupProvider: .shared(group)).get()
        do { _ = try await withTimeout(5) { try await self.client.query("SELECT 1").get() } } catch { skipDueToEnv = true }
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
    }

    private func deep() -> Bool { env("TDS_ENABLE_DEEP_SCENARIO_TESTS") == "1" }

    @available(macOS 12.0, *)
    func testForeignKeyCascadeMatrix() async throws {
        if skipDueToEnv { throw XCTSkip("Skipping due to unstable server during setup") }
        try await withTemporaryDatabase(client: self.client, prefix: "fkmx") { db in
            let parent = "fk_parent_\(UUID().uuidString.prefix(6))"
            let child = "fk_child_\(UUID().uuidString.prefix(6))"

            let adminClient = SQLServerAdministrationClient(client: self.client)
            let constraintClient = SQLServerConstraintClient(client: self.client)

            let parentColumns: [SQLServerColumnDefinition] = [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "d", definition: .standard(.init(dataType: .int, defaultValue: "0")))
            ]
            let childColumns: [SQLServerColumnDefinition] = [
                .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
                .init(name: "pid", definition: .standard(.init(dataType: .int, isNullable: true, defaultValue: "0"))),
                .init(name: "v", definition: .standard(.init(dataType: .nvarchar(length: .length(10)))))
            ]

            try await adminClient.createTable(name: parent, columns: parentColumns)
            try await adminClient.createTable(name: child, columns: childColumns)
            struct Case {
                let del: ForeignKeyOptions.ReferentialAction
                let upd: ForeignKeyOptions.ReferentialAction
                let expectNullOnDelete: Bool
                let expectDefaultOnDelete: Bool
            }
            var cases: [Case] = [
                Case(del: .noAction, upd: .noAction, expectNullOnDelete: false, expectDefaultOnDelete: false),
                Case(del: .cascade, upd: .noAction, expectNullOnDelete: false, expectDefaultOnDelete: false),
                Case(del: .setNull, upd: .noAction, expectNullOnDelete: true, expectDefaultOnDelete: false),
            ]
            if self.deep() { cases.append(Case(del: .setDefault, upd: .noAction, expectNullOnDelete: false, expectDefaultOnDelete: true)) }

            try await withDbConnection(client: self.client, database: db) { connection in
                for (i, c) in cases.enumerated() {
                    let fk = "FK_\(i)_\(UUID().uuidString.prefix(4))"

                    // Clean up any existing constraints on both tables
                    let parentConstraints = try await constraintClient.listTableConstraints(table: parent)
                    let childConstraints = try await constraintClient.listTableConstraints(table: child)

                    // Drop any foreign key constraints that reference our tables
                    for constraint in parentConstraints + childConstraints {
                        if constraint.type == .foreignKey {
                            try await constraintClient.dropForeignKey(name: constraint.name, table: constraint.tableName)
                        }
                    }

                    // Clear all data from both tables
                    _ = try await connection.execute("DELETE FROM [dbo].[\(child)]").get()
                    _ = try await connection.execute("DELETE FROM [dbo].[\(parent)]").get()

                    // Create the foreign key constraint with proper options
                    let options = ForeignKeyOptions(onDelete: c.del, onUpdate: c.upd)
                    try await constraintClient.addForeignKey(
                        name: fk,
                        table: child,
                        columns: ["pid"],
                        referencedTable: parent,
                        referencedColumns: ["id"],
                        options: options
                    )

                    // Insert test data
                    _ = try await connection.execute("INSERT INTO [dbo].[\(parent)](id) VALUES (1)").get()
                    _ = try await connection.execute("INSERT INTO [dbo].[\(child)](id, pid, v) VALUES (11, 1, N'x')").get()

                    // Delete parent; for NO ACTION we expect a failure
                    if c.del == .noAction {
                        do {
                            _ = try await connection.execute("DELETE FROM [dbo].[\(parent)] WHERE id = 1").get()
                            XCTFail("Expected NO ACTION delete to fail due to FK constraint")
                        } catch {
                            // expected — continue
                        }
                    } else {
                        _ = try await connection.execute("DELETE FROM [dbo].[\(parent)] WHERE id = 1").get()
                    }

                    // Check child table results
                    let rows = try await connection.query("SELECT COUNT(*) AS cnt, SUM(CASE WHEN pid IS NULL THEN 1 ELSE 0 END) AS nulls, SUM(CASE WHEN pid = 0 THEN 1 ELSE 0 END) AS defs FROM [dbo].[\(child)]").get()
                    guard let r = rows.first else { XCTFail("Missing row"); continue }
                    let cnt = r.column("cnt")?.int ?? 0
                    let nulls = r.column("nulls")?.int ?? 0
                    let defs = r.column("defs")?.int ?? 0
                    if c.del == .cascade { XCTAssertEqual(cnt, 0, "CASCADE should remove child") }
                    if c.expectNullOnDelete { XCTAssertEqual(nulls, 1, "SET NULL should null pid") }
                    if c.expectDefaultOnDelete { XCTAssertEqual(defs, 1, "SET DEFAULT should set pid=0") }

                    // Drop FK for this case to avoid interference across cases
                    try await constraintClient.dropForeignKey(name: fk, table: child)
                }
            }
        }
    }
}
