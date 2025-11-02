import Foundation
import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerBulkCopyTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var bulkCopyClient: SQLServerBulkCopyClient!
    private var tablesToDrop: [String] = []
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        adminClient = SQLServerAdministrationClient(client: client)
        bulkCopyClient = SQLServerBulkCopyClient(client: client)
    }
    
    override func tearDownWithError() throws {
        for table in tablesToDrop {
            try? adminClient.dropTable(name: table).wait()
        }
        tablesToDrop.removeAll()
        try client.shutdownGracefully().wait()
        try group.syncShutdownGracefully()
        bulkCopyClient = nil
        adminClient = nil
        client = nil
        group = nil
        try super.tearDownWithError()
    }
    
    func testBulkCopyInsertsRows() async throws {
        let tableName = "bulk_copy_target_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (1, 1)))),
            SQLServerColumnDefinition(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
            SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let options = SQLServerBulkCopyOptions(
            table: tableName,
            columns: ["category", "amount"],
            batchSize: 2
        )
        let rows = [
            SQLServerBulkCopyRow(values: [.nString("Hardware"), .decimal("123.45")]),
            SQLServerBulkCopyRow(values: [.nString("Hardware"), .decimal("50.55")]),
            SQLServerBulkCopyRow(values: [.nString("Software"), .decimal("300.00")])
        ]
        
        let summary = try await bulkCopyClient.copy(rows: rows, options: options)
        XCTAssertEqual(summary.totalRows, rows.count)
        XCTAssertEqual(summary.batchesExecuted, 2)
        
        let results = try await client.query("SELECT category, SUM(amount) AS total FROM [\(tableName)] GROUP BY category ORDER BY category")
        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[0].column("category")?.string, "Hardware")
        let hardwareTotal = try XCTUnwrap(results[0].column("total")?.double, "Hardware total missing")
        XCTAssertEqual(hardwareTotal, 174.0, accuracy: 0.0001)
        XCTAssertEqual(results[1].column("category")?.string, "Software")
        let softwareTotal = try XCTUnwrap(results[1].column("total")?.double, "Software total missing")
        XCTAssertEqual(softwareTotal, 300.0, accuracy: 0.0001)
    }

    func testBulkCopySupportsIdentityInsert() async throws {
        let tableName = "bulk_identity_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, identity: (100, 5)))),
            SQLServerColumnDefinition(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(40)), isNullable: false))),
            SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let rows = [
            SQLServerBulkCopyRow(values: [.int(5), .nString("CapEx"), .decimal("999.99")]),
            SQLServerBulkCopyRow(values: [.int(10), .nString("OpEx"), .decimal("250.25")]),
            SQLServerBulkCopyRow(values: [.int(15), .nString("CapEx"), .decimal("10.01")])
        ]
        let options = SQLServerBulkCopyOptions(
            table: tableName,
            columns: ["id", "category", "amount"],
            batchSize: 2,
            identityInsert: true
        )
        
        let summary = try await bulkCopyClient.copy(rows: rows, options: options)
        XCTAssertEqual(summary.totalRows, rows.count)
        XCTAssertEqual(summary.identityInsert, true)
        
        let fetched = try await client.query("SELECT id, category FROM [\(tableName)] ORDER BY id")
        XCTAssertEqual(fetched.map { $0.column("id")?.int ?? -1 }, [5, 10, 15])
        XCTAssertEqual(fetched[0].column("category")?.string, "CapEx")
    }
    
    func testBulkCopyHandlesMixedTypesAndLargePayloads() async throws {
        let tableName = "bulk_payloads_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "payload", definition: .standard(.init(dataType: .nvarchar(length: .max), isNullable: false))),
            SQLServerColumnDefinition(name: "snapshot_time", definition: .standard(.init(dataType: .datetime2(precision: 7)))),
            SQLServerColumnDefinition(name: "document", definition: .standard(.init(dataType: .varbinary(length: .max)))),
            SQLServerColumnDefinition(name: "reference_id", definition: .standard(.init(dataType: .uniqueidentifier)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let lorem = String(repeating: "DATA-", count: 2048)
        let docBytes = Array("document".utf8)
        let rows = [
            SQLServerBulkCopyRow(values: [
                .int(1),
                .nString(lorem),
                .raw("CONVERT(datetime2(7), '2023-08-12T10:00:00.1234567', 126)"),
                .bytes(docBytes),
                .uuid(UUID(uuidString: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")!)
            ]),
            SQLServerBulkCopyRow(values: [
                .int(2),
                .nString("short"),
                .raw("CONVERT(datetime2(7), '2024-01-02T03:04:05.7654321', 126)"),
                .bytes(docBytes + [0x00, 0xFF]),
                .uuid(UUID(uuidString: "ffffffff-1111-2222-3333-444444444444")!)
            ])
        ]
        
        let summary = try await bulkCopyClient.copy(
            rows: rows,
            options: SQLServerBulkCopyOptions(table: tableName, columns: ["id", "payload", "snapshot_time", "document", "reference_id"], batchSize: 1)
        )
        XCTAssertEqual(summary.totalRows, 2)
        
        let result = try await client.query("SELECT id, LEN(payload) AS payload_len FROM [\(tableName)] ORDER BY id")
        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0].column("payload_len")?.int, lorem.count)
    }
    
    func testBulkCopyConstraintViolationSurfacesAndKeepsCommittedBatches() async throws {
        let tableName = "bulk_constraints_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2)))),
            SQLServerColumnDefinition(name: "category", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        _ = try await client.execute("""
        ALTER TABLE [\(tableName)] ADD CONSTRAINT CK_\(tableName)_amount CHECK (amount >= 0);
        """)
        
        let rows = [
            SQLServerBulkCopyRow(values: [.int(1), .decimal("10.00"), .nString("ok")]),
            SQLServerBulkCopyRow(values: [.int(2), .decimal("-5.00"), .nString("bad")])
        ]
        
        do {
            _ = try await bulkCopyClient.copy(
                rows: rows,
                options: SQLServerBulkCopyOptions(table: tableName, columns: ["id", "amount", "category"], batchSize: 1)
            )
            XCTFail("Constraint violation should throw")
        } catch {
            guard case SQLServerError.sqlExecutionError(let message) = error else {
                XCTFail("Expected SQL execution error, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("CK_") || message.contains("PRIMARY KEY"), "Unexpected error message: \(message)")
        }
        
        let count = try await client.query("SELECT COUNT(*) AS inserted FROM [\(tableName)]")
        XCTAssertEqual(count.first?.column("inserted")?.int, 1)
    }
    
    func testBulkCopyColumnCountMismatchThrows() async throws {
        let tableName = "bulk_mismatch_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(20)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let rows = [
            SQLServerBulkCopyRow(values: [.int(1), .nString("valid")]),
            SQLServerBulkCopyRow(values: [.int(2)]) // Missing column
        ]
        
        do {
            _ = try await bulkCopyClient.copy(
                rows: rows,
                options: SQLServerBulkCopyOptions(table: tableName, columns: ["id", "name"])
            )
            XCTFail("Expected columnCountMismatch error")
        } catch {
            guard case SQLServerBulkCopyError.columnCountMismatch = error else {
                XCTFail("Expected columnCountMismatch, got \(error)")
                return
            }
        }
    }
    
    func testBulkCopyOutperformsRowByRowInserts() async throws {
        let tableName = "bulk_perf_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "payload", definition: .standard(.init(dataType: .nvarchar(length: .length(120))))),
            SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .decimal(precision: 12, scale: 4))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let rows = (1...120).map { idx in
            SQLServerBulkCopyRow(values: [
                .int(idx),
                .nString("payload-\(idx)"),
                .decimal("\(Double(idx) * 1.5)")
            ])
        }
        let options = SQLServerBulkCopyOptions(table: tableName, columns: ["id", "payload", "amount"], batchSize: 25)
        
        let rowDuration = try await executeRowByRow(rows: rows, tableName: tableName, columns: options.columns)
        try await truncateTable(named: tableName)
        let bulkSummary = try await bulkCopyClient.copy(rows: rows, options: options)
        
        XCTAssertEqual(bulkSummary.totalRows, rows.count)
        XCTAssertLessThanOrEqual(bulkSummary.batchesExecuted, Int(ceil(Double(rows.count) / Double(options.batchSize))))
        XCTAssertLessThan(bulkSummary.duration, rowDuration, "Bulk copy should be faster than issuing individual INSERT statements")
    }
    
    func testBulkCopyDetectsConnectionDropsMidStream() async throws {
        let tableName = "bulk_network_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "payload", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let rows = (1...10).map { idx in
            SQLServerBulkCopyRow(values: [.int(idx), .nString("row-\(idx)")])
        }
        let options = SQLServerBulkCopyOptions(table: tableName, columns: ["id", "payload"], batchSize: 3)
        
        var dropCount = 0
        do {
            _ = try await bulkCopyClient.copy(rows: rows, options: options, afterBatch: { connection, batch in
                if batch == 1 && dropCount == 0 {
                    dropCount += 1
                    try await self.closeUnderlyingConnection(connection)
                }
            })
            XCTFail("Connection drop should force retry failure")
        } catch {
            XCTAssertEqual(dropCount, 1)
            guard case SQLServerError.sqlExecutionError(let message) = error else {
                XCTFail("Expected SQL execution error after retry exhaustion, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("PRIMARY KEY") || message.localizedCaseInsensitiveContains("connection"), "Unexpected error message: \(message)")
        }
        
        let persisted = try await client.query("SELECT COUNT(*) AS inserted FROM [\(tableName)]")
        XCTAssertEqual(persisted.first?.column("inserted")?.int, options.batchSize)
        
        try await truncateTable(named: tableName)
        let recoverySummary = try await bulkCopyClient.copy(rows: rows, options: options)
        XCTAssertEqual(recoverySummary.totalRows, rows.count)
    }
    
    // MARK: - Helpers
    
    private func executeRowByRow(rows: [SQLServerBulkCopyRow], tableName: String, columns: [String]) async throws -> TimeInterval {
        let start = Date()
        for row in rows {
            let insert = singleInsertStatement(row: row, tableName: tableName, columns: columns)
            _ = try await client.execute(insert)
        }
        return Date().timeIntervalSince(start)
    }
    
    private func truncateTable(named tableName: String) async throws {
        _ = try await client.execute("TRUNCATE TABLE \(qualifiedTableName(tableName));")
    }
    
    private func singleInsertStatement(row: SQLServerBulkCopyRow, tableName: String, columns: [String]) -> String {
        let columnList = columns
            .map { "[\(SQLServerBulkCopyClient.escapeIdentifier($0))]" }
            .joined(separator: ", ")
        let literals = row.values.map { $0.sqlLiteral() }.joined(separator: ", ")
        return "INSERT INTO \(qualifiedTableName(tableName)) (\(columnList)) VALUES (\(literals));"
    }
    
    private func qualifiedTableName(_ tableName: String) -> String {
        "[dbo].[\(SQLServerBulkCopyClient.escapeIdentifier(tableName))]"
    }
    
    private func closeUnderlyingConnection(_ connection: SQLServerConnection) async throws {
        try await withCheckedThrowingContinuation { continuation in
            connection.underlying.close().whenComplete { result in
                continuation.resume(with: result)
            }
        }
    }
}
