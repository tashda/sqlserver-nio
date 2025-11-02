import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerTableValuedParameterTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var tablesToDrop: [String] = []
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        TestEnvironmentManager.loadEnvironmentVariables(); // Load environment configuration
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let config = makeSQLServerClientConfiguration()
        client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        adminClient = SQLServerAdministrationClient(client: client)
    }
    
    override func tearDownWithError() throws {
        for table in tablesToDrop {
            try? adminClient.dropTable(name: table).wait()
        }
        tablesToDrop.removeAll()
        try client.shutdownGracefully().wait()
        try group.syncShutdownGracefully()
        client = nil
        adminClient = nil
        group = nil
        try super.tearDownWithError()
    }
    
    func testExecuteWithTableValuedParameter() async throws {
        let tableName = "tvp_target_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let parameter = SQLServerTableValuedParameter(
            name: "NewRows",
            columns: [
                .init(name: "id", dataType: .int),
                .init(name: "name", dataType: .nvarchar(length: .length(50)))
            ],
            rows: [
                .init(values: [.int(1), .nString("Alice")]),
                .init(values: [.int(2), .nString("Bob")])
            ]
        )
        
        _ = try await client.execute("""
        INSERT INTO [\(tableName)] (id, name)
        SELECT id, name FROM @NewRows;
        """, tableParameters: [parameter])
        
        let verification = try await client.query("SELECT COUNT(*) AS inserted FROM [\(tableName)]")
        let insertedCount = verification.first?.column("inserted")?.int ?? 0
        XCTAssertEqual(insertedCount, 2)
    }
    
    func testExecuteWithMultipleTableValuedParametersAndScalars() async throws {
        let tableName = "tvp_multi_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "order_id", definition: .standard(.init(dataType: .int))),
            SQLServerColumnDefinition(name: "customer", definition: .standard(.init(dataType: .nvarchar(length: .length(50))))),
            SQLServerColumnDefinition(name: "amount", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2)))),
            SQLServerColumnDefinition(name: "note", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let primary = SQLServerTableValuedParameter(
            name: "PrimaryOrders",
            columns: [
                .init(name: "order_id", dataType: .int),
                .init(name: "customer", dataType: .nvarchar(length: .length(50))),
                .init(name: "amount", dataType: .decimal(precision: 10, scale: 2))
            ],
            rows: [
                .init(values: [.int(1), .nString("A"), .decimal("125.00")]),
                .init(values: [.int(2), .nString("B"), .decimal("240.00")])
            ]
        )
        
        let adjustments = SQLServerTableValuedParameter(
            name: "Adjustments",
            columns: [
                .init(name: "order_id", dataType: .int),
                .init(name: "note", dataType: .nvarchar(length: .length(100)))
            ],
            rows: [
                .init(values: [.int(1), .nString("Promo applied")]),
                .init(values: [.int(3), .nString("Manual add")])
            ]
        )
        
        let sql = """
        DECLARE @limit DECIMAL(10,2) = 300.00;
        INSERT INTO [\(tableName)] (order_id, customer, amount)
        SELECT order_id, customer, amount
        FROM @PrimaryOrders WHERE amount <= @limit;
        
        UPDATE target
        SET note = src.note
        FROM [\(tableName)] AS target
        INNER JOIN @Adjustments AS src ON src.order_id = target.order_id;
        
        INSERT INTO [\(tableName)] (order_id, customer, amount, note)
        SELECT a.order_id, 'System', 0, a.note
        FROM @Adjustments AS a
        WHERE NOT EXISTS (SELECT 1 FROM [\(tableName)] WHERE order_id = a.order_id);
        """
        
        _ = try await client.execute(sql, tableParameters: [primary, adjustments])
        let rows = try await client.query("SELECT COUNT(*) AS total, SUM(amount) AS total_amount FROM [\(tableName)]")
        XCTAssertEqual(rows.first?.column("total")?.int, 3)
        let totalAmount = try XCTUnwrap(rows.first?.column("total_amount")?.double, "Missing aggregated amount")
        XCTAssertEqual(totalAmount, 365.0, accuracy: 0.001)
    }
    
    func testTableValuedParameterBinaryAndJsonPayloads() async throws {
        let tableName = "tvp_payloads_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "payload", definition: .standard(.init(dataType: .varbinary(length: .max)))),
            SQLServerColumnDefinition(name: "metadata", definition: .standard(.init(dataType: .nvarchar(length: .max))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let parameter = SQLServerTableValuedParameter(
            name: "BlobRows",
            columns: [
                .init(name: "id", dataType: .int),
                .init(name: "payload", dataType: .varbinary(length: .max)),
                .init(name: "metadata", dataType: .nvarchar(length: .max))
            ],
            rows: [
                .init(values: [.int(1), .bytes(Array("alpha".utf8)), .nString("{\"tier\":\"gold\"}")]),
                .init(values: [.int(2), .bytes([0x00, 0xFF, 0x10]), .nString("{\"tier\":\"silver\"}")])
            ]
        )
        
        _ = try await client.execute("""
        INSERT INTO [\(tableName)] (id, payload, metadata)
        SELECT id, payload, metadata FROM @BlobRows;
        """, tableParameters: [parameter])
        
        let rows = try await client.query("SELECT metadata FROM [\(tableName)] ORDER BY id")
        XCTAssertEqual(rows.map { $0.column("metadata")?.string ?? "" }, ["{\"tier\":\"gold\"}", "{\"tier\":\"silver\"}"])
    }
    
    func testTableValuedParameterHighRowCount() async throws {
        let tableName = "tvp_bulk_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "value", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        let batch = SQLServerTableValuedParameter(
            name: "BulkRows",
            columns: [
                .init(name: "id", dataType: .int),
                .init(name: "value", dataType: .nvarchar(length: .length(50)))
            ],
            rows: (1...250).map { idx in
                SQLServerTableValuedParameter.Row(values: [.int(idx), .nString("row-\(idx)")])
            }
        )
        
        _ = try await client.execute("""
        INSERT INTO [\(tableName)] (id, value)
        SELECT id, value FROM @BulkRows;
        """, tableParameters: [batch])
        
        let count = try await client.query("SELECT COUNT(*) AS inserted FROM [\(tableName)]")
        XCTAssertEqual(count.first?.column("inserted")?.int, 250)
    }
    
    func testTableValuedParameterMismatchedColumnsThrows() async throws {
        let parameter = SQLServerTableValuedParameter(
            name: "BadRows",
            columns: [
                .init(name: "id", dataType: .int),
                .init(name: "value", dataType: .nvarchar(length: .length(10)))
            ],
            rows: [
                .init(values: [.int(1), .nString("ok")]),
                .init(values: [.int(2)]) // Missing column on purpose
            ]
        )
        
        do {
            _ = try await client.execute("SELECT 1", tableParameters: [parameter])
            XCTFail("Expected invalid argument error for mismatched TVP columns")
        } catch {
            guard case SQLServerError.invalidArgument = error else {
                XCTFail("Expected SQLServerError.invalidArgument, got \(error)")
                return
            }
        }
    }
}
