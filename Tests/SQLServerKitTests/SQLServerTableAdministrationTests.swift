import XCTest
import Logging
import NIO
@testable import SQLServerKit

final class SQLServerTableAdministrationTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private var adminClient: SQLServerAdministrationClient!
    private var tablesToDrop: [String] = []

    private var eventLoop: EventLoop { self.group.next() }

    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        loadEnvFileIfPresent()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        let config = makeSQLServerClientConfiguration()
        self.client = try SQLServerClient.connect(configuration: config, eventLoopGroupProvider: .shared(group)).wait()
        self.adminClient = SQLServerAdministrationClient(client: client)
    }

    override func tearDownWithError() throws {
        // Drop any tables that were created during the test
        for table in tablesToDrop {
            try adminClient.dropTable(name: table).wait()
        }
        tablesToDrop.removeAll()

        try self.client.shutdownGracefully().wait()
        try self.group?.syncShutdownGracefully()
        self.group = nil
        try super.tearDownWithError()
    }

    // MARK: - Tests

    func testDropTable() async throws {
        let tableName = "test_drop_table_\(UUID().uuidString.prefix(8))"
        // Don't add to tablesToDrop because we are testing the drop functionality itself

        // 1. Create a simple table
        let columns = [SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int)))]
        try await adminClient.createTable(name: tableName, columns: columns)

        // 2. Verify it exists
        var tableCount = try await getTableCount(name: tableName)
        XCTAssertEqual(tableCount, 1, "Table should exist after creation")

        // 3. Drop the table
        try await adminClient.dropTable(name: tableName)

        // 4. Verify it's gone
        tableCount = try await getTableCount(name: tableName)
        XCTAssertEqual(tableCount, 0, "Table should not exist after being dropped")
    }

    func testCreateTableSimple() async throws {
        let tableName = "test_simple_table_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)))))
        ]

        try await adminClient.createTable(name: tableName, columns: columns)

        let tableCount = try await getTableCount(name: tableName)
        XCTAssertEqual(tableCount, 1, "The simple table should have been created.")
    }

    func testCreateTableWithIdentityAndDefaults() async throws {
        let tableName = "test_identity_defaults_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, identity: (10, 2)))),
            SQLServerColumnDefinition(name: "status", definition: .standard(.init(dataType: .varchar(length: .length(20)), defaultValue: "'pending'")))
        ]

        try await adminClient.createTable(name: tableName, columns: columns)

        // Verify Identity Column
        let identitySQL = """
        SELECT seed_value, increment_value
        FROM sys.identity_columns
        WHERE object_id = OBJECT_ID('\(tableName)');
        """
        let identityResult = try await client.query(identitySQL).get()
        XCTAssertEqual(identityResult.count, 1, "Should find one identity column.")
        XCTAssertEqual(identityResult.first?.column("seed_value")?.int, 10)
        XCTAssertEqual(identityResult.first?.column("increment_value")?.int, 2)

        // Verify Default Constraint
        let defaultSQL = """
        SELECT definition
        FROM sys.default_constraints
        WHERE parent_object_id = OBJECT_ID('\(tableName)');
        """
        let defaultResult = try await client.query(defaultSQL).get()
        XCTAssertEqual(defaultResult.count, 1, "Should find one default constraint.")
        XCTAssertEqual(defaultResult.first?.column("definition")?.string, "('pending')")
    }

    func testCreateTableWithCommentsAndComputedColumn() async throws {
        let tableName = "test_comments_computed_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "comment_col", definition: .standard(.init(dataType: .ntext, comment: "This is a column comment."))),
            SQLServerColumnDefinition(name: "computed_col", definition: .computed(expression: "id * 10"))
        ]

        // Execute CREATE TABLE (comments will be added automatically after table creation)
        try await adminClient.createTable(name: tableName, columns: columns)

        // Add a small delay to ensure transaction is fully committed and visible to other connections
        try await Task.sleep(nanoseconds: 100_000_000) // 100ms

        // Verify Comment
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'comment_col';
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, "This is a column comment.")

        // Verify Computed Column
        let computedSQL = """
        SELECT definition
        FROM sys.computed_columns
        WHERE object_id = OBJECT_ID('dbo.\(tableName)') AND name = 'computed_col';
        """
        let computedResult = try await client.query(computedSQL).get()
        XCTAssertEqual(computedResult.count, 1, "Should find one computed column.")
        XCTAssertEqual(computedResult.first?.column("definition")?.string, "([id]*(10))")
    }

    func testCreateTableWithVariousConstraints() async throws {
        let tableName = "test_constraints_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)

        let columns = [
            SQLServerColumnDefinition(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "unique_col", definition: .standard(.init(dataType: .int, isUnique: true))),
            SQLServerColumnDefinition(name: "sparse_col", definition: .standard(.init(dataType: .int, isNullable: true, isSparse: true))),
            SQLServerColumnDefinition(name: "not_null_col", definition: .standard(.init(dataType: .int, isNullable: false)))
        ]

        try await adminClient.createTable(name: tableName, columns: columns)

        // Verify Unique Constraint
        let uniqueSQL = "SELECT COUNT(*) as count FROM sys.indexes WHERE object_id = OBJECT_ID('\(tableName)') AND is_unique = 1 AND is_primary_key = 0;"
        let uniqueResult = try await client.query(uniqueSQL).get()
        XCTAssertEqual(uniqueResult.first?.column("count")?.int, 1, "Should find one unique index.")

        // Verify Sparse Column
        let sparseSQL = "SELECT is_sparse FROM sys.columns WHERE object_id = OBJECT_ID('\(tableName)') AND name = 'sparse_col';"
        let sparseResult = try await client.query(sparseSQL).get()
        XCTAssertEqual(sparseResult.first?.column("is_sparse")?.bool, true, "Column should be marked as sparse.")

        // Verify Nullability
        let nullableSQL = "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '\(tableName)';"
        let nullableResult = try await client.query(nullableSQL).get()
        let nullableDict = Dictionary(uniqueKeysWithValues: nullableResult.map { ($0.column("COLUMN_NAME")!.string!, $0.column("IS_NULLABLE")!.string!) })
        XCTAssertEqual(nullableDict["sparse_col"], "YES")
        XCTAssertEqual(nullableDict["not_null_col"], "NO")
    }

    func testCreateTableWithCompositePrimaryKey() async throws {
        let tableName = "test_composite_pk_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)

        let columns = [
            SQLServerColumnDefinition(name: "id1", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            SQLServerColumnDefinition(name: "id2", definition: .standard(.init(dataType: .int, isPrimaryKey: true)))
        ]

        try await adminClient.createTable(name: tableName, columns: columns)

        let pkSQL = """
        SELECT COUNT(ku.COLUMN_NAME) as key_count
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
        ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        WHERE ku.TABLE_NAME = '\(tableName)';
        """
        let pkResult = try await client.query(pkSQL).get()
        XCTAssertEqual(pkResult.first?.column("key_count")?.int, 2, "Should have a composite primary key with 2 columns.")
    }

    func testCreateTableWithAllDataTypes() async throws {
        let tableName = "test_all_types_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)

        let columns: [SQLServerColumnDefinition] = [
            .init(name: "t_bit", definition: .standard(.init(dataType: .bit))),
            .init(name: "t_tinyint", definition: .standard(.init(dataType: .tinyint))),
            .init(name: "t_smallint", definition: .standard(.init(dataType: .smallint))),
            .init(name: "t_int", definition: .standard(.init(dataType: .int))),
            .init(name: "t_bigint", definition: .standard(.init(dataType: .bigint))),
            .init(name: "t_decimal", definition: .standard(.init(dataType: .decimal(precision: 10, scale: 2)))),
            .init(name: "t_numeric", definition: .standard(.init(dataType: .numeric(precision: 12, scale: 4)))),
            .init(name: "t_money", definition: .standard(.init(dataType: .money))),
            .init(name: "t_smallmoney", definition: .standard(.init(dataType: .smallmoney))),
            .init(name: "t_float", definition: .standard(.init(dataType: .float(mantissa: 40)))),
            .init(name: "t_real", definition: .standard(.init(dataType: .real))),
            .init(name: "t_date", definition: .standard(.init(dataType: .date))),
            .init(name: "t_datetime", definition: .standard(.init(dataType: .datetime))),
            .init(name: "t_datetime2", definition: .standard(.init(dataType: .datetime2(precision: 5)))),
            .init(name: "t_smalldatetime", definition: .standard(.init(dataType: .smalldatetime))),
            .init(name: "t_time", definition: .standard(.init(dataType: .time(precision: 3)))),
            .init(name: "t_datetimeoffset", definition: .standard(.init(dataType: .datetimeoffset(precision: 1)))),
            .init(name: "t_char", definition: .standard(.init(dataType: .char(length: 10)))),
            .init(name: "t_varchar", definition: .standard(.init(dataType: .varchar(length: .length(50))))),
            .init(name: "t_varcharmax", definition: .standard(.init(dataType: .varchar(length: .max)))),
            .init(name: "t_text", definition: .standard(.init(dataType: .text))),
            .init(name: "t_nchar", definition: .standard(.init(dataType: .nchar(length: 20)))),
            .init(name: "t_nvarchar", definition: .standard(.init(dataType: .nvarchar(length: .length(100))))),
            .init(name: "t_nvarcharmax", definition: .standard(.init(dataType: .nvarchar(length: .max)))),
            .init(name: "t_ntext", definition: .standard(.init(dataType: .ntext))),
            .init(name: "t_binary", definition: .standard(.init(dataType: .binary(length: 30)))),
            .init(name: "t_varbinary", definition: .standard(.init(dataType: .varbinary(length: .length(40))))),
            .init(name: "t_varbinarymax", definition: .standard(.init(dataType: .varbinary(length: .max)))),
            .init(name: "t_image", definition: .standard(.init(dataType: .image))),
            .init(name: "t_uniqueidentifier", definition: .standard(.init(dataType: .uniqueidentifier))),
            .init(name: "t_sql_variant", definition: .standard(.init(dataType: .sql_variant))),
            .init(name: "t_xml", definition: .standard(.init(dataType: .xml)))
        ]

        try await adminClient.createTable(name: tableName, columns: columns)

        let schemaSQL = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '\(tableName)' ORDER BY ORDINAL_POSITION;"
        let result = try await client.query(schemaSQL).get()

        let expected: [String: String] = [
            "t_bit": "bit",
            "t_tinyint": "tinyint",
            "t_smallint": "smallint",
            "t_int": "int",
            "t_bigint": "bigint",
            "t_decimal": "decimal",
            "t_numeric": "numeric",
            "t_money": "money",
            "t_smallmoney": "smallmoney",
            "t_float": "float",
            "t_real": "real",
            "t_date": "date",
            "t_datetime": "datetime",
            "t_datetime2": "datetime2",
            "t_smalldatetime": "smalldatetime",
            "t_time": "time",
            "t_datetimeoffset": "datetimeoffset",
            "t_char": "char",
            "t_varchar": "varchar",
            "t_varcharmax": "varchar",
            "t_text": "text",
            "t_nchar": "nchar",
            "t_nvarchar": "nvarchar",
            "t_nvarcharmax": "nvarchar",
            "t_ntext": "ntext",
            "t_binary": "binary",
            "t_varbinary": "varbinary",
            "t_varbinarymax": "varbinary",
            "t_image": "image",
            "t_uniqueidentifier": "uniqueidentifier",
            "t_sql_variant": "sql_variant",
            "t_xml": "xml"
        ]

        XCTAssertEqual(result.count, expected.count, "The number of created columns should match the number of expected columns.")

        for column in result {
            let name = column.column("COLUMN_NAME")!.string!
            let type = column.column("DATA_TYPE")!.string!
            XCTAssertEqual(type, expected[name], "Data type for column '\(name)' did not match.")
        }
    }

    func testAddColumnCommentAfterTableCreation() async throws {
        let tableName = "test_add_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        // Create table without comments
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(50)))))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Add comment after table creation
        try await adminClient.addColumnComment(
            tableName: tableName,
            columnName: "name",
            comment: "Added after table creation"
        )
        
        // Verify comment was added
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'name';
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Added after table creation")
    }
    
    func testAddTableComment() async throws {
        let tableName = "test_table_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        // Create simple table
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Add table comment
        try await adminClient.addTableComment(tableName: tableName, comment: "This is a table comment")
        
        // Verify table comment was added
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0;
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one table comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, "This is a table comment")
    }
    
    func testMultipleColumnComments() async throws {
        let tableName = "test_multi_comments_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, comment: "Primary key identifier"))),
            .init(name: "name", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), comment: "Full name of the entity"))),
            .init(name: "email", definition: .standard(.init(dataType: .nvarchar(length: .length(255)), isNullable: true, comment: "Email address (optional)")))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Verify all comments were added
        let commentsSQL = """
        SELECT c.name as column_name, p.value as comment_value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)')
        ORDER BY c.column_id;
        """
        let commentsResult = try await client.query(commentsSQL).get()
        XCTAssertEqual(commentsResult.count, 3, "Should find three column comments.")
        
        let expectedComments = [
            ("id", "Primary key identifier"),
            ("name", "Full name of the entity"),
            ("email", "Email address (optional)")
        ]
        
        for (index, row) in commentsResult.enumerated() {
            let columnName = row.column("column_name")?.string
            let commentValue = row.column("comment_value")?.string
            XCTAssertEqual(columnName, expectedComments[index].0)
            XCTAssertEqual(commentValue, expectedComments[index].1)
        }
    }
    
    func testCommentWithSpecialCharacters() async throws {
        let tableName = "test_special_chars_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let specialComment = "Comment with 'quotes', \"double quotes\", and unicode: 🚀 世界"
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "special_col", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: true, comment: specialComment)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Verify comment with special characters
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'special_col';
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, specialComment)
    }
    
    func testUpdateColumnComment() async throws {
        let tableName = "test_update_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true, comment: "Original comment")))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Update the comment
        try await adminClient.updateColumnComment(
            tableName: tableName,
            columnName: "description",
            comment: "Updated comment"
        )
        
        // Verify comment was updated
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'description';
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Updated comment")
    }
    
    func testUpdateTableComment() async throws {
        let tableName = "test_update_table_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Add initial table comment
        try await adminClient.addTableComment(tableName: tableName, comment: "Original table comment")
        
        // Update the table comment
        try await adminClient.updateTableComment(tableName: tableName, comment: "Updated table comment")
        
        // Verify comment was updated
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0;
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one table comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, "Updated table comment")
    }
    
    func testRemoveColumnComment() async throws {
        let tableName = "test_remove_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "description", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true, comment: "Comment to be removed")))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Verify comment exists
        let initialCommentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'description';
        """
        let initialResult = try await client.query(initialCommentSQL).get()
        XCTAssertEqual(initialResult.count, 1, "Should find one comment initially.")
        
        // Remove the comment
        try await adminClient.removeColumnComment(tableName: tableName, columnName: "description")
        
        // Verify comment was removed
        let finalResult = try await client.query(initialCommentSQL).get()
        XCTAssertEqual(finalResult.count, 0, "Should find no comments after removal.")
    }
    
    func testRemoveTableComment() async throws {
        let tableName = "test_remove_table_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Add table comment
        try await adminClient.addTableComment(tableName: tableName, comment: "Comment to be removed")
        
        // Verify comment exists
        let initialCommentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0;
        """
        let initialResult = try await client.query(initialCommentSQL).get()
        XCTAssertEqual(initialResult.count, 1, "Should find one table comment initially.")
        
        // Remove the table comment
        try await adminClient.removeTableComment(tableName: tableName)
        
        // Verify comment was removed
        let finalResult = try await client.query(initialCommentSQL).get()
        XCTAssertEqual(finalResult.count, 0, "Should find no table comments after removal.")
    }
    
    func testCommentWithLongText() async throws {
        let tableName = "test_long_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let longComment = String(repeating: "This is a very long comment that tests the limits of extended properties. ", count: 50)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "long_comment_col", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: true, comment: longComment)))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Verify long comment was stored correctly
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'long_comment_col';
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one comment.")
        XCTAssertEqual(commentResult.first?.column("value")?.string, longComment)
    }
    
    func testCommentWithEmptyString() async throws {
        let tableName = "test_empty_comment_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true))),
            .init(name: "empty_comment_col", definition: .standard(.init(dataType: .nvarchar(length: .length(50)), isNullable: true, comment: "")))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Verify empty comment was stored
        let commentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND c.name = N'empty_comment_col';
        """
        let commentResult = try await client.query(commentSQL).get()
        XCTAssertEqual(commentResult.count, 1, "Should find one comment.")
        
        // In SQL Server, empty strings in extended properties are stored as NULL
        // Accept either nil or empty string as valid for empty comments
        let value = commentResult.first?.column("value")?.string
        XCTAssertTrue(value == nil || value == "", "Empty comment should be nil or empty string, got: \(String(describing: value))")
    }
    
    func testTableAndColumnCommentsSimultaneously() async throws {
        let tableName = "test_both_comments_\(UUID().uuidString.prefix(8))"
        tablesToDrop.append(tableName)
        
        let columns: [SQLServerColumnDefinition] = [
            .init(name: "id", definition: .standard(.init(dataType: .int, isPrimaryKey: true, comment: "Primary key column comment"))),
            .init(name: "data", definition: .standard(.init(dataType: .nvarchar(length: .length(100)), isNullable: true, comment: "Data column comment")))
        ]
        try await adminClient.createTable(name: tableName, columns: columns)
        
        // Add table comment
        try await adminClient.addTableComment(tableName: tableName, comment: "Table level comment")
        
        // Verify table comment
        let tableCommentSQL = """
        SELECT p.value
        FROM sys.extended_properties p
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)') AND p.minor_id = 0;
        """
        let tableCommentResult = try await client.query(tableCommentSQL).get()
        XCTAssertEqual(tableCommentResult.count, 1, "Should find one table comment.")
        XCTAssertEqual(tableCommentResult.first?.column("value")?.string, "Table level comment")
        
        // Verify column comments
        let columnCommentsSQL = """
        SELECT c.name as column_name, p.value as comment_value
        FROM sys.extended_properties p
        JOIN sys.columns c ON p.major_id = c.object_id AND p.minor_id = c.column_id
        WHERE p.major_id = OBJECT_ID(N'dbo.\(tableName)')
        ORDER BY c.column_id;
        """
        let columnCommentsResult = try await client.query(columnCommentsSQL).get()
        XCTAssertEqual(columnCommentsResult.count, 2, "Should find two column comments.")
    }

    // MARK: - Helpers

    private func getTableCount(name: String) async throws -> Int {
        let sql = "SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '\(name)'"
        let result = try await client.query(sql).get()
        return result.first?.column("table_count")?.int ?? 0
    }
}
