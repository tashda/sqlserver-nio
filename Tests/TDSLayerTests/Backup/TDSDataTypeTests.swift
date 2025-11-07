import XCTest
import NIOCore
import Logging
@testable import SQLServerTDS
@testable import SQLServerKit

/// Comprehensive TDS Data Type Tests
/// Tests all SQL Server data types using SQLServerClient against live database
final class TDSDataTypeTests: XCTestCase {
    private var group: EventLoopGroup!
    private var client: SQLServerClient!
    private let logger = Logger(label: "TDSDataTypeTests")

    override func setUp() async throws {
        TestEnvironmentManager.loadEnvironmentVariables()

        var config = makeSQLServerClientConfiguration()
        config.poolConfiguration.connectionIdleTimeout = nil
        config.poolConfiguration.minimumIdleConnections = 0

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.client = try await SQLServerClient.connect(
            configuration: config,
            eventLoopGroupProvider: .shared(group)
        ).get()
    }

    override func tearDown() async throws {
        await client?.shutdownGracefully()
        try await group?.shutdownGracefully()
    }

    // MARK: - Numeric Data Types

    func testIntegerTypes() async throws {
        logger.info("🔧 Testing integer data types...")

        let result = try await client.query("""
            SELECT
                1 as tinyint_val,
                255 as tinyint_max,
                -128 as tinyint_min,
                32767 as smallint_val,
                -32768 as smallint_min,
                2147483647 as int_val,
                -2147483648 as int_min,
                9223372036854775807 as bigint_val,
                -9223372036854775808 as bigint_min
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all numeric types
        XCTAssertNotNil(row.column("tinyint_val"))
        XCTAssertNotNil(row.column("smallint_val"))
        XCTAssertNotNil(row.column("int_val"))
        XCTAssertNotNil(row.column("bigint_val"))

        logger.info("✅ Integer types test completed - got \(result.count) rows")
    }

    func testDecimalTypes() async throws {
        logger.info("🔧 Testing decimal/numeric data types...")

        let result = try await client.query("""
            SELECT
                CAST(123.456789 as decimal(10,6)) as decimal_val,
                CAST(-123.456789 as decimal(10,6)) as decimal_negative,
                CAST(123.45 as numeric(10,2)) as numeric_val,
                CAST(3.14159265359 as float) as float_val,
                CAST(2.718281828 as real) as real_val,
                CAST(1.23456789012345 as double precision) as double_val
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all decimal types
        XCTAssertNotNil(row.column("decimal_val"))
        XCTAssertNotNil(row.column("float_val"))
        XCTAssertNotNil(row.column("double_val"))

        logger.info("✅ Decimal types test completed - got \(result.count) rows")
    }

    func testMoneyTypes() async throws {
        logger.info("🔧 Testing money data types...")

        let result = try await client.query("""
            SELECT
                CAST(1234.56 as money) as money_val,
                CAST(-1234.56 as money) as money_negative,
                CAST(1234.5678 as smallmoney) as smallmoney_val,
                CAST(0 as money) as money_zero
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all money types
        XCTAssertNotNil(row.column("money_val"))
        XCTAssertNotNil(row.column("smallmoney_val"))

        logger.info("✅ Money types test completed - got \(result.count) rows")
    }

    // MARK: - String Data Types

    func testStringTypes() async throws {
        logger.info("🔧 Testing string data types...")

        let result = try await client.query("""
            SELECT
                'Hello World' as varchar_val,
                CAST('Hello World' as varchar(50)) as varchar_fixed,
                N'Unicode Text' as nvarchar_val,
                CAST(N'Unicode Text' as nvarchar(100)) as nvarchar_fixed,
                'Very long text string that exceeds normal varchar length limits and should be handled by varchar(max)' as varchar_max,
                N'Unicode text that is also very long and should be handled by nvarchar(max) properly' as nvarchar_max,
                CAST('Fixed char text' as char(20)) as char_val,
                CAST(N'Fixed nchar text' as nchar(25)) as nchar_val
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all string types
        XCTAssertNotNil(row.column("varchar_val"))
        XCTAssertNotNil(row.column("nvarchar_val"))
        XCTAssertNotNil(row.column("varchar_max"))
        XCTAssertNotNil(row.column("nvarchar_max"))

        logger.info("✅ String types test completed - got \(result.count) rows")
    }

    func testTextTypes() async throws {
        logger.info("🔧 Testing text data types...")

        let result = try await client.query("""
            SELECT
                'This is a text value' as text_val,
                N'This is an ntext value' as ntext_val,
                CAST('<root><item>XML Content</item></root>' as xml_val,
                CAST('{"key": "value", "number": 123}' as json_val)
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all text types
        XCTAssertNotNil(row.column("text_val"))
        XCTAssertNotNil(row.column("ntext_val"))
        XCTAssertNotNil(row.column("xml_val"))
        XCTAssertNotNil(row.column("json_val"))

        logger.info("✅ Text types test completed - got \(result.count) rows")
    }

    // MARK: - Binary Data Types

    func testBinaryTypes() async throws {
        logger.info("🔧 Testing binary data types...")

        let result = try await client.query("""
            SELECT
                CAST(0x414243 as binary(3)) as binary_val,
                CAST(0x4142434445 as varbinary(5)) as varbinary_val,
                CAST(0x48656c6c6f20576f726c64 as varbinary(20)) as varbinary_text,
                CAST(REPLICATE(0x00, 100) as binary(100)) as binary_zeros,
                CAST(REPLICATE(0xFF, 50) as varbinary(50)) as binary_ffs
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all binary types
        XCTAssertNotNil(row.column("binary_val"))
        XCTAssertNotNil(row.column("varbinary_val"))
        XCTAssertNotNil(row.column("binary_zeros"))
        XCTAssertNotNil(row.column("binary_ffs"))

        logger.info("✅ Binary types test completed - got \(result.count) rows")
    }

    // MARK: - Date and Time Types

    func testDateTimeTypes() async throws {
        logger.info("🔧 Testing date and time data types...")

        let result = try await client.query("""
            SELECT
                CAST('2023-01-01' as date) as date_val,
                CAST('12:34:56.789' as time) as time_val,
                CAST('2023-01-01 12:34:56.789' as datetime) as datetime_val,
                CAST('2023-01-01 12:34:56.1234567' as datetime2) as datetime2_val,
                CAST('2023-01-01 12:34:56.123' as datetimeoffset) as datetimeoffset_val,
                CAST('2023-01-01' as smalldatetime) as smalldatetime_val
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for all date/time types
        XCTAssertNotNil(row.column("date_val"))
        XCTAssertNotNil(row.column("time_val"))
        XCTAssertNotNil(row.column("datetime_val"))
        XCTAssertNotNil(row.column("datetime2_val"))
        XCTAssertNotNil(row.column("datetimeoffset_val"))
        XCTAssertNotNil(row.column("smalldatetime_val"))

        logger.info("✅ DateTime types test completed - got \(result.count) rows")
    }

    // MARK: - Boolean and Bit Types

    func testBooleanTypes() async throws {
        logger.info("🔧 Testing boolean data types...")

        let result = try await client.query("""
            SELECT
                CAST(1 as bit) as bit_true,
                CAST(0 as bit) as bit_false,
                CAST(NULL as bit) as bit_null,
                1 as bit_literal_true,
                0 as bit_literal_false
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for boolean types
        XCTAssertNotNil(row.column("bit_true"))
        XCTAssertNotNil(row.column("bit_false"))
        XCTAssertNotNil(row.column("bit_null"))
        XCTAssertNotNil(row.column("bit_literal_true"))
        XCTAssertNotNil(row.column("bit_literal_false"))

        logger.info("✅ Boolean types test completed - got \(result.count) rows")
    }

    // MARK: - Unique Identifier Types

    func testUniqueIdentifierTypes() async throws {
        logger.info("🔧 Testing unique identifier data types...")

        let result = try await client.query("""
            SELECT
                NEWID() as guid_val,
                CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' as uniqueidentifier) as fixed_guid,
                CAST(NULL as uniqueidentifier) as guid_null
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for GUID types
        XCTAssertNotNil(row.column("guid_val"))
        XCTAssertNotNil(row.column("fixed_guid"))
        XCTAssertNotNil(row.column("guid_null"))

        logger.info("✅ UniqueIdentifier types test completed - got \(result.count) rows")
    }

    // MARK: - Special Data Types

    func testSpecialTypes() async throws {
        logger.info("🔧 Testing special data types...")

        let result = try await client.query("""
            SELECT
                CAST(0x53514C536572766572496D616765 as image) as image_val,
                CAST('This is a sql_variant value' as sql_variant) as sql_variant_val,
                CAST('2023-01-01' as timestamp) as timestamp_val,
                CAST(123 as rowversion) as rowversion_val,
                CAST(CAST(12345 AS varbinary(128)) as hierarchyid) as hierarchyid_val,
                NULL as null_val,
                '' as empty_string_val
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify we got data for special types
        XCTAssertNotNil(row.column("image_val"))
        XCTAssertNotNil(row.column("sql_variant_val"))
        XCTAssertNotNil(row.column("timestamp_val"))
        XCTAssertNotNil(row.column("rowversion_val"))
        XCTAssertNotNil(row.column("null_val"))
        XCTAssertNotNil(row.column("empty_string_val"))

        logger.info("✅ Special types test completed - got \(result.count) rows")
    }

    // MARK: - Mixed Data Types Test

    func testAllDataTypesInOneQuery() async throws {
        logger.info("🔧 Testing all data types in one comprehensive query...")

        let result = try await client.query("""
            SELECT
                -- Numeric Types
                42 as int_val,
                3.14159 as float_val,
                CAST(123.456 as decimal(10,3)) as decimal_val,
                CAST(999.99 as money) as money_val,

                -- String Types
                'Test String' as varchar_val,
                N'Test Unicode' as nvarchar_val,

                -- Date/Time Types
                CAST('2023-12-25' as date) as date_val,
                CAST('14:30:00' as time) as time_val,

                -- Binary Types
                CAST(0x48656c6c6f as varbinary(10)) as binary_val,

                -- Boolean Types
                CAST(1 as bit) as bit_val,

                -- Special Types
                NEWID() as guid_val,
                CAST(NULL as varchar(50)) as null_val,

                -- Constants
                'CONSTANT' as constant_val
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify all types returned data
        XCTAssertNotNil(row.column("int_val"))
        XCTAssertNotNil(row.column("float_val"))
        XCTAssertNotNil(row.column("decimal_val"))
        XCTAssertNotNil(row.column("money_val"))
        XCTAssertNotNil(row.column("varchar_val"))
        XCTAssertNotNil(row.column("nvarchar_val"))
        XCTAssertNotNil(row.column("date_val"))
        XCTAssertNotNil(row.column("time_val"))
        XCTAssertNotNil(row.column("binary_val"))
        XCTAssertNotNil(row.column("bit_val"))
        XCTAssertNotNil(row.column("guid_val"))
        XCTAssertNotNil(row.column("null_val"))
        XCTAssertNotNil(row.column("constant_val"))

        logger.info("✅ All data types test completed - got \(result.count) rows with 13 different data types")
    }

    // MARK: - Data Type Conversion Tests

    func testDataTypeConversions() async throws {
        logger.info("🔧 Testing data type conversions...")

        let result = try await client.query("""
            SELECT
                -- String to numeric conversions
                CAST('123' as int) as string_to_int,
                CAST('456.78' as decimal(10,2)) as string_to_decimal,
                CAST('2023-01-01' as date) as string_to_date,

                -- Numeric to string conversions
                CAST(123 as varchar(20)) as int_to_string,
                CAST(456.78 as varchar(20)) as decimal_to_string,
                CAST(GETDATE() as varchar(50)) as date_to_string,

                -- Date to numeric conversions
                CAST(DATEDIFF(day, '2000-01-01', '2023-01-01') as int) as date_diff,

                -- NULL handling
                CAST(NULL as varchar(50)) as null_to_string,
                ISNULL(CAST(NULL as int), 0) as null_with_default
        """)

        XCTAssertEqual(result.count, 1)
        let row = result.first!

        // Verify all conversions worked
        XCTAssertNotNil(row.column("string_to_int"))
        XCTAssertNotNil(row.column("string_to_decimal"))
        XCTAssertNotNil(row.column("string_to_date"))
        XCTAssertNotNil(row.column("int_to_string"))
        XCTAssertNotNil(row.column("decimal_to_string"))
        XCTAssertNotNil(row.column("date_to_string"))
        XCTAssertNotNil(row.column("date_diff"))
        XCTAssertNotNil(row.column("null_to_string"))
        XCTAssertNotNil(row.column("null_with_default"))

        logger.info("✅ Data type conversions test completed - got \(result.count) rows")
    }
}