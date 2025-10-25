@testable import SQLServerKit
import XCTest

final class SQLServerQuerySplitterTests: XCTestCase {
    
    func testBasicGOSplitting() {
        let sql = """
        SELECT 1
        GO
        SELECT 2
        GO
        SELECT 3
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[1].text, "SELECT 2")
        XCTAssertEqual(results[2].text, "SELECT 3")
    }
    
    func testGOWithWhitespace() {
        let sql = """
        SELECT 1
        GO   
        SELECT 2
        	GO	
        SELECT 3
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[1].text, "SELECT 2")
        XCTAssertEqual(results[2].text, "SELECT 3")
    }
    
    func testGOCaseInsensitive() {
        let sql = """
        SELECT 1
        go
        SELECT 2
        Go
        SELECT 3
        GO
        SELECT 4
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 4)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[1].text, "SELECT 2")
        XCTAssertEqual(results[2].text, "SELECT 3")
        XCTAssertEqual(results[3].text, "SELECT 4")
    }
    
    func testEmptyBatchesFiltered() {
        let sql = """
        SELECT 1
        GO
        
        GO
        SELECT 2
        GO
        -- Just a comment
        GO
        SELECT 3
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        // Should filter out empty batches but keep comments
        XCTAssertEqual(results.count, 4)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[1].text, "SELECT 2")
        XCTAssertEqual(results[2].text, "-- Just a comment")
        XCTAssertEqual(results[3].text, "SELECT 3")
    }
    
    func testComplexSQLWithGO() {
        let sql = """
        CREATE TABLE TestTable (
            id INT PRIMARY KEY,
            name NVARCHAR(50)
        )
        GO
        INSERT INTO TestTable (id, name) VALUES (1, N'Test')
        GO
        CREATE INDEX IX_TestTable_Name ON TestTable(name)
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        XCTAssertTrue(results[0].text.contains("CREATE TABLE"))
        XCTAssertTrue(results[1].text.contains("INSERT INTO"))
        XCTAssertTrue(results[2].text.contains("CREATE INDEX"))
    }
    
    func testStoredProcedureWithGO() {
        let sql = """
        CREATE PROCEDURE TestProc
        AS
        BEGIN
            SELECT 1
        END
        GO
        EXEC TestProc
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results[0].text.contains("CREATE PROCEDURE"))
        XCTAssertTrue(results[0].text.contains("BEGIN"))
        XCTAssertTrue(results[0].text.contains("END"))
        XCTAssertTrue(results[1].text.contains("EXEC TestProc"))
    }
    
    func testGOAtEndOfFile() {
        let sql = """
        SELECT 1
        GO
        SELECT 2
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[1].text, "SELECT 2")
    }
    
    func testGOWithoutNewline() {
        let sql = "SELECT 1 GO SELECT 2"
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        // GO should work even without newlines in some cases
        XCTAssertGreaterThanOrEqual(results.count, 1)
    }
    
    func testMultilineStatements() {
        let sql = """
        CREATE TABLE MultilineTest (
            id INT PRIMARY KEY,
            description NVARCHAR(MAX),
            created_date DATETIME2 DEFAULT GETDATE()
        )
        GO
        INSERT INTO MultilineTest 
        (id, description) 
        VALUES 
        (1, N'Multi-line insert statement')
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results[0].text.contains("CREATE TABLE"))
        XCTAssertTrue(results[0].text.contains("created_date"))
        XCTAssertTrue(results[1].text.contains("INSERT INTO"))
        XCTAssertTrue(results[1].text.contains("Multi-line"))
    }
    
    func testCommentsWithGO() {
        let sql = """
        -- This is a comment before the first statement
        SELECT 1 as first_query
        GO
        /* 
         * Multi-line comment
         * before second statement
         */
        SELECT 2 as second_query
        GO
        -- Final comment
        SELECT 3 as third_query -- inline comment
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        XCTAssertTrue(results[0].text.contains("-- This is a comment"))
        XCTAssertTrue(results[0].text.contains("first_query"))
        XCTAssertTrue(results[1].text.contains("Multi-line comment"))
        XCTAssertTrue(results[1].text.contains("second_query"))
        XCTAssertTrue(results[2].text.contains("-- Final comment"))
        XCTAssertTrue(results[2].text.contains("third_query"))
    }
    
    func testStringLiteralsWithGO() {
        let sql = """
        SELECT 'This string contains GO but should not split'
        GO
        SELECT N'Unicode string with GO inside'
        GO
        SELECT "Double quoted string with GO"
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        XCTAssertTrue(results[0].text.contains("'This string contains GO"))
        XCTAssertTrue(results[1].text.contains("N'Unicode string"))
        XCTAssertTrue(results[2].text.contains("\"Double quoted"))
    }
    
    func testPositionTracking() {
        let sql = """
        SELECT 1
        GO
        SELECT 2
        GO
        SELECT 3
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        
        // Verify positions are tracked correctly
        XCTAssertEqual(results[0].startPosition, 0)
        XCTAssertGreaterThan(results[0].endPosition, results[0].startPosition)
        
        XCTAssertGreaterThan(results[1].startPosition, results[0].endPosition)
        XCTAssertGreaterThan(results[1].endPosition, results[1].startPosition)
        
        XCTAssertGreaterThan(results[2].startPosition, results[1].endPosition)
        XCTAssertGreaterThan(results[2].endPosition, results[2].startPosition)
    }
    
    func testEditorOptions() {
        let sql = """
        SELECT 1;
        SELECT 2;
        GO
        SELECT 3
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssqlEditor)
        
        // Editor options might handle semicolons differently
        XCTAssertGreaterThanOrEqual(results.count, 2)
    }
    
    func testAdaptiveGOSplit() {
        let sql = """
        SELECT 1;
        SELECT 2;
        CREATE PROCEDURE TestProc
        AS
        BEGIN
            SELECT 3;
        END
        GO
        EXEC TestProc
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssqlEditor)
        
        // Should handle procedure creation specially
        XCTAssertGreaterThanOrEqual(results.count, 1)
    }
    
    func testLargeSQL() {
        let largeSql = (1...1000).map { "SELECT \($0)" }.joined(separator: "\nGO\n")
        
        let results = SQLServerQuerySplitter.splitQuery(largeSql, options: .mssql)
        
        XCTAssertEqual(results.count, 1000)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[999].text, "SELECT 1000")
    }
    
    func testSpecialCharactersInSQL() {
        let sql = """
        SELECT N'Unicode: 世界 🌍'
        GO
        SELECT 'Special chars: !@#$%^&*()_+-=[]{}|;:,.<>?'
        GO
        SELECT 'Quotes: ''single'' and "double"'
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 3)
        XCTAssertTrue(results[0].text.contains("世界 🌍"))
        XCTAssertTrue(results[1].text.contains("!@#$%^&*()"))
        XCTAssertTrue(results[2].text.contains("''single''"))
    }
    
    func testWindowsFunctionWithGO() {
        let sql = """
        SELECT 
            id,
            name,
            ROW_NUMBER() OVER (ORDER BY id) as row_num
        FROM TestTable
        GO
        SELECT 
            id,
            SUM(amount) OVER (PARTITION BY category) as total
        FROM SalesTable
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results[0].text.contains("ROW_NUMBER()"))
        XCTAssertTrue(results[0].text.contains("OVER (ORDER BY"))
        XCTAssertTrue(results[1].text.contains("SUM(amount)"))
        XCTAssertTrue(results[1].text.contains("PARTITION BY"))
    }
    
    func testCTEWithGO() {
        let sql = """
        WITH NumberedRows AS (
            SELECT 
                id,
                name,
                ROW_NUMBER() OVER (ORDER BY id) as rn
            FROM TestTable
        )
        SELECT * FROM NumberedRows WHERE rn <= 10
        GO
        WITH RecursiveCTE AS (
            SELECT 1 as n
            UNION ALL
            SELECT n + 1 FROM RecursiveCTE WHERE n < 10
        )
        SELECT * FROM RecursiveCTE
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results[0].text.contains("WITH NumberedRows"))
        XCTAssertTrue(results[0].text.contains("ROW_NUMBER()"))
        XCTAssertTrue(results[1].text.contains("WITH RecursiveCTE"))
        XCTAssertTrue(results[1].text.contains("UNION ALL"))
    }
    
    func testDynamicSQLWithGO() {
        let sql = """
        DECLARE @sql NVARCHAR(MAX) = N'SELECT * FROM TestTable'
        EXEC sp_executesql @sql
        GO
        DECLARE @tableName NVARCHAR(128) = N'TestTable'
        DECLARE @dynamicSQL NVARCHAR(MAX) = N'SELECT COUNT(*) FROM ' + QUOTENAME(@tableName)
        EXEC sp_executesql @dynamicSQL
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results[0].text.contains("DECLARE @sql"))
        XCTAssertTrue(results[0].text.contains("sp_executesql"))
        XCTAssertTrue(results[1].text.contains("@tableName"))
        XCTAssertTrue(results[1].text.contains("QUOTENAME"))
    }
    
    func testErrorHandlingInSplitter() {
        // Test with malformed SQL
        let malformedSQL = """
        SELECT * FROM
        GO
        INSERT INTO VALUES
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(malformedSQL, options: .mssql)
        
        // Splitter should still work even with malformed SQL
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results[0].text.contains("SELECT * FROM"))
        XCTAssertTrue(results[1].text.contains("INSERT INTO VALUES"))
    }
    
    func testEmptyInput() {
        let results = SQLServerQuerySplitter.splitQuery("", options: .mssql)
        XCTAssertEqual(results.count, 0)
    }
    
    func testWhitespaceOnlyInput() {
        let results = SQLServerQuerySplitter.splitQuery("   \n\t\r\n   ", options: .mssql)
        XCTAssertEqual(results.count, 0)
    }
    
    func testSingleGO() {
        let results = SQLServerQuerySplitter.splitQuery("GO", options: .mssql)
        XCTAssertEqual(results.count, 0)
    }
    
    func testMultipleConsecutiveGOs() {
        let sql = """
        SELECT 1
        GO
        GO
        GO
        SELECT 2
        GO
        """
        
        let results = SQLServerQuerySplitter.splitQuery(sql, options: .mssql)
        
        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[0].text, "SELECT 1")
        XCTAssertEqual(results[1].text, "SELECT 2")
    }
}