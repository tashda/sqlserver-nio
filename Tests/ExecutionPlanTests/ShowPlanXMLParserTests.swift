import XCTest
@testable import SQLServerKit

final class ShowPlanXMLParserTests: XCTestCase {

    // MARK: - Minimal Plan

    func testParseMinimalPlan() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT 1" StatementType="SELECT" StatementSubTreeCost="0.0000001" StatementEstRows="1">
                  <QueryPlan CachedPlanSize="16" CompileTime="0" CompileCPU="0">
                    <RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0.0000001" AvgRowSize="11" EstimatedTotalSubtreeCost="0.0000001" Parallel="0">
                      <OutputList>
                        <ColumnReference Column="Union1001" />
                      </OutputList>
                      <ConstantScan />
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        XCTAssertEqual(plan.buildVersion, "15.0.4123.1")
        XCTAssertEqual(plan.statements.count, 1)

        let stmt = plan.statements[0]
        XCTAssertEqual(stmt.statementText, "SELECT 1")
        XCTAssertEqual(stmt.statementType, "SELECT")
        XCTAssertEqual(stmt.statementEstRows, 1.0)

        let qp = try XCTUnwrap(stmt.queryPlan)
        XCTAssertEqual(qp.cachedPlanSize, 16)
        XCTAssertEqual(qp.compileTime, 0)

        let root = try XCTUnwrap(qp.rootOperator)
        XCTAssertEqual(root.nodeId, 0)
        XCTAssertEqual(root.physicalOp, "Constant Scan")
        XCTAssertEqual(root.logicalOp, "Constant Scan")
        XCTAssertEqual(root.estimateRows, 1.0)
        XCTAssertEqual(root.avgRowSize, 11)
        XCTAssertFalse(root.isParallel)
        XCTAssertTrue(root.children.isEmpty)
        XCTAssertEqual(root.outputColumns.count, 1)
        XCTAssertEqual(root.outputColumns.first?.column, "Union1001")
    }

    // MARK: - Nested RelOps (3-level tree)

    func testParseNestedRelOps() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT * FROM A JOIN B ON A.id = B.id" StatementType="SELECT" StatementSubTreeCost="0.05">
                  <QueryPlan>
                    <RelOp NodeId="0" PhysicalOp="Hash Match" LogicalOp="Inner Join" EstimateRows="100" EstimatedTotalSubtreeCost="0.05">
                      <OutputList />
                      <HashMatch>
                        <RelOp NodeId="1" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="1000" EstimatedTotalSubtreeCost="0.03">
                          <OutputList />
                          <IndexScan>
                            <RelOp NodeId="2" PhysicalOp="Compute Scalar" LogicalOp="Compute Scalar" EstimateRows="1000" EstimatedTotalSubtreeCost="0.01">
                              <OutputList />
                              <ComputeScalar />
                            </RelOp>
                          </IndexScan>
                        </RelOp>
                        <RelOp NodeId="3" PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimateRows="50" EstimatedTotalSubtreeCost="0.01">
                          <OutputList />
                          <IndexScan />
                        </RelOp>
                      </HashMatch>
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        let root = try XCTUnwrap(plan.statements.first?.queryPlan?.rootOperator)

        XCTAssertEqual(root.physicalOp, "Hash Match")
        XCTAssertEqual(root.logicalOp, "Inner Join")
        XCTAssertEqual(root.children.count, 2)

        // Left child: Clustered Index Scan
        let left = root.children[0]
        XCTAssertEqual(left.nodeId, 1)
        XCTAssertEqual(left.physicalOp, "Clustered Index Scan")
        XCTAssertEqual(left.children.count, 1)

        // Left-left grandchild: Compute Scalar
        let grandchild = left.children[0]
        XCTAssertEqual(grandchild.nodeId, 2)
        XCTAssertEqual(grandchild.physicalOp, "Compute Scalar")
        XCTAssertTrue(grandchild.children.isEmpty)

        // Right child: Index Seek
        let right = root.children[1]
        XCTAssertEqual(right.nodeId, 3)
        XCTAssertEqual(right.physicalOp, "Index Seek")
        XCTAssertTrue(right.children.isEmpty)
    }

    // MARK: - Actual Metrics (RunTimeInformation)

    func testParseActualMetrics() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT * FROM Users" StatementType="SELECT">
                  <QueryPlan>
                    <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="100" Parallel="1" EstimatedTotalSubtreeCost="0.1">
                      <OutputList />
                      <RunTimeInformation>
                        <RunTimeCountersPerThread Thread="1" ActualRows="60" ActualExecutions="1" ActualElapsedms="15" ActualCPUms="10" />
                        <RunTimeCountersPerThread Thread="2" ActualRows="40" ActualExecutions="1" ActualElapsedms="12" ActualCPUms="8" />
                      </RunTimeInformation>
                      <IndexScan />
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        let root = try XCTUnwrap(plan.statements.first?.queryPlan?.rootOperator)

        XCTAssertTrue(root.isParallel)
        // Rows summed across threads: 60 + 40 = 100
        XCTAssertEqual(root.actualRows, 100)
        // Executions summed: 1 + 1 = 2
        XCTAssertEqual(root.actualExecutions, 2)
        // Elapsed: max(15, 12) = 15
        XCTAssertEqual(root.actualElapsedMs, 15)
        // CPU summed: 10 + 8 = 18
        XCTAssertEqual(root.actualCPUMs, 18)
    }

    // MARK: - Missing Index

    func testParseMissingIndex() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT * FROM Orders WHERE CustomerId = 1 AND Status > 0" StatementType="SELECT">
                  <QueryPlan>
                    <MissingIndexes>
                      <MissingIndexGroup Impact="95.5">
                        <MissingIndex Database="[TestDB]" Schema="[dbo]" Table="[Orders]">
                          <ColumnGroup Usage="EQUALITY">
                            <Column Name="CustomerId" />
                          </ColumnGroup>
                          <ColumnGroup Usage="INEQUALITY">
                            <Column Name="Status" />
                          </ColumnGroup>
                          <ColumnGroup Usage="INCLUDE">
                            <Column Name="OrderDate" />
                            <Column Name="Total" />
                          </ColumnGroup>
                        </MissingIndex>
                      </MissingIndexGroup>
                    </MissingIndexes>
                    <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="50" EstimatedTotalSubtreeCost="0.5">
                      <OutputList />
                      <TableScan />
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        let qp = try XCTUnwrap(plan.statements.first?.queryPlan)
        XCTAssertEqual(qp.missingIndexes.count, 1)

        let mi = qp.missingIndexes[0]
        XCTAssertEqual(mi.impact, 95.5)
        XCTAssertEqual(mi.database, "TestDB")
        XCTAssertEqual(mi.schema, "dbo")
        XCTAssertEqual(mi.table, "Orders")
        XCTAssertEqual(mi.equalityColumns, ["CustomerId"])
        XCTAssertEqual(mi.inequalityColumns, ["Status"])
        XCTAssertEqual(mi.includeColumns, ["OrderDate", "Total"])
    }

    // MARK: - Column References

    func testParseColumnReferences() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT id, name FROM Users" StatementType="SELECT">
                  <QueryPlan>
                    <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="100" EstimatedTotalSubtreeCost="0.01">
                      <OutputList>
                        <ColumnReference Database="[TestDB]" Schema="[dbo]" Table="[Users]" Column="id" />
                        <ColumnReference Database="[TestDB]" Schema="[dbo]" Table="[Users]" Column="name" />
                      </OutputList>
                      <IndexScan />
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        let root = try XCTUnwrap(plan.statements.first?.queryPlan?.rootOperator)
        XCTAssertEqual(root.outputColumns.count, 2)

        let col1 = root.outputColumns[0]
        XCTAssertEqual(col1.database, "TestDB")
        XCTAssertEqual(col1.schema, "dbo")
        XCTAssertEqual(col1.table, "Users")
        XCTAssertEqual(col1.column, "id")

        let col2 = root.outputColumns[1]
        XCTAssertEqual(col2.column, "name")
    }

    // MARK: - Warnings

    func testParseWarnings() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT * FROM LargeTable ORDER BY col1" StatementType="SELECT">
                  <QueryPlan>
                    <RelOp NodeId="0" PhysicalOp="Sort" LogicalOp="Sort" EstimateRows="1000000" EstimatedTotalSubtreeCost="10.5">
                      <OutputList />
                      <Warnings>
                        <SpillToTempDb SpillLevel="1" />
                        <NoJoinPredicate />
                      </Warnings>
                      <Sort />
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        let root = try XCTUnwrap(plan.statements.first?.queryPlan?.rootOperator)
        XCTAssertEqual(root.warnings.count, 2)
        XCTAssertTrue(root.warnings.contains("SpillToTempDb (Level 1)"))
        XCTAssertTrue(root.warnings.contains("NoJoinPredicate"))
    }

    // MARK: - Invalid XML

    func testInvalidXML() {
        let xml = "<this is not valid xml"
        XCTAssertThrowsError(try ShowPlanXMLParser.parse(xml: xml)) { error in
            guard case ShowPlanParseError.invalidXML = error else {
                XCTFail("Expected ShowPlanParseError.invalidXML, got \(error)")
                return
            }
        }
    }

    // MARK: - Raw XML Preserved

    func testRawXMLPreserved() throws {
        let xml = """
        <?xml version="1.0" encoding="utf-16"?>
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.6" BuildVersion="15.0.4123.1">
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT 1" StatementType="SELECT">
                  <QueryPlan>
                    <RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimateRows="1" EstimatedTotalSubtreeCost="0.0000001">
                      <OutputList />
                      <ConstantScan />
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """

        let plan = try ShowPlanXMLParser.parse(xml: xml)
        XCTAssertEqual(plan.xml, xml)
    }
}
