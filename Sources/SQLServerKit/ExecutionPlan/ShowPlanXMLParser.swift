import Foundation
#if canImport(FoundationXML)
import FoundationXML
#endif

// MARK: - ShowPlanXMLParser

/// SAX-based parser for SQL Server ShowPlanXML execution plans.
/// Uses Foundation XMLParser for cross-platform compatibility (macOS + Linux).
internal final class ShowPlanXMLParser: NSObject, XMLParserDelegate, @unchecked Sendable {

    // MARK: - Parse entry point

    static func parse(xml: String) throws -> ShowPlan {
        // SQL Server returns XML with encoding="utf-16" but by the time it's a Swift String
        // it's already decoded. Replace the encoding declaration so NSXMLParser doesn't reject it.
        let normalized = xml.replacingOccurrences(of: "encoding=\"utf-16\"", with: "encoding=\"utf-8\"")
        guard let data = normalized.data(using: .utf8) else {
            throw ShowPlanParseError.invalidXML("Unable to encode XML as UTF-8")
        }
        let handler = ShowPlanXMLParser(rawXML: xml)
        let parser = XMLParser(data: data)
        parser.delegate = handler
        parser.shouldProcessNamespaces = false
        parser.shouldReportNamespacePrefixes = false

        guard parser.parse() else {
            if let error = handler.parseError {
                throw error
            }
            if let parserError = parser.parserError {
                throw ShowPlanParseError.invalidXML(parserError.localizedDescription)
            }
            throw ShowPlanParseError.invalidXML("Unknown parse error")
        }

        if let error = handler.parseError {
            throw error
        }

        return ShowPlan(
            statements: handler.statements,
            buildVersion: handler.buildVersion,
            xml: xml
        )
    }

    // MARK: - State

    private let rawXML: String
    private var buildVersion: String?
    private var statements: [ShowPlanStatement] = []
    private var parseError: ShowPlanParseError?

    // Stack-based parsing context
    private var elementStack: [ParseContext] = []

    // Current statement being built
    private var currentStatementText: String = ""
    private var currentStatementType: String = ""
    private var currentStatementSubTreeCost: Double?
    private var currentStatementEstRows: Double?
    private var currentQueryHash: String?
    private var currentQueryPlanHash: String?
    private var currentOptimizationLevel: String?

    // Current query plan
    private var currentCachedPlanSize: Int?
    private var currentCompileTime: Int?
    private var currentCompileCPU: Int?

    // Operator stack for recursive RelOp parsing
    private var operatorStack: [OperatorBuilder] = []
    private var rootOperator: ShowPlanOperator?

    // Missing indexes
    private var currentMissingIndexes: [ShowPlanMissingIndex] = []
    private var currentMissingIndexImpact: Double?
    private var currentMissingIndexDatabase: String?
    private var currentMissingIndexSchema: String?
    private var currentMissingIndexTable: String?
    private var currentMissingEqualityCols: [String] = []
    private var currentMissingInequalityCols: [String] = []
    private var currentMissingIncludeCols: [String] = []
    private var currentMissingColumnUsage: String?

    // Column references
    private var currentOutputColumns: [ShowPlanColumnReference] = []

    // Warnings
    private var currentWarnings: [String] = []

    // Runtime counters accumulation
    private var currentActualRows: Int?
    private var currentActualExecutions: Int?
    private var currentActualElapsedMs: Int?
    private var currentActualCPUMs: Int?

    private init(rawXML: String) {
        self.rawXML = rawXML
    }

    // MARK: - Parse contexts

    private enum ParseContext {
        case showPlanXML
        case batchSequence
        case batch
        case statements
        case stmtSimple
        case stmtCond
        case queryPlan
        case relOp
        case outputList
        case columnReference
        case runTimeInformation
        case runTimeCountersPerThread
        case warnings
        case missingIndexes
        case missingIndexGroup
        case missingIndex
        case columnGroup
        case column
        case other(String)
    }

    // MARK: - Operator builder (mutable during parsing)

    private final class OperatorBuilder {
        var nodeId: Int = 0
        var physicalOp: String = ""
        var logicalOp: String = ""
        var estimateRows: Double?
        var estimateIO: Double?
        var estimateCPU: Double?
        var avgRowSize: Int?
        var totalSubtreeCost: Double?
        var isParallel: Bool = false
        var estimatedExecutions: Double?
        var actualRows: Int?
        var actualExecutions: Int?
        var actualElapsedMs: Int?
        var actualCPUMs: Int?
        var children: [ShowPlanOperator] = []
        var outputColumns: [ShowPlanColumnReference] = []
        var warnings: [String] = []

        func build() -> ShowPlanOperator {
            ShowPlanOperator(
                nodeId: nodeId,
                physicalOp: physicalOp,
                logicalOp: logicalOp,
                estimateRows: estimateRows,
                estimateIO: estimateIO,
                estimateCPU: estimateCPU,
                avgRowSize: avgRowSize,
                totalSubtreeCost: totalSubtreeCost,
                isParallel: isParallel,
                estimatedExecutions: estimatedExecutions,
                actualRows: actualRows,
                actualExecutions: actualExecutions,
                actualElapsedMs: actualElapsedMs,
                actualCPUMs: actualCPUMs,
                children: children,
                outputColumns: outputColumns,
                warnings: warnings
            )
        }
    }

    // MARK: - XMLParserDelegate

    func parser(
        _ parser: XMLParser,
        didStartElement elementName: String,
        namespaceURI: String?,
        qualifiedName: String?,
        attributes attributeDict: [String: String]
    ) {
        switch elementName {
        case "ShowPlanXML":
            buildVersion = attributeDict["BuildVersion"]
            elementStack.append(.showPlanXML)

        case "BatchSequence":
            elementStack.append(.batchSequence)

        case "Batch":
            elementStack.append(.batch)

        case "Statements":
            elementStack.append(.statements)

        case "StmtSimple":
            currentStatementText = attributeDict["StatementText"] ?? ""
            currentStatementType = attributeDict["StatementType"] ?? ""
            currentStatementSubTreeCost = attributeDict["StatementSubTreeCost"].flatMap(Double.init)
            currentStatementEstRows = attributeDict["StatementEstRows"].flatMap(Double.init)
            currentQueryHash = attributeDict["QueryHash"]
            currentQueryPlanHash = attributeDict["QueryPlanHash"]
            currentOptimizationLevel = attributeDict["StatementOptmLevel"]
            rootOperator = nil
            currentMissingIndexes = []
            elementStack.append(.stmtSimple)

        case "StmtCond":
            currentStatementText = attributeDict["StatementText"] ?? ""
            currentStatementType = attributeDict["StatementType"] ?? "COND"
            currentStatementSubTreeCost = attributeDict["StatementSubTreeCost"].flatMap(Double.init)
            currentStatementEstRows = attributeDict["StatementEstRows"].flatMap(Double.init)
            currentQueryHash = attributeDict["QueryHash"]
            currentQueryPlanHash = attributeDict["QueryPlanHash"]
            currentOptimizationLevel = attributeDict["StatementOptmLevel"]
            rootOperator = nil
            currentMissingIndexes = []
            elementStack.append(.stmtCond)

        case "QueryPlan":
            currentCachedPlanSize = attributeDict["CachedPlanSize"].flatMap(Int.init)
            currentCompileTime = attributeDict["CompileTime"].flatMap(Int.init)
            currentCompileCPU = attributeDict["CompileCPU"].flatMap(Int.init)
            elementStack.append(.queryPlan)

        case "RelOp":
            let builder = OperatorBuilder()
            builder.nodeId = attributeDict["NodeId"].flatMap(Int.init) ?? 0
            builder.physicalOp = attributeDict["PhysicalOp"] ?? ""
            builder.logicalOp = attributeDict["LogicalOp"] ?? ""
            builder.estimateRows = attributeDict["EstimateRows"].flatMap(Double.init)
            builder.estimateIO = attributeDict["EstimateIO"].flatMap(Double.init)
            builder.estimateCPU = attributeDict["EstimateCPU"].flatMap(Double.init)
            builder.avgRowSize = attributeDict["AvgRowSize"].flatMap(Int.init)
            builder.totalSubtreeCost = attributeDict["EstimatedTotalSubtreeCost"].flatMap(Double.init)
            builder.isParallel = attributeDict["Parallel"] == "1" || attributeDict["Parallel"] == "true"
            builder.estimatedExecutions = attributeDict["EstimatedExecutionMode"].flatMap(Double.init)
                ?? attributeDict["EstimateExecutions"].flatMap(Double.init)
            currentOutputColumns = []
            currentWarnings = []
            currentActualRows = nil
            currentActualExecutions = nil
            currentActualElapsedMs = nil
            currentActualCPUMs = nil
            operatorStack.append(builder)
            elementStack.append(.relOp)

        case "OutputList":
            currentOutputColumns = []
            elementStack.append(.outputList)

        case "ColumnReference":
            let colRef = ShowPlanColumnReference(
                database: attributeDict["Database"]?.trimmingCharacters(in: CharacterSet(charactersIn: "[]")),
                schema: attributeDict["Schema"]?.trimmingCharacters(in: CharacterSet(charactersIn: "[]")),
                table: attributeDict["Table"]?.trimmingCharacters(in: CharacterSet(charactersIn: "[]")),
                column: attributeDict["Column"] ?? ""
            )
            if let top = elementStack.last {
                switch top {
                case .outputList:
                    currentOutputColumns.append(colRef)
                case .columnGroup, .column:
                    // Missing index column reference
                    let colName = attributeDict["Column"] ?? ""
                    if !colName.isEmpty {
                        switch currentMissingColumnUsage {
                        case "EQUALITY":
                            currentMissingEqualityCols.append(colName)
                        case "INEQUALITY":
                            currentMissingInequalityCols.append(colName)
                        case "INCLUDE":
                            currentMissingIncludeCols.append(colName)
                        default:
                            break
                        }
                    }
                default:
                    break
                }
            }
            elementStack.append(.columnReference)

        case "RunTimeInformation":
            currentActualRows = 0
            currentActualExecutions = 0
            currentActualElapsedMs = 0
            currentActualCPUMs = 0
            elementStack.append(.runTimeInformation)

        case "RunTimeCountersPerThread":
            // Sum across threads for parallel plans
            if let rows = attributeDict["ActualRows"].flatMap(Int.init) {
                currentActualRows = (currentActualRows ?? 0) + rows
            }
            if let executions = attributeDict["ActualExecutions"].flatMap(Int.init) {
                currentActualExecutions = (currentActualExecutions ?? 0) + executions
            }
            if let elapsed = attributeDict["ActualElapsedms"].flatMap(Int.init) {
                // Take the max elapsed time across threads
                currentActualElapsedMs = max(currentActualElapsedMs ?? 0, elapsed)
            }
            if let cpu = attributeDict["ActualCPUms"].flatMap(Int.init) {
                currentActualCPUMs = (currentActualCPUMs ?? 0) + cpu
            }
            elementStack.append(.runTimeCountersPerThread)

        case "Warnings":
            currentWarnings = []
            elementStack.append(.warnings)

        case "SpillToTempDb":
            if let spill = attributeDict["SpillLevel"] {
                currentWarnings.append("SpillToTempDb (Level \(spill))")
            } else {
                currentWarnings.append("SpillToTempDb")
            }

        case "NoJoinPredicate":
            currentWarnings.append("NoJoinPredicate")

        case "ColumnsWithNoStatistics":
            currentWarnings.append("ColumnsWithNoStatistics")

        case "MissingIndexes":
            elementStack.append(.missingIndexes)

        case "MissingIndexGroup":
            currentMissingIndexImpact = attributeDict["Impact"].flatMap(Double.init)
            elementStack.append(.missingIndexGroup)

        case "MissingIndex":
            currentMissingIndexDatabase = attributeDict["Database"]?.trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
            currentMissingIndexSchema = attributeDict["Schema"]?.trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
            currentMissingIndexTable = attributeDict["Table"]?.trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
            currentMissingEqualityCols = []
            currentMissingInequalityCols = []
            currentMissingIncludeCols = []
            elementStack.append(.missingIndex)

        case "ColumnGroup":
            currentMissingColumnUsage = attributeDict["Usage"]
            elementStack.append(.columnGroup)

        case "Column":
            let colName = attributeDict["Name"] ?? ""
            if !colName.isEmpty {
                switch currentMissingColumnUsage {
                case "EQUALITY":
                    currentMissingEqualityCols.append(colName)
                case "INEQUALITY":
                    currentMissingInequalityCols.append(colName)
                case "INCLUDE":
                    currentMissingIncludeCols.append(colName)
                default:
                    break
                }
            }
            elementStack.append(.column)

        default:
            elementStack.append(.other(elementName))
        }
    }

    func parser(
        _ parser: XMLParser,
        didEndElement elementName: String,
        namespaceURI: String?,
        qualifiedName: String?
    ) {
        guard !elementStack.isEmpty else { return }
        _ = elementStack.removeLast()

        switch elementName {
        case "StmtSimple", "StmtCond":
            let queryPlan: ShowPlanQueryPlan?
            if rootOperator != nil || !currentMissingIndexes.isEmpty ||
               currentCachedPlanSize != nil || currentCompileTime != nil {
                queryPlan = ShowPlanQueryPlan(
                    cachedPlanSize: currentCachedPlanSize,
                    compileTime: currentCompileTime,
                    compileCPU: currentCompileCPU,
                    rootOperator: rootOperator,
                    missingIndexes: currentMissingIndexes
                )
            } else {
                queryPlan = nil
            }

            let statement = ShowPlanStatement(
                statementText: currentStatementText,
                statementType: currentStatementType,
                statementSubTreeCost: currentStatementSubTreeCost,
                statementEstRows: currentStatementEstRows,
                queryHash: currentQueryHash,
                queryPlanHash: currentQueryPlanHash,
                optimizationLevel: currentOptimizationLevel,
                queryPlan: queryPlan
            )
            statements.append(statement)

            // Reset query plan state
            currentCachedPlanSize = nil
            currentCompileTime = nil
            currentCompileCPU = nil
            rootOperator = nil

        case "RelOp":
            guard let builder = operatorStack.popLast() else { break }
            // Attach accumulated state
            builder.outputColumns = currentOutputColumns
            builder.warnings = currentWarnings
            builder.actualRows = currentActualRows
            builder.actualExecutions = currentActualExecutions
            builder.actualElapsedMs = currentActualElapsedMs
            builder.actualCPUMs = currentActualCPUMs

            let op = builder.build()

            if let parent = operatorStack.last {
                // Nested: add as child of parent operator
                parent.children.append(op)
            } else {
                // Root operator
                rootOperator = op
            }

            // Restore parent's output columns/warnings context
            currentOutputColumns = []
            currentWarnings = []
            currentActualRows = nil
            currentActualExecutions = nil
            currentActualElapsedMs = nil
            currentActualCPUMs = nil

        case "MissingIndex":
            let missingIndex = ShowPlanMissingIndex(
                impact: currentMissingIndexImpact,
                database: currentMissingIndexDatabase,
                schema: currentMissingIndexSchema,
                table: currentMissingIndexTable,
                equalityColumns: currentMissingEqualityCols,
                inequalityColumns: currentMissingInequalityCols,
                includeColumns: currentMissingIncludeCols
            )
            currentMissingIndexes.append(missingIndex)

        case "ColumnGroup":
            currentMissingColumnUsage = nil

        default:
            break
        }
    }

    func parser(_ parser: XMLParser, parseErrorOccurred parseError: Error) {
        self.parseError = ShowPlanParseError.invalidXML(parseError.localizedDescription)
    }
}

// MARK: - ShowPlanParseError

public enum ShowPlanParseError: Error, Sendable {
    case invalidXML(String)
}
